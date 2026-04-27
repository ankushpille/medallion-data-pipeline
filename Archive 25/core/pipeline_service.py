from typing import Dict, List, Tuple, Optional
import io
import re
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session
from loguru import logger

from core.settings import settings
from core.azure_storage import get_storage_client, AzureStorageClient
from core.database import SessionLocal
from core.notifications import NotificationService
from models.master_config_authoritative import MasterConfigAuthoritative
from models.dq_schema_config import DQSchemaConfig, ExpectedDataType, DQRule, Severity


class PipelineService:
    def __init__(self):
        self.s3 = get_storage_client()          # AzureStorageClient (S3-compatible API)
        self.bucket = settings.AZURE_CONTAINER_NAME or "datalake"
        self.raw_root = "Raw"
        self.notifier = NotificationService()

    def run(self, dataset_id: str, suppress_email: bool = False) -> Dict:
        db: Session = SessionLocal()
        self._suppress_email = suppress_email
        metrics = {
            "dataset_id": dataset_id,
            "raw": {"rows_read": 0},
            "bronze": {"rows_written": 0},
            "dq": {"failed_rows": 0, "warnings": 0},
            "silver": {"rows_written": 0},
        }
        batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        try:
            mc = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == dataset_id).first()
            mc_row = self._find_master_config_row(dataset_id)
            if not mc and not mc_row:
                raise RuntimeError(f"Master configuration not found for dataset_id: {dataset_id}")

            # Prefer explicit raw path from Master Config CSV, else fallback to DB-driven scan
            raw_dfs = []
            raw_path = None
            if mc and getattr(mc, "raw_layer_path", None):
                raw_path = mc.raw_layer_path
            elif mc_row and mc_row.get("raw_layer_path"):
                raw_path = mc_row.get("raw_layer_path")

            if raw_path and not str(raw_path).startswith("s3://") and not str(raw_path).startswith("http"):
                try:
                    raw_dfs = self._read_raw_from_path(raw_path)
                except Exception as path_err:
                    logger.warning(
                        f"Could not read from raw_layer_path '{raw_path}': {path_err}. "
                        f"Falling back to DB-driven scan."
                    )
                    raw_dfs = []
            
            # Fallback to DB-driven scan if path read failed or was skipped
            if not raw_dfs:
                if mc:
                    raw_dfs = self._read_raw(mc)
                elif mc_row:
                    # Build a minimal stub for fallback scan
                    class _MCStub:
                        pass
                    stub = _MCStub()
                    stub.client_name = mc_row.get("client_name", "")
                    stub.source_folder = mc_row.get("source_folder", "")
                    stub.source_object = mc_row.get("source_object", "")
                    stub.dataset_id = dataset_id
                    raw_dfs = self._read_raw(stub)
            if not raw_dfs:
                raise RuntimeError("no raw files found")
            df_bronze = self._standardize_and_tag(raw_dfs, batch_id)
            metrics["raw"]["rows_read"] = int(df_bronze.shape[0])

            _mc_for_bronze = mc
            if not _mc_for_bronze and mc_row:
                class _MCStubB:
                    pass
                _mc_for_bronze = _MCStubB()
                _mc_for_bronze.client_name = mc_row.get("client_name")
                _mc_for_bronze.source_folder = mc_row.get("source_folder")
                _mc_for_bronze.source_object = mc_row.get("source_object")
            bronze_key = self._write_bronze(df_bronze, _mc_for_bronze, batch_id)
            metrics["bronze"]["rows_written"] = int(df_bronze.shape[0])

            dq_cfg = db.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id == dataset_id, DQSchemaConfig.is_active == True).all()
            try:
                valid_df, invalid_df, warn_count = self._apply_dq(df_bronze, dq_cfg)
            except Exception as e:
                subject = f"Pipeline Error: {dataset_id} - DQ"
                body = f"Reason: {str(e)}"
                if not getattr(self, "_suppress_email", False):
                    self.notifier.send_email(subject, body)
                raise
            metrics["dq"]["failed_rows"] = int(invalid_df.shape[0])
            metrics["dq"]["warnings"] = int(warn_count)
            try:
                dq_details = self._dq_details(df_bronze, dq_cfg)
            except Exception as e:
                logger.warning(f"dq details failed: {e}")
                dq_details = {"casts": [], "violations": [], "warnings": []}

            silver_keys = []
            rejected_keys = []
            # Always write Silver regardless of master config is_active
            _mc_for_silver = mc
            if not _mc_for_silver and mc_row:
                class _MCStub2:
                    pass
                _mc_for_silver = _MCStub2()
                _mc_for_silver.client_name = mc_row.get("client_name")
                _mc_for_silver.source_folder = mc_row.get("source_folder")
                _mc_for_silver.source_object = mc_row.get("source_object")
                _mc_for_silver.target_layer_silver = mc_row.get("target_layer_silver")
                _mc_for_silver.upsert_key = mc_row.get("upsert_key")
                _mc_for_silver.partition_column = mc_row.get("partition_column")
                _mc_for_silver.watermark_column = mc_row.get("watermark_column")
                _mc_for_silver.load_type = mc_row.get("load_type")
            written, silver_keys = self._write_silver(valid_df, _mc_for_silver, batch_id)
            metrics["silver"]["rows_written"] = int(written)
            if invalid_df.shape[0] > 0:
                try:
                    rkey = self._write_rejected(invalid_df, _mc_for_silver, batch_id)
                    rejected_keys.append(rkey)
                except Exception as e:
                    logger.warning(f"rejected write failed: {e}")

            try:
                client_name = (mc.client_name if mc else mc_row.get("client_name", "")) or ""
                metrics["paths"] = {"bronze": bronze_key, "silver": silver_keys, "rejected": rejected_keys}
                metrics["dq_details"] = dq_details
                if not suppress_email:
                    html = self._compose_success_report(dataset_id, client_name, metrics, bronze_key, silver_keys, rejected_keys, dq_details)
                    self.notifier.send_email_html(f"Pipeline Success: {dataset_id}", html)
            except Exception as e:
                logger.warning(f"success email failed: {e}")
            return metrics

        except Exception as e:
            try:
                if not getattr(self, "_suppress_email", False):
                    subject = f"Pipeline Error: {dataset_id}"
                    body = f"Failure: {str(e)}"
                    self.notifier.send_email(subject, body)
            except Exception:
                pass
            raise
        finally:
            db.close()

    def _read_raw(self, mc: MasterConfigAuthoritative) -> List[pd.DataFrame]:
        client = (mc.client_name or "").strip("/")
        
        file_path = getattr(mc, "raw_layer_path", "") or ""
        if file_path and (file_path.startswith("s3://") or file_path.startswith("az://")):
            folder_uri = file_path.rsplit('/', 1)[0]
        else:
            folder_uri = mc.source_folder
            
        folder = self._clean_folder_path(folder_uri)
        obj = (mc.source_object or "").strip("/")
        batch = (getattr(mc, "last_seen_batch", None) or "").strip("/")
        if batch:
            prefix = "/".join([p for p in [self.raw_root, client, batch] if p]).strip("/") + "/"
        else:
            prefix = "/".join([p for p in [self.raw_root, client] if p]).strip("/") + "/"
        resp = self.s3.list_objects_v2(Prefix=prefix, Container=self.bucket)
        items = []
        for c in resp.get("Contents", []):
            key = c["Key"]
            if key.endswith("/"):
                continue
            if f"/{folder.strip('/')}/" not in key:
                continue
            fname = key.split("/")[-1]
            if obj and fname != obj:
                continue
            ext = fname.split(".")[-1].lower() if "." in fname else ""
            if ext in ["csv", "xlsx", "json"]:
                try:
                    df = self._load_as_df(self.bucket, key, ext)
                    items.append(df)
                except Exception as e:
                    logger.error(f"raw read failed for {key}: {e}")
                    subject = f"Pipeline Error: {mc.dataset_id} - Raw"
                    body = f"Key: {key}\nReason: {str(e)}"
                    if not getattr(self, "_suppress_email", False):
                        self.notifier.send_email(subject, body)
                    raise
        return items

    def _read_raw_from_path(self, s3_url: str) -> List[pd.DataFrame]:
        bkt, key = self._parse_s3(s3_url)
        # Guard: if parsed container doesn't match our known bucket, use default
        if bkt != self.bucket:
            logger.warning(
                f"Raw path container '{bkt}' != configured bucket '{self.bucket}'. "
                f"Retrying with default bucket."
            )
            bkt = self.bucket
        ext = key.split(".")[-1].lower() if "." in key else ""
        if ext not in ["csv", "xlsx", "json", "parquet"]:
            raise RuntimeError(f"unsupported raw file format: {ext}")
        if ext == "parquet":
            obj = self.s3.get_object(Key=key, Container=bkt)
            import pyarrow.parquet as pq
            import io as _io
            table = pq.read_table(_io.BytesIO(obj["Body"].read()))
            return [table.to_pandas()]
        df = self._load_as_df(bkt, key, ext)
        return [df]

    def _find_master_config_row(self, dataset_id: str) -> Optional[Dict]:
        try:
            prefix = "Master_Configuration/"
            resp = self.s3.list_objects_v2(Prefix=prefix, Delimiter="/", Container=self.bucket)
            clients = [p["Prefix"].split("/")[1] for p in resp.get("CommonPrefixes", [])]
            for client in clients:
                key = f"Master_Configuration/{client}/master_config.csv"
                try:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
                    rows = df[df["dataset_id"] == dataset_id]
                    if not rows.empty:
                        return rows.iloc[0].to_dict()
                except Exception:
                    continue
        except Exception:
            pass
        return None

    def _load_as_df(self, bucket: str, key: str, ext: str) -> pd.DataFrame:
        obj = self.s3.get_object(Key=key, Container=bucket)
        data = obj["Body"].read()
        if ext == "csv":
            return pd.read_csv(io.BytesIO(data))
        if ext == "xlsx":
            return pd.read_excel(io.BytesIO(data))
        if ext == "json":
            return pd.read_json(io.BytesIO(data), lines=False)
        raise ValueError("unsupported format")

    def _standardize_and_tag(self, dfs: List[pd.DataFrame], batch_id: str) -> pd.DataFrame:
        cols = set()
        for d in dfs:
            cols |= set([str(c).strip() for c in d.columns])
        cols = list(cols)
        aligned = []
        for d in dfs:
            cmap = {c: str(c).strip() for c in d.columns}
            d2 = d.rename(columns=cmap)
            for c in cols:
                if c not in d2.columns:
                    d2[c] = None
            d2["_ingest_ts"] = datetime.utcnow().isoformat()
            d2["_batch_id"] = batch_id
            aligned.append(d2[cols + ["_ingest_ts", "_batch_id"]])
        
        full_df = pd.concat(aligned, ignore_index=True)
        logger.info(f"Standardized {len(dfs)} raw segments into a single DataFrame with {len(full_df)} rows and {len(cols)} columns.")
        return full_df

    def _write_bronze(self, df: pd.DataFrame, mc: MasterConfigAuthoritative, batch_id: str) -> str:
        bkt = self.bucket
        ts = datetime.utcnow().strftime("%B_%d_%H")
        client = (mc.client_name or "").strip("/")
        folder = self._clean_folder_path(mc.source_folder)
        base = (mc.source_object or "").rsplit(".", 1)[0]
        key = f"Bronze/{client}/{folder}/{ts}/{base}.parquet"
        buf = io.BytesIO()
        table = self._to_arrow(df)
        import pyarrow.parquet as pq
        pq.write_table(table, buf)
        buf.seek(0)
        try:
            self.s3.put_object(Container=bkt, Key=key, Body=buf.getvalue())
            logger.info(f"Bronze Write SUCCESS: {key} ({len(df)} rows)")
            return key
        except Exception as e:
            subject = f"Pipeline Error: {mc.dataset_id} - Bronze"
            body = f"Reason: {str(e)}"
            if not getattr(self, "_suppress_email", False):
                self.notifier.send_email(subject, body)
            raise

    def _apply_dq(self, df: pd.DataFrame, cfg_rows: List[DQSchemaConfig]) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
        if not cfg_rows:
            return df.copy(), df.iloc[0:0].copy(), 0
        casted = df.copy()
        warn_count = 0
        invalid_mask = pd.Series(False, index=casted.index)
        for r in cfg_rows:
            # Skip incomplete placeholder rules
            if getattr(r, "expected_data_type", None) is None or getattr(r, "dq_rule", None) is None or getattr(r, "severity", None) is None:
                continue
            col = r.column_name
            if col not in casted.columns:
                casted[col] = None
            if r.expected_data_type == ExpectedDataType.STRING:
                casted[col] = casted[col].astype(str)
            elif r.expected_data_type == ExpectedDataType.INTEGER:
                casted[col] = pd.to_numeric(casted[col], errors="coerce").astype("Int64")
            elif r.expected_data_type == ExpectedDataType.FLOAT:
                casted[col] = pd.to_numeric(casted[col], errors="coerce")
            elif r.expected_data_type == ExpectedDataType.BOOLEAN:
                casted[col] = casted[col].astype(str).str.lower().map({"true": True, "false": False})
            elif r.expected_data_type in [ExpectedDataType.DATE, ExpectedDataType.TIMESTAMP]:
                casted[col] = pd.to_datetime(casted[col], errors="coerce")
            cast_fail = casted[col].isna() & df[col].notna()
            if cast_fail.any():
                if r.severity in [Severity.ERROR, Severity.BLOCK]:
                    invalid_mask = invalid_mask | cast_fail
                else:
                    warn_count += int(cast_fail.sum())
            if r.dq_rule == DQRule.NOT_NULL:
                null_mask = casted[col].isna() | (casted[col].astype(str).str.strip() == "")
                if r.severity in [Severity.ERROR, Severity.BLOCK]:
                    invalid_mask = invalid_mask | null_mask
                else:
                    warn_count += int(null_mask.sum())
            elif r.dq_rule == DQRule.UNIQUE:
                dup_mask = casted[col].duplicated(keep=False)
                if r.severity in [Severity.ERROR, Severity.BLOCK]:
                    invalid_mask = invalid_mask | dup_mask
                else:
                    warn_count += int(dup_mask.sum())
            elif r.dq_rule == DQRule.RANGE:
                min_v = None
                max_v = None
                if r.rule_value:
                    parts = str(r.rule_value).split(":")
                    if len(parts) == 2:
                        min_v = float(parts[0]) if parts[0] else None
                        max_v = float(parts[1]) if parts[1] else None
                rng_mask = pd.Series(False, index=casted.index)
                if min_v is not None:
                    rng_mask = rng_mask | (pd.to_numeric(casted[col], errors="coerce") < min_v)
                if max_v is not None:
                    rng_mask = rng_mask | (pd.to_numeric(casted[col], errors="coerce") > max_v)
                if r.severity in [Severity.ERROR, Severity.BLOCK]:
                    invalid_mask = invalid_mask | rng_mask
                else:
                    warn_count += int(rng_mask.sum())
            elif r.dq_rule == DQRule.REGEX:
                pattern = str(r.rule_value) if r.rule_value is not None else ""
                rx = re.compile(pattern) if pattern else None
                rx_mask = pd.Series(False, index=casted.index)
                if rx:
                    rx_mask = ~casted[col].astype(str).str.match(rx)
                if r.severity in [Severity.ERROR, Severity.BLOCK]:
                    invalid_mask = invalid_mask | rx_mask
                else:
                    warn_count += int(rx_mask.sum())
            elif r.dq_rule in [DQRule.CUSTOM]:
                rv = str(r.rule_value) if r.rule_value is not None else ""
                if rv.upper().startswith("ALLOWED_VALUES="):
                    allowed = [x.strip() for x in rv.split("=", 1)[1].split(",")]
                    mask = ~casted[col].astype(str).isin(allowed)
                    if r.severity in [Severity.ERROR, Severity.BLOCK]:
                        invalid_mask = invalid_mask | mask
                    else:
                        warn_count += int(mask.sum())
                elif rv.upper().startswith("NEGATIVE_CHECK="):
                    flag = rv.split("=", 1)[1].strip().lower() == "true"
                    if flag:
                        mask = pd.to_numeric(casted[col], errors="coerce") < 0
                        if r.severity in [Severity.ERROR, Severity.BLOCK]:
                            invalid_mask = invalid_mask | mask
                        else:
                            warn_count += int(mask.sum())
        
        valid_df = casted[~invalid_mask].copy()
        invalid_df = casted[invalid_mask].copy()
        
        logger.info(f"DQ Filtering Complete for {len(casted)} rows: {len(valid_df)} PASSED, {len(invalid_df)} FAILED (Errors), {warn_count} Warnings logged.")
        return valid_df, invalid_df, warn_count

    def _write_silver(self, df: pd.DataFrame, mc: MasterConfigAuthoritative, batch_id: str) -> Tuple[int, List[str]]:
        bkt = self.bucket
        client = (mc.client_name or "").strip("/")
        folder = self._clean_folder_path(mc.source_folder)
        base = (mc.source_object or "").rsplit(".", 1)[0]
        ts = datetime.utcnow().strftime("%B_%d_%H")
        if df.empty:
            return 0, []
        df2 = df.copy()
        if mc.watermark_column and mc.watermark_column in df2.columns:
            df2.sort_values(by=[mc.watermark_column], inplace=True)
        lt = str(mc.load_type).upper() if getattr(mc, "load_type", None) is not None else None
        if lt == "INCREMENTAL" and mc.upsert_key:
            existing = self._read_existing_silver(bkt, f"Silver/{client}/{folder}/")
            if existing is not None and not existing.empty:
                combined = pd.concat([existing, df2], ignore_index=True)
                combined.drop_duplicates(subset=[mc.upsert_key], keep="last", inplace=True)
                df2 = combined
        if mc.partition_column and mc.partition_column in df2.columns:
            logger.info(f"Writing partitioned Silver data on column: {mc.partition_column}")
            total, keys = self._write_partitioned(df2, bkt, f"Silver/{client}/{folder}/{ts}", mc.partition_column, batch_id, base)
            return total, keys
        key = f"Silver/{client}/{folder}/{ts}/{base}.parquet"
        buf = io.BytesIO()
        table = self._to_arrow(df2)
        import pyarrow.parquet as pq
        pq.write_table(table, buf)
        buf.seek(0)
        try:
            self.s3.put_object(Container=bkt, Key=key, Body=buf.getvalue())
            logger.info(f"Silver Write SUCCESS: {key} ({len(df2)} rows)")
            return int(df2.shape[0]), [key]
        except Exception as e:
            subject = f"Pipeline Error: {mc.dataset_id} - Silver"
            body = f"Reason: {str(e)}"
            if not getattr(self, "_suppress_email", False):
                self.notifier.send_email(subject, body)
            raise

    def _write_partitioned(self, df: pd.DataFrame, bucket: str, prefix: str, part_col: str, batch_id: str, base_name: str) -> Tuple[int, List[str]]:
        total = 0
        keys: List[str] = []
        for val, grp in df.groupby(part_col):
            part = str(val).strip().replace("/", "_")
            key = f"{prefix.strip('/')}/{part}/{base_name}.parquet"
            buf = io.BytesIO()
            table = self._to_arrow(grp)
            import pyarrow.parquet as pq
            pq.write_table(table, buf)
            buf.seek(0)
            self.s3.put_object(Container=bucket, Key=key, Body=buf.getvalue())
            total += int(grp.shape[0])
            keys.append(key)
        return total, keys

    def _read_existing_silver(self, bucket: str, prefix: str) -> Optional[pd.DataFrame]:
        resp = self.s3.list_objects_v2(Prefix=prefix, Container=bucket)
        frames = []
        for c in resp.get("Contents", []):
            key = c["Key"]
            if key.endswith("/"):
                continue
            if not key.lower().endswith(".parquet"):
                continue
            obj = self.s3.get_object(Key=key, Container=bucket)
            data = obj["Body"].read()
            import pyarrow.parquet as pq
            table = pq.read_table(io.BytesIO(data))
            frames.append(table.to_pandas())
        if frames:
            return pd.concat(frames, ignore_index=True)
        return None

    def _write_rejected(self, df: pd.DataFrame, mc: MasterConfigAuthoritative, batch_id: str) -> str:
        bkt = self.bucket
        client = (mc.client_name or "").strip("/")
        folder = self._clean_folder_path(mc.source_folder)
        base = (mc.source_object or "").rsplit(".", 1)[0]
        ts = datetime.utcnow().strftime("%B_%d_%H")
        key = f"Silver/{client}/{folder}/{ts}/invalid_{base}.parquet"
        buf = io.BytesIO()
        table = self._to_arrow(df)
        import pyarrow.parquet as pq
        pq.write_table(table, buf)
        buf.seek(0)
        self.s3.put_object(Container=bkt, Key=key, Body=buf.getvalue())
        return key

    def _clean_folder_path(self, folder: str) -> str:
        if not folder:
            return "root"
        f = folder.strip("/")
        if f.startswith("az://") or f.startswith("s3://"):
            _rest = f.split("://", 1)[1]
            _parts = _rest.split("/", 1)
            return (_parts[1] if len(_parts) > 1 else _parts[0]).strip("/").replace("/", "_") or "root"
        return f.replace("/", "_") or "root"

    def _parse_s3(self, url: str) -> Tuple[str, str]:
        """Parses az://container/blob or legacy s3://bucket/key URLs."""
        return AzureStorageClient.parse_az_url(url)

    def _to_arrow(self, df: pd.DataFrame):
        import pyarrow as pa
        return pa.Table.from_pandas(df, preserve_index=False)

    def _dq_details(self, df: pd.DataFrame, cfg_rows: List[DQSchemaConfig]) -> Dict:
        details = {"casts": [], "violations": [], "warnings": []}
        if not cfg_rows:
            return details
        for r in cfg_rows:
            col = r.column_name
            s = df[col] if col in df.columns else pd.Series([None]*len(df))
            if r.expected_data_type == ExpectedDataType.INTEGER:
                casted = pd.to_numeric(s, errors="coerce")
            elif r.expected_data_type == ExpectedDataType.FLOAT:
                casted = pd.to_numeric(s, errors="coerce")
            elif r.expected_data_type == ExpectedDataType.BOOLEAN:
                casted = s.astype(str).str.lower().map({"true": True, "false": False})
            elif r.expected_data_type in [ExpectedDataType.DATE, ExpectedDataType.TIMESTAMP]:
                casted = pd.to_datetime(s, errors="coerce")
            else:
                casted = s.astype(str)
            cast_fail = casted.isna() & s.notna()
            if cast_fail.any():
                samples = s[cast_fail].astype(str).head(5).tolist()
                entry = {"column": col, "expected": r.expected_data_type.value, "count": int(cast_fail.sum()), "samples": samples}
                if r.severity in [Severity.ERROR, Severity.BLOCK]:
                    details["violations"].append({"rule": "CAST_FAIL", **entry})
                else:
                    details["warnings"].append({"rule": "CAST_FAIL", **entry})
            if r.dq_rule == DQRule.NOT_NULL:
                mask = casted.isna() | (casted.astype(str).str.strip() == "")
            elif r.dq_rule == DQRule.UNIQUE:
                mask = casted.duplicated(keep=False)
            elif r.dq_rule == DQRule.RANGE:
                min_v = None; max_v = None
                if r.rule_value:
                    parts = str(r.rule_value).split(":")
                    if len(parts) == 2:
                        min_v = float(parts[0]) if parts[0] else None
                        max_v = float(parts[1]) if parts[1] else None
                mask = pd.Series(False, index=casted.index)
                if min_v is not None:
                    mask = mask | (pd.to_numeric(casted, errors="coerce") < min_v)
                if max_v is not None:
                    mask = mask | (pd.to_numeric(casted, errors="coerce") > max_v)
            elif r.dq_rule == DQRule.REGEX:
                pattern = str(r.rule_value) if r.rule_value is not None else ""
                rx = re.compile(pattern) if pattern else None
                mask = pd.Series(False, index=casted.index)
                if rx:
                    mask = ~casted.astype(str).str.match(rx)
            elif r.dq_rule in [DQRule.CUSTOM]:
                mask = pd.Series(False, index=casted.index)
                rv = str(r.rule_value) if r.rule_value is not None else ""
                if rv.upper().startswith("ALLOWED_VALUES="):
                    allowed = [x.strip() for x in rv.split("=", 1)[1].split(",")]
                    mask = ~casted.astype(str).isin(allowed)
                elif rv.upper().startswith("NEGATIVE_CHECK="):
                    flag = rv.split("=", 1)[1].strip().lower() == "true"
                    if flag:
                        mask = pd.to_numeric(casted, errors="coerce") < 0
            else:
                mask = pd.Series(False, index=casted.index)
            if mask.any():
                samples = s[mask].astype(str).head(5).tolist()
                entry = {"column": col, "rule": r.dq_rule.value, "count": int(mask.sum()), "samples": samples}
                if r.severity in [Severity.ERROR, Severity.BLOCK]:
                    details["violations"].append(entry)
                else:
                    details["warnings"].append(entry)
        return details

    def _compose_dq_report_html(self, dataset_id: str, client_name: str, metrics: Dict, dq_details: Dict) -> str:
        rows_raw = metrics.get("raw", {}).get("rows_read", 0)
        rows_bronze = metrics.get("bronze", {}).get("rows_written", 0)
        dq_failed = metrics.get("dq", {}).get("failed_rows", 0)
        dq_warn = metrics.get("dq", {}).get("warnings", 0)
        rows_silver = metrics.get("silver", {}).get("rows_written", 0)
        v_html = "".join([f"<li>{d['rule']} on {d['column']} (count={d['count']}) samples={', '.join(d['samples'])}</li>" for d in dq_details.get("violations", [])]) or "<li>None</li>"
        w_html = "".join([f"<li>{d['rule']} on {d['column']} (count={d['count']}) samples={', '.join(d['samples'])}</li>" for d in dq_details.get("warnings", [])]) or "<li>None</li>"
        html = f"""
        <h2>DQ Report</h2>
        <p><strong>Client:</strong> {client_name}</p>
        <p><strong>Dataset:</strong> {dataset_id}</p>
        <h3>Metrics</h3>
        <ul>
          <li>Raw rows read: {rows_raw}</li>
          <li>Bronze rows written: {rows_bronze}</li>
          <li>DQ failed rows: {dq_failed}</li>
          <li>DQ warnings: {dq_warn}</li>
          <li>Silver rows written: {rows_silver}</li>
        </ul>
        <h3>Violations</h3>
        <ul>{v_html}</ul>
        <h3>Warnings</h3>
        <ul>{w_html}</ul>
        """
        return html
    def _compose_success_report(self, dataset_id: str, client_name: str, metrics: Dict, bronze_key: str, silver_keys: List[str], rejected_keys: List[str], dq_details: Dict) -> str:
        rows_raw = metrics.get("raw", {}).get("rows_read", 0)
        rows_bronze = metrics.get("bronze", {}).get("rows_written", 0)
        dq_failed = metrics.get("dq", {}).get("failed_rows", 0)
        dq_warn = metrics.get("dq", {}).get("warnings", 0)
        rows_silver = metrics.get("silver", {}).get("rows_written", 0)
        sil_html = "".join([f"<li>{k}</li>" for k in silver_keys]) or "<li>None</li>"
        rej_html = "".join([f"<li>{k}</li>" for k in rejected_keys]) or "<li>None</li>"
        v_html = "".join([f"<li>{d['rule']} on {d['column']} (count={d['count']}) samples={', '.join(d['samples'])}</li>" for d in dq_details.get("violations", [])]) or "<li>None</li>"
        w_html = "".join([f"<li>{d['rule']} on {d['column']} (count={d['count']}) samples={', '.join(d['samples'])}</li>" for d in dq_details.get("warnings", [])]) or "<li>None</li>"
        html = f"""
        <h2>Pipeline Report</h2>
        <p><strong>Client:</strong> {client_name}</p>
        <p><strong>Dataset:</strong> {dataset_id}</p>
        <h3>Metrics</h3>
        <ul>
          <li>Raw rows read: {rows_raw}</li>
          <li>Bronze rows written: {rows_bronze}</li>
          <li>DQ failed rows: {dq_failed}</li>
          <li>DQ warnings: {dq_warn}</li>
          <li>Silver rows written: {rows_silver}</li>
        </ul>
        <h3>Outputs</h3>
        <p><strong>Bronze:</strong> {bronze_key}</p>
        <p><strong>Silver Files:</strong></p>
        <ul>{sil_html}</ul>
        <p><strong>Rejected Files:</strong></p>
        <ul>{rej_html}</ul>
        <h3>DQ Details</h3>
        <h4>Violations</h4>
        <ul>{v_html}</ul>
        <h4>Warnings</h4>
        <ul>{w_html}</ul>
        """
        return html