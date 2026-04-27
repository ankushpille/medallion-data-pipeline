from datetime import datetime
from loguru import logger
from core.settings import settings
from core.azure_storage import get_storage_client
from core.mcp_connector import get_mcp_connector
from core.master_config_manager import MasterConfigManager
from core.database import SessionLocal
from models.dq_schema_config import DQSchemaConfig, ExpectedDataType, Severity
import hashlib
import io
import pandas as pd

class IngestionService:
    def __init__(self):
        # Robustly fetch connection details (handling potential reload glitches)
        self.storage = get_storage_client()
        self.container = settings.AZURE_CONTAINER_NAME or "datalake"
        self.config_manager = MasterConfigManager()

    def run_ingestion(self, source_type: str, client_name: str, folder_path: str):
        """
        Orchestrates the full flow:
        1. List Datasets (MCP)
        2. Filter & Validate
        3. Copy to Raw Layer (S3)
        4. Update Master Config
        """
        batch_id = datetime.utcnow().strftime("%b-%d-%H") # e.g. Jan-06-11
        logger.info(f"Starting Ingestion Batch: {batch_id} for {client_name} from {source_type}")

        try:
            # 1. List Datasets
            connector = get_mcp_connector(source_type)
            datasets = connector.list_datasets(client_name, folder_path)
            
            valid_datasets_for_config = []
            success_list_for_email = []
            dq_prepared = []
            failed_datasets = []
            
            # 2. Process Each Dataset
            for ds in datasets:
                try:
                    # VALIDATION
                    from core.validation import ValidationService
                    
                    # Get Content 
                    content = connector.get_file_content(ds.file_path, client_name)
                    
                    # Validate Content (Empty check, size completeness)
                    # Pass the expected file size to ensure full data was downloaded from MCP
                    ValidationService.validate_content(content, ds.file_name, expected_size=ds.file_size)
                    
                    # 3. Copy to Raw Layer
                    # Sanitize folder_path URI to a clean folder segment
                    if folder_path.startswith("az://") or folder_path.startswith("s3://"):
                        _rest = folder_path.split("://", 1)[1]
                        _parts = _rest.split("/", 1)
                        _rel = _parts[1] if len(_parts) > 1 else _parts[0]
                        source_folder_clean = _rel.strip("/").replace("/", "_") or "root"
                    else:
                        source_folder_clean = folder_path.replace("/", "_")

                    raw_key = f"Raw/{client_name}/{batch_id}/{source_folder_clean}/{ds.file_name}"
                    
                    logger.info(f"Ingesting {ds.file_name} to {raw_key}")
                    
                    self.storage.put_object(
                        Container=self.container,
                        Key=raw_key,
                        Body=content
                    )
                    # Prepare record for Master Config update with raw path
                    valid_datasets_for_config.append({
                        "file_path": ds.file_path,
                        "file_name": ds.file_name,
                        "file_format": ds.file_format,
                        "client_name": client_name,
                        "source_type": source_type,
                        "raw_layer_path": f"az://{self.container}/{raw_key}"
                    })
                    # Keep original DatasetInfo for email reporting (has file_size)
                    success_list_for_email.append(ds)

                    # Prepare placeholder DQ configuration (inactive) with nulls
                    try:
                        df_cols = []
                        df_infer = None
                        if ds.file_name.lower().endswith(".csv"):
                            try:
                                df_infer = pd.read_csv(io.BytesIO(content), nrows=50, on_bad_lines="skip")
                            except Exception:
                                df_infer = pd.read_csv(io.BytesIO(content), nrows=20)
                        elif ds.file_name.lower().endswith((".xlsx", ".xls")):
                            df_infer = pd.read_excel(io.BytesIO(content), nrows=50)
                        elif ds.file_name.lower().endswith(".json"):
                            try:
                                df_infer = pd.read_json(io.BytesIO(content), lines=False)
                            except Exception:
                                df_infer = pd.read_json(io.BytesIO(content), lines=True)
                            if isinstance(df_infer, list):
                                df_infer = pd.DataFrame(df_infer)
                        df_cols = list(df_infer.columns) if df_infer is not None else []
                        inferred_types = {}
                        if df_infer is not None:
                            for c in df_cols:
                                dtype = str(df_infer[c].dtype).lower()
                                if "int" in dtype:
                                    inferred_types[c] = ExpectedDataType.INTEGER
                                elif "float" in dtype:
                                    inferred_types[c] = ExpectedDataType.FLOAT
                                elif "datetime" in dtype or "date" in dtype:
                                    inferred_types[c] = ExpectedDataType.DATE
                                elif "bool" in dtype:
                                    inferred_types[c] = ExpectedDataType.BOOLEAN
                                else:
                                    inferred_types[c] = ExpectedDataType.STRING
                        # Compute dataset_id deterministically
                        d_id = hashlib.sha256(f"{client_name}{source_type}{ds.file_path}".encode("utf-8")).hexdigest()
                        dq_prepared.append({"dataset_id": d_id, "columns": df_cols, "types": inferred_types})
                    except Exception as e:
                        logger.warning(f"Failed preparing DQ placeholder for {ds.file_name}: {e}")
                    
                except Exception as e:
                    logger.error(f"Failed to ingest {ds.file_name}: {e}")
                    failed_datasets.append({"file": ds.file_name, "reason": str(e)})
            
            # 4. Update Master Configuration (Only for valid ones)
            if valid_datasets_for_config:
                mcp_output = {
                    "client_name": client_name,
                    "source_type": source_type,
                    "source_folder": folder_path,
                    "datasets": valid_datasets_for_config
                }
                self.config_manager.update_master_config(mcp_output)
                # Create placeholder DQ config entries (inactive) so pipeline doesn't block
                try:
                    session = SessionLocal()
                    for item in dq_prepared:
                        dsid = item["dataset_id"]
                        for col in item["columns"]:
                            exists = session.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id == dsid, DQSchemaConfig.column_name == col).first()
                            if exists:
                                continue
                            et = item.get("types", {}).get(col, ExpectedDataType.STRING)
                            session.add(DQSchemaConfig(
                                dataset_id=dsid,
                                column_name=col,
                                expected_data_type=et,
                                dq_rule=None,
                                rule_value=None,
                                severity=Severity.INFO,
                                is_active=False
                            ))
                    session.commit()
                    session.close()
                except Exception as e:
                    logger.warning(f"Failed to create placeholder DQ configs: {e}")
                logger.info("Ingestion & Config Update Complete.")
            else:
                logger.warning("No valid datasets ingested in this batch.")

            # 5. SEND CUMULATIVE NOTIFICATION
            try:
                from core.notifications import NotificationService
                notifier = NotificationService()
                notifier.send_ingestion_report(
                    client_name=client_name,
                    batch_id=batch_id,
                    success_list=success_list_for_email,
                    failure_list=failed_datasets
                )
                try:
                    from core.pipeline_service import PipelineService
                    svc = PipelineService()
                    pipeline_results = []
                    for item in dq_prepared:
                        dsid = item["dataset_id"]
                        try:
                            res = svc.run(dsid, suppress_email=True)
                            pipeline_results.append({"dataset_id": dsid, "status": "SUCCESS", "metrics": res})
                        except Exception as pe:
                            pipeline_results.append({"dataset_id": dsid, "status": "FAILURE", "reason": str(pe)})
                            logger.error(f"Pipeline run failed for {dsid}: {pe}")
                    try:
                        succ = [r for r in pipeline_results if r["status"] == "SUCCESS"]
                        fail = [r for r in pipeline_results if r["status"] == "FAILURE"]
                        html_parts = []
                        html_parts.append(f"<h2>Pipeline Batch Report</h2><p><strong>Client:</strong> {client_name}</p><p><strong>Batch:</strong> {batch_id}</p>")
                        html_parts.append(f"<h3>Success ({len(succ)})</h3>")
                        for r in succ:
                            m = r["metrics"]
                            keys = m.get("paths", {})
                            html_parts.append(f"<p><strong>{r['dataset_id']}</strong>: raw={m['raw']['rows_read']}, bronze={m['bronze']['rows_written']}, silver={m['silver']['rows_written']}</p>")
                            html_parts.append(f"<ul><li>Bronze: {keys.get('bronze')}</li><li>Silver: {', '.join(keys.get('silver', [])) or 'None'}</li><li>Rejected: {', '.join(keys.get('rejected', [])) or 'None'}</li></ul>")
                        html_parts.append(f"<h3>Failure ({len(fail)})</h3>")
                        for r in fail:
                            html_parts.append(f"<p><strong>{r['dataset_id']}</strong>: {r.get('reason','')}</p>")
                        html = "".join(html_parts)
                        notifier.send_email_html(f"Pipeline Batch Report: {client_name} - {batch_id}", html)
                    except Exception as e2:
                        logger.error(f"Failed to send pipeline batch report: {e2}")
                except Exception as pe:
                    logger.error(f"Pipeline trigger failed: {pe}")
            except Exception as e:
                logger.error(f"Failed to send notification: {e}")

        except Exception as e:
            logger.error(f"Ingestion Run Failed: {e}")
            raise e
