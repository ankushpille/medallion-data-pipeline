import logging
from typing import Dict, List, Any, Tuple
from .base import CloudScanner

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError
except ImportError:
    boto3 = None
    BotoCoreError = ClientError = NoCredentialsError = PartialCredentialsError = Exception

logger = logging.getLogger("pipeline_ie.scanner.aws")


FILE_EXTENSIONS = {"csv", "json", "parquet"}


class AWSScanner(CloudScanner):
    def can_scan(self, settings: Any) -> bool:
        return bool(settings.aws_access_key_id and settings.aws_secret_access_key)

    def scan(self, settings: Any, **kwargs) -> Dict[str, List[Any]]:
        warnings: List[str] = []
        errors: List[str] = []
        raw_assets: Dict[str, List[Any]] = {"s3": [], "glue_jobs": [], "glue_databases": [], "glue_tables": []}

        if not boto3:
            return {
                "raw_cloud_dump": [raw_assets],
                "warnings": [],
                "errors": ["boto3 is not installed; AWS live scan cannot run."],
                "_scan_meta": [{"auth_failed": True, "assumed_role": False}],
            }

        region = getattr(settings, "aws_region", None) or "us-east-1"
        role_arn = getattr(settings, "aws_role_arn", None)

        try:
            session, assumed_role = self._build_session(settings, region, role_arn)
            sts = session.client("sts", region_name=region)
            identity = sts.get_caller_identity()
            account_id = identity.get("Account")
            logger.info("AWS scan authenticated successfully. region=%s auth_mode=%s account=%s", region, "assumed_role" if assumed_role else "credentials", account_id)
        except (ClientError, NoCredentialsError, PartialCredentialsError, BotoCoreError) as exc:
            return {
                "raw_cloud_dump": [raw_assets],
                "warnings": [],
                "errors": [self._safe_aws_error("AWS authentication failed", exc)],
                "_scan_meta": [{"auth_failed": True, "assumed_role": bool(role_arn), "region": region}],
            }
        except Exception as exc:
            return {
                "raw_cloud_dump": [raw_assets],
                "warnings": [],
                "errors": [f"AWS authentication failed: {exc.__class__.__name__}"],
                "_scan_meta": [{"auth_failed": True, "assumed_role": bool(role_arn), "region": region}],
            }

        self._scan_s3(session, region, raw_assets, warnings)
        self._scan_glue(session, region, raw_assets, warnings)

        return {
            "raw_cloud_dump": [raw_assets],
            "warnings": warnings,
            "errors": errors,
            "_scan_meta": [{
                "auth_failed": False,
                "assumed_role": assumed_role,
                "region": region,
                "account_id": account_id,
                "s3_bucket_count": len(raw_assets["s3"]),
                "glue_job_count": len(raw_assets["glue_jobs"]),
                "glue_database_count": len(raw_assets["glue_databases"]),
                "glue_table_count": len(raw_assets["glue_tables"]),
            }],
        }

    def _build_session(self, settings: Any, region: str, role_arn: str | None) -> Tuple[Any, bool]:
        base_session = boto3.Session(
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            aws_session_token=getattr(settings, "aws_session_token", None),
            region_name=region,
        )

        if not role_arn:
            return base_session, False

        sts = base_session.client("sts", region_name=region)
        assumed = sts.assume_role(RoleArn=role_arn, RoleSessionName="dea-framework-scan")
        creds = assumed["Credentials"]
        return boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=region,
        ), True

    def _scan_s3(self, session: Any, region: str, raw_assets: Dict[str, List[Any]], warnings: List[str]) -> None:
        s3 = session.client("s3", region_name=region)
        try:
            buckets = s3.list_buckets().get("Buckets", [])
        except ClientError as exc:
            warnings.append(self._safe_aws_error("S3 scan skipped due to AccessDenied or missing permission", exc))
            return

        for bucket in buckets:
            name = bucket.get("Name")
            if not name:
                continue

            bucket_region = region
            prefixes: List[str] = []
            sample_keys: List[str] = []
            file_types = set()
            suggested_prefix = ""

            try:
                location = s3.get_bucket_location(Bucket=name).get("LocationConstraint")
                bucket_region = location or "us-east-1"
            except ClientError as exc:
                warnings.append(self._safe_aws_error(f"S3 bucket region lookup skipped for {name}", exc))

            bucket_client = session.client("s3", region_name=bucket_region)

            try:
                root = bucket_client.list_objects_v2(Bucket=name, MaxKeys=50, Delimiter="/")
                prefixes = [p.get("Prefix", "") for p in root.get("CommonPrefixes", []) if p.get("Prefix")]
                contents = root.get("Contents", [])
                sample_keys.extend([obj.get("Key") for obj in contents if obj.get("Key")])

                scan_prefixes = prefixes[:5] if prefixes else [""]
                for prefix in scan_prefixes:
                    page = bucket_client.list_objects_v2(Bucket=name, Prefix=prefix, MaxKeys=50)
                    for obj in page.get("Contents", []):
                        key = obj.get("Key")
                        if not key:
                            continue
                        if key not in sample_keys:
                            sample_keys.append(key)
                        ext = key.rsplit(".", 1)[-1].lower() if "." in key else ""
                        if ext in FILE_EXTENSIONS:
                            file_types.add(ext.upper())
                            if not suggested_prefix:
                                suggested_prefix = prefix or self._prefix_from_key(key)
            except ClientError as exc:
                warnings.append(self._safe_aws_error(f"S3 object listing skipped for {name}", exc))

            if not suggested_prefix and prefixes:
                suggested_prefix = prefixes[0]

            raw_assets["s3"].append({
                "id": f"aws || {name}",
                "configuration": {
                    "BucketName": name,
                    "Region": bucket_region,
                    "CreationDate": str(bucket.get("CreationDate", "")),
                    "Prefixes": prefixes,
                    "SampleObjectKeys": sample_keys[:25],
                    "DetectedFileTypes": sorted(file_types),
                    "SuggestedPath": f"s3://{name}/{suggested_prefix}".rstrip("/"),
                },
            })

    def _scan_glue(self, session: Any, region: str, raw_assets: Dict[str, List[Any]], warnings: List[str]) -> None:
        glue = session.client("glue", region_name=region)

        try:
            paginator = glue.get_paginator("get_jobs")
            for page in paginator.paginate():
                for job in page.get("Jobs", []):
                    command = job.get("Command", {}) or {}
                    raw_assets["glue_jobs"].append({
                        "id": f"aws-glue-job || {job.get('Name')}",
                        "configuration": {
                            "Name": job.get("Name"),
                            "Role": job.get("Role"),
                            "CommandName": command.get("Name"),
                            "ScriptLocation": command.get("ScriptLocation"),
                            "GlueVersion": job.get("GlueVersion"),
                            "CreatedOn": str(job.get("CreatedOn", "")),
                            "LastModifiedOn": str(job.get("LastModifiedOn", "")),
                            "MaxCapacity": job.get("MaxCapacity"),
                            "WorkerType": job.get("WorkerType"),
                            "NumberOfWorkers": job.get("NumberOfWorkers"),
                        },
                    })
        except ClientError as exc:
            warnings.append(self._safe_aws_error("Glue jobs scan skipped due to AccessDenied or missing permission", exc))

        try:
            db_paginator = glue.get_paginator("get_databases")
            for db_page in db_paginator.paginate():
                for database in db_page.get("DatabaseList", []):
                    db_name = database.get("Name")
                    raw_assets["glue_databases"].append({
                        "id": f"aws-glue-database || {db_name}",
                        "configuration": {
                            "Name": db_name,
                            "Description": database.get("Description"),
                            "LocationUri": database.get("LocationUri"),
                            "CreateTime": str(database.get("CreateTime", "")),
                        },
                    })

                    try:
                        table_paginator = glue.get_paginator("get_tables")
                        for table_page in table_paginator.paginate(DatabaseName=db_name):
                            for table in table_page.get("TableList", []):
                                storage = table.get("StorageDescriptor", {}) or {}
                                raw_assets["glue_tables"].append({
                                    "id": f"aws-glue-table || {db_name}.{table.get('Name')}",
                                    "configuration": {
                                        "DatabaseName": db_name,
                                        "Name": table.get("Name"),
                                        "TableType": table.get("TableType"),
                                        "Location": storage.get("Location"),
                                        "InputFormat": storage.get("InputFormat"),
                                        "OutputFormat": storage.get("OutputFormat"),
                                        "Columns": [
                                            {"Name": col.get("Name"), "Type": col.get("Type")}
                                            for col in storage.get("Columns", [])
                                        ],
                                        "CreateTime": str(table.get("CreateTime", "")),
                                        "UpdateTime": str(table.get("UpdateTime", "")),
                                    },
                                })
                    except ClientError as exc:
                        warnings.append(self._safe_aws_error(f"Glue tables scan skipped for database {db_name}", exc))
        except ClientError as exc:
            warnings.append(self._safe_aws_error("Glue databases scan skipped due to AccessDenied or missing permission", exc))

    @staticmethod
    def _prefix_from_key(key: str) -> str:
        if "/" not in key:
            return ""
        return key.rsplit("/", 1)[0] + "/"

    @staticmethod
    def _safe_aws_error(prefix: str, exc: Exception) -> str:
        if isinstance(exc, ClientError):
            err = exc.response.get("Error", {})
            code = err.get("Code", "ClientError")
            message = err.get("Message", "")
            return f"{prefix}: {code} - {message}"
        return f"{prefix}: {exc.__class__.__name__}"
