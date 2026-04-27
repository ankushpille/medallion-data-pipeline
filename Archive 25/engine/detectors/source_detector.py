"""
Source Detector
───────────────
Detects data source types from payload content.

Recognised surfaces: S3, JDBC variants, ADLS/WASB, REST APIs,
Google Cloud Storage, FTP/SFTP, local filesystem, on-prem databases.
"""
from __future__ import annotations

import re
from typing import List, Tuple

from engine.detectors.base import AnalysisPayload, BaseDetector, DetectionResult

SOURCE_PATTERNS: List[Tuple[str, List[str]]] = [
    # Object storage
    ("S3",              [r"s3://", r"s3a://", r"s3n://", r"\baws.*s3\b", r"boto3.*s3"]),
    ("ADLS Gen2",       [r"abfss://", r"azure.*data.*lake", r"\badls\b"]),
    ("WASB/Azure Blob", [r"wasbs://", r"wasb://", r"azure.*blob.*storage"]),
    ("GCS",             [r"gs://", r"google.*cloud.*storage", r"gcs_bucket"]),
    # JDBC / Relational
    ("JDBC/Redshift",   [r"jdbc:redshift", r"redshift.*jdbc"]),
    ("JDBC/PostgreSQL", [r"jdbc:postgresql", r"psycopg2", r"postgres.*conn"]),
    ("JDBC/MySQL",      [r"jdbc:mysql", r"pymysql", r"mysql.*connector"]),
    ("JDBC/MSSQL",      [r"jdbc:sqlserver", r"mssql", r"pyodbc.*sqlserver"]),
    ("JDBC/Oracle",     [r"jdbc:oracle", r"cx_oracle", r"oracle.*conn"]),
    ("JDBC/Generic",    [r"\bjdbc:", r"java\.sql\.DriverManager"]),
    # NoSQL / Cloud stores
    ("DynamoDB",        [r"\bdynamodb\b", r"aws.*dynamo"]),
    ("Cosmos DB",       [r"cosmosdb", r"azure.*cosmos"]),
    ("MongoDB",         [r"\bmongodb\b", r"pymongo", r"mongo://", r"mongodb\+srv"]),
    # Streaming
    ("Kafka Topic",     [r"kafka.*topic", r"bootstrap.*servers", r"confluent.*topic"]),
    ("Kinesis",         [r"\bkinesis\b", r"aws.*kinesis"]),
    ("Event Hubs",      [r"event.?hubs?", r"azure.*eventhub"]),
    # APIs
    ("REST API",        [r"https?://.*api\.", r"rest.*api", r"requests\.get\(", r"httpx\.", r"openapi"]),
    ("GraphQL",         [r"\bgraphql\b", r"gql\("]),
    ("SOAP/WS",         [r"wsdl://", r"\bsoap\b", r"zeep\."]),
    # File / FTP
    ("SFTP",            [r"\bsftp\b", r"paramiko", r"sftp://"]),
    ("FTP",             [r"\bftp://", r"ftplib"]),
    ("Local FS",        [r"file://", r"/mnt/", r"open\(.*['\"]r['\"]"]),
    # Snowflake-specific
    ("Snowflake Stage", [r"@.*stage", r"snowflake.*stage", r"copy into"]),
]


class SourceDetector(BaseDetector):
    name = "source_detector"

    def detect(self, payload: AnalysisPayload) -> DetectionResult:
        found: List[str] = []
        evidence: List[str] = []
        text = payload.all_text()

        for display_name, patterns in SOURCE_PATTERNS:
            for pattern in patterns:
                if re.search(pattern, text):
                    if display_name not in found:
                        found.append(display_name)
                        evidence.append(f"source pattern '{pattern}' → {display_name}")
                    break

        # Also scan DataHub upstream datasets
        for entity in payload.datahub_entities:
            upstreams = entity.get("upstreams", []) or entity.get("upstreamLineage", {}).get("upstreams", [])
            for u in upstreams:
                urn = u.get("dataset", u.get("urn", ""))
                tag = self._classify_urn(urn)
                if tag and tag not in found:
                    found.append(tag)
                    evidence.append(f"DataHub upstream lineage urn → {tag}")

        confidence = 0.90 if found else 0.0
        return DetectionResult(results=found, confidence=confidence, evidence=evidence)

    @staticmethod
    def _classify_urn(urn: str) -> str | None:
        urn_lower = urn.lower()
        mapping = {
            "s3":         "S3",
            "redshift":   "JDBC/Redshift",
            "snowflake":  "Snowflake Stage",
            "kafka":      "Kafka Topic",
            "mysql":      "JDBC/MySQL",
            "postgres":   "JDBC/PostgreSQL",
            "adls":       "ADLS Gen2",
            "bigquery":   "BigQuery",
        }
        for key, label in mapping.items():
            if key in urn_lower:
                return label
        return None
