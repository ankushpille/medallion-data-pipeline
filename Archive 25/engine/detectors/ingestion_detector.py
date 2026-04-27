"""
Ingestion Detector
──────────────────
Detects ingestion engines / orchestration frameworks.
Also parses DataHub dataJobs and dataFlows entities.
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

from engine.detectors.base import AnalysisPayload, BaseDetector, DetectionResult

INGESTION_PATTERNS: List[Tuple[str, List[str]]] = [
    # AWS
    ("AWS Glue Jobs",          [r"gluejob", r"glue_job", r"aws_glue", r"glue.*crawler", r"glue.*trigger",
                                 r"'platform':\s*'glue'", r'"platform":\s*"glue"',
                                 r"platform.*glue", r"type.*gluejob"]),
    ("AWS Step Functions",     [r"step.?function", r"statemachine", r"aws.*sfn"]),
    ("AWS Lambda ETL",         [r"lambda.*handler", r"aws.*lambda.*etl"]),
    # Azure
    ("ADF Pipelines",          [r"adf_pipeline", r"azure.*data.*factory.*pipeline", r"pipeline.*activity"]),
    ("Azure Functions ETL",    [r"azure.*function.*etl", r"azurefunction"]),
    # Orchestration
    ("Apache Airflow DAGs",    [r"dag_id", r"\bdag\b", r"airflow", r"@dag\b", r"pythonoperator", r"bashoperator"]),
    ("Apache Spark Jobs",      [r"spark.*job", r"sparksession", r"pyspark.*main", r"spark-submit"]),
    ("Databricks Jobs",        [r"databricks.*job", r"dbutils\.", r"notebook.*run"]),
    # Streaming
    ("Kafka Connect",          [r"kafka.connect", r"kafka.*connector", r"debezium"]),
    ("Apache Flink Jobs",      [r"flink.*job", r"StreamExecutionEnvironment"]),
    ("AWS Kinesis Firehose",   [r"firehose", r"kinesis.*delivery"]),
    # ELT / Transformation
    ("dbt Jobs",               [r"dbt.?run", r"dbt.?test", r"dbt.*model", r"dbt_project\.yml"]),
    ("Informatica",            [r"\binformatica\b", r"infa_session"]),
    ("Talend",                 [r"\btalend\b", r"talend_job"]),
    ("Pentaho/Kettle",         [r"pentaho", r"\bkettle\b", r"\.ktr\b"]),
    ("Fivetran",               [r"\bfivetran\b"]),
    ("Airbyte",                [r"\bairbyte\b", r"airbyte_connection"]),
    ("Stitch",                 [r"\bstitch\b.*etl", r"stitchdata"]),
    ("Meltano",                [r"\bmeltano\b"]),
]

# DataHub dataJob/dataFlow type hints
DATAHUB_JOB_TYPE_MAP: Dict[str, str] = {
    "GlueJob":           "AWS Glue Jobs",
    "AirflowTask":       "Apache Airflow DAGs",
    "SparkJob":          "Apache Spark Jobs",
    "DataBricksJob":     "Databricks Jobs",
    "AzureDataFactory":  "ADF Pipelines",
}


class IngestionDetector(BaseDetector):
    name = "ingestion_detector"

    def detect(self, payload: AnalysisPayload) -> DetectionResult:
        found: List[str] = []
        evidence: List[str] = []
        text = payload.all_text()

        # --- Pattern matching ---
        for display_name, patterns in INGESTION_PATTERNS:
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    if display_name not in found:
                        found.append(display_name)
                        evidence.append(f"ingestion pattern '{pattern}' → {display_name}")
                    break

        # --- DataHub dataJobs / dataFlows ---
        for entity in payload.datahub_entities:
            entity_type = entity.get("type", entity.get("entityType", ""))
            if entity_type in ("dataJob", "dataFlow", "DATA_JOB", "DATA_FLOW"):
                job_type = entity.get("jobType", entity.get("orchestrator", ""))
                mapped = DATAHUB_JOB_TYPE_MAP.get(job_type)
                if mapped and mapped not in found:
                    found.append(mapped)
                    evidence.append(f"DataHub {entity_type} jobType='{job_type}' → {mapped}")

                # fallback: scan name/description
                name_str = str(entity.get("name", "")) + str(entity.get("description", ""))
                for display_name, patterns in INGESTION_PATTERNS:
                    for pat in patterns:
                        if re.search(pat, name_str, re.IGNORECASE):
                            if display_name not in found:
                                found.append(display_name)
                                evidence.append(f"DataHub entity name pattern → {display_name}")
                            break

        confidence = 0.95 if found else 0.0
        return DetectionResult(results=found, confidence=confidence, evidence=evidence)
