"""
Framework Detector
──────────────────
Detects data platform frameworks from payload content and DataHub entity metadata.

Supported frameworks (extendable via FRAMEWORK_PATTERNS):
  AWS:        Glue, Redshift, EMR, Athena, S3
  Azure:      ADF, Synapse, ADLS
  Snowflake, Databricks, dbt, Airflow, Spark, Kafka, Flink
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

from engine.detectors.base import AnalysisPayload, BaseDetector, DetectionResult

# ---------------------------------------------------------------------------
# Pattern registry — (display_name, [regex_patterns])
# ---------------------------------------------------------------------------
FRAMEWORK_PATTERNS: List[Tuple[str, List[str]]] = [
    # AWS
    ("AWS Glue",           [r"\bglue\b", r"gluejob", r"glue_job", r"aws.*glue", r"glue.*crawler"]),
    ("Amazon Redshift",    [r"\bredshift\b", r"redshift_cluster", r"jdbc.*redshift"]),
    ("Amazon EMR",         [r"\bemr\b", r"elastic.?map.?reduce", r"emr_cluster"]),
    ("Amazon Athena",      [r"\bathena\b", r"aws.*athena"]),
    # Azure
    ("Azure Data Factory", [r"\badf\b", r"azure.?data.?factory", r"datafactory", r"adf_pipeline"]),
    ("Azure Synapse",      [r"\bsynapse\b", r"synapse_analytics", r"azure.?synapse"]),
    ("Azure ADLS",         [r"\badls\b", r"data.?lake.?storage", r"abfss://", r"wasbs://"]),
    # Cloud-agnostic
    ("Snowflake",          [r"snowflake", r"snowflake_connector", r"snowflakecomputing"]),
    ("Databricks",         [r"\bdatabricks\b", r"dbfs://", r"databricks_cluster", r"dbx"]),
    ("Apache Spark",       [r"\bspark\b", r"pyspark", r"sparksession", r"spark_job"]),
    ("Apache Kafka",       [r"\bkafka\b", r"kafka_topic", r"confluent"]),
    ("Apache Flink",       [r"\bflink\b", r"flinkenvironment"]),
    ("Apache Airflow",     [r"\bairflow\b", r"dag\b", r"airflow_dag"]),
    ("dbt",                [r"\bdbt\b", r"dbt_project", r"dbt_model"]),
    ("Apache Hive",        [r"\bhive\b", r"hivecontext", r"hive_metastore"]),
    ("BigQuery",           [r"\bbigquery\b", r"bq_table", r"google.?bigquery"]),
]

# Combination rules — if all required frameworks are present, add a combo label
COMBINATION_RULES: List[Tuple[List[str], str]] = [
    (["AWS Glue", "Amazon Redshift"],     "Combo: Glue → Redshift"),
    (["Azure Data Factory", "Databricks"],"Combo: ADF → Databricks"),
    (["Azure Data Factory", "Azure Synapse"], "Combo: ADF → Synapse"),
    (["Apache Spark", "Snowflake"],       "Combo: Spark → Snowflake"),
    (["AWS Glue", "Amazon Athena"],       "Combo: Glue → Athena"),
]

# DataHub platform URN → display name mapping
DATAHUB_PLATFORM_MAP: Dict[str, str] = {
    "glue":        "AWS Glue",
    "redshift":    "Amazon Redshift",
    "emr":         "Amazon EMR",
    "athena":      "Amazon Athena",
    "adf":         "Azure Data Factory",
    "synapse":     "Azure Synapse",
    "snowflake":   "Snowflake",
    "databricks":  "Databricks",
    "spark":       "Apache Spark",
    "kafka":       "Apache Kafka",
    "flink":       "Apache Flink",
    "airflow":     "Apache Airflow",
    "dbt":         "dbt",
    "hive":        "Apache Hive",
    "bigquery":    "BigQuery",
}


class FrameworkDetector(BaseDetector):
    name = "framework_detector"

    def detect(self, payload: AnalysisPayload) -> DetectionResult:
        found: List[str] = []
        evidence: List[str] = []
        text = payload.all_text()

        # --- Pattern matching ---
        for display_name, patterns in FRAMEWORK_PATTERNS:
            for pattern in patterns:
                if re.search(pattern, text):
                    if display_name not in found:
                        found.append(display_name)
                        evidence.append(f"pattern match '{pattern}' → {display_name}")
                    break

        # --- DataHub entity platform enrichment ---
        for entity in payload.datahub_entities:
            platform = self._extract_platform(entity)
            if platform:
                mapped = DATAHUB_PLATFORM_MAP.get(platform.lower())
                if mapped and mapped not in found:
                    found.append(mapped)
                    evidence.append(f"DataHub entity platform='{platform}' → {mapped}")

        # --- Combination detection ---
        for required, combo_label in COMBINATION_RULES:
            if all(f in found for f in required):
                if combo_label not in found:
                    found.append(combo_label)
                    evidence.append(f"combination rule: {' + '.join(required)}")

        confidence = 0.95 if found else 0.0
        return DetectionResult(results=found, confidence=confidence, evidence=evidence)

    @staticmethod
    def _extract_platform(entity: Dict) -> str | None:
        """Pull platform out of a DataHub entity dict (several shapes)."""
        for key in ("platform", "dataPlatformInstance", "urn"):
            val = entity.get(key, "")
            if val:
                # urn:li:dataPlatform:glue  →  glue
                if "dataPlatform:" in str(val):
                    return str(val).split("dataPlatform:")[-1].split(",")[0].strip(")")
                return str(val)
        return None
