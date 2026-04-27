"""
Data Quality Detector
─────────────────────
Detects data quality rules from:
  1. Great Expectations (suites, checkpoints, expectation types)
  2. SQL-based validations (COUNT checks, NOT NULL, BETWEEN, custom assertions)
  3. Custom DQ frameworks (Soda, dbt tests, Monte Carlo, custom Python)
  4. DataHub assertion entities
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from engine.detectors.base import AnalysisPayload, BaseDetector, DetectionResult

# ---------------------------------------------------------------------------
# Great Expectations expectation type → human label
# ---------------------------------------------------------------------------
GE_EXPECTATION_MAP: Dict[str, str] = {
    "expect_column_values_to_not_be_null":          "GE: Not Null check",
    "expect_column_values_to_be_unique":            "GE: Uniqueness check",
    "expect_column_values_to_be_between":           "GE: Range check",
    "expect_column_values_to_match_regex":          "GE: Regex pattern check",
    "expect_column_values_to_be_in_set":            "GE: Value set check",
    "expect_table_row_count_to_be_between":         "GE: Row count range",
    "expect_column_mean_to_be_between":             "GE: Column mean check",
    "expect_column_stdev_to_be_between":            "GE: Column stdev check",
    "expect_column_pair_values_to_be_equal":        "GE: Column pair equality",
    "expect_multicolumn_sum_to_equal":              "GE: Multicolumn sum check",
    "expect_column_values_to_be_of_type":           "GE: Data type check",
    "expect_table_columns_to_match_ordered_list":   "GE: Schema validation",
}

# Patterns for Great Expectations suite / checkpoint markers
GE_SUITE_PATTERNS: List[str] = [
    r"great.?expectations",
    r"ge_suite",
    r"suite_name.*?'([^']+)'",
    r'suite_name.*?"([^"]+)"',
    r"checkpoint_name.*?'([^']+)'",
    r'checkpoint_name.*?"([^"]+)"',
    r"datacontext\(\)",
    r"batchrequest\(",
    r"\.run_checkpoint\(",
    r"context\.get_expectation_suite\(",
]

# SQL-based DQ patterns
SQL_DQ_PATTERNS: List[Tuple[str, str]] = [
    (r"count\s*\(\s*\*\s*\)\s*(>|<|=|!=|>=|<=)\s*\d+",       "SQL: Row count assertion"),
    (r"where\s+\w+\s+is\s+null",                               "SQL: Null check"),
    (r"sum\s*\([^)]+\)\s*(>|<|=|!=|>=|<=)\s*\d+",             "SQL: Sum assertion"),
    (r"distinct\s+count",                                      "SQL: Distinct count check"),
    (r"between\s+\d+\s+and\s+\d+",                            "SQL: Range/BETWEEN check"),
    (r"not\s+in\s*\(",                                         "SQL: NOT IN set check"),
    (r"assert\b.*\bcount\b",                                   "SQL: Assert row count"),
    (r"dq_check|data_quality_check|quality_assertion",         "SQL: Custom DQ check"),
    (r"freshness_check|freshness_sla",                         "SQL: Data freshness check"),
    (r"duplicate.*check|check.*duplicate",                     "SQL: Duplicate check"),
    (r"referential.*integrity|foreign.*key.*check",            "SQL: Referential integrity"),
    (r"schema.*validation|column.*exists",                     "SQL: Schema validation"),
]

# Third-party / custom DQ framework markers
CUSTOM_DQ_PATTERNS: List[Tuple[str, str]] = [
    (r"\bsoda\b",                     "Soda DQ"),
    (r"soda.?cloud|sodacl",           "Soda Cloud"),
    (r"dbt.*test",                    "dbt Tests"),
    (r"\.test\(.*schema\)",           "dbt Schema Tests"),
    (r"monte.?carlo",                 "Monte Carlo"),
    (r"acceldata",                    "Acceldata"),
    (r"datafold",                     "Datafold"),
    (r"bigeye",                       "Bigeye"),
    (r"anomalo",                      "Anomalo"),
    (r"lightup",                      "Lightup"),
    (r"re_data",                      "re_data"),
    (r"pandera\.",                     "Pandera (Python DQ)"),
    (r"pydantic.*validator",          "Pydantic validation"),
    (r"cerberus\.",                    "Cerberus (Python DQ)"),
    (r"custom_dq|CustomDQCheck",      "Custom DQ Framework"),
]


class DQDetector(BaseDetector):
    name = "dq_detector"

    def detect(self, payload: AnalysisPayload) -> DetectionResult:
        found: List[str] = []
        evidence: List[str] = []
        text = payload.all_text()

        # --- Great Expectations ---
        self._detect_ge(text, payload.raw_json, found, evidence)

        # --- SQL validations ---
        self._detect_sql(text, found, evidence)

        # --- Custom / third-party DQ ---
        self._detect_custom(text, found, evidence)

        # --- DataHub assertion entities ---
        self._detect_datahub_assertions(payload.datahub_entities, found, evidence)

        confidence = 0.85 if found else 0.0
        return DetectionResult(results=found, confidence=confidence, evidence=evidence)

    # ------------------------------------------------------------------

    def _detect_ge(
        self,
        text: str,
        raw: Dict[str, Any],
        found: List[str],
        evidence: List[str],
    ) -> None:
        # Check for GE presence
        for pattern in GE_SUITE_PATTERNS:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                label = (
                    f"Great Expectations Suite: {m.group(1)}"
                    if m.lastindex
                    else "Great Expectations"
                )
                if label not in found:
                    found.append(label)
                    evidence.append(f"GE pattern '{pattern}'")
                break

        # Extract individual expectation types from raw JSON
        self._scan_ge_expectations(raw, found, evidence)

    def _scan_ge_expectations(
        self,
        obj: Any,
        found: List[str],
        evidence: List[str],
        _depth: int = 0,
    ) -> None:
        if _depth > 10:
            return
        if isinstance(obj, dict):
            expectation_type = obj.get("expectation_type")
            if expectation_type:
                label = GE_EXPECTATION_MAP.get(
                    expectation_type, f"GE: {expectation_type}"
                )
                if label not in found:
                    found.append(label)
                    evidence.append(f"GE expectation type '{expectation_type}'")
            for v in obj.values():
                self._scan_ge_expectations(v, found, evidence, _depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                self._scan_ge_expectations(item, found, evidence, _depth + 1)

    def _detect_sql(
        self, text: str, found: List[str], evidence: List[str]
    ) -> None:
        for pattern, label in SQL_DQ_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                if label not in found:
                    found.append(label)
                    evidence.append(f"SQL DQ pattern '{pattern}'")

    def _detect_custom(
        self, text: str, found: List[str], evidence: List[str]
    ) -> None:
        for pattern, label in CUSTOM_DQ_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                if label not in found:
                    found.append(label)
                    evidence.append(f"custom DQ pattern '{pattern}'")

    def _detect_datahub_assertions(
        self,
        entities: List[Dict[str, Any]],
        found: List[str],
        evidence: List[str],
    ) -> None:
        for entity in entities:
            if entity.get("type") in ("assertion", "ASSERTION"):
                assertion_info = entity.get("assertionInfo", {})
                a_type = assertion_info.get("type", "")
                label = f"DataHub Assertion: {a_type}" if a_type else "DataHub Assertion"
                if label not in found:
                    found.append(label)
                    evidence.append(f"DataHub assertion entity type='{a_type}'")
