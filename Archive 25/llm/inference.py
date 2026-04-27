"""
LLM Inference Layer
────────────────────
Optional enrichment layer that sends payload + rule-based results to
an LLM (Anthropic Claude) for dynamic inference and gap-filling.

Only runs when:
  - settings.llm_enabled is True, and
  - the /analyze request has use_llm=True (opt-in from UI or API client).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from config.settings import settings

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an expert Cloud Configuration Extraction AI. Your mission is to extract precise, evidence-backed SOURCE, INGESTION, and TARGET configurations from discovered cloud topology.

MISSION MANIFESTO:
1. Accuracy > Completeness.
2. DO NOT assume. DO NOT guess relationships. 
3. If data is not explicitly available in configurations or verified code analysis, use "UNKNOWN".
4. Only create connections (SOURCE -> INGESTION -> TARGET) if proven via trigger mappings, SDK calls, or IAM policies.

5. Ensure all raw configuration data from the scanner is preserved and structured in the "detailed_inventory" section for every discovered resource.

EXTRACTION SCHEMA:

1. SOURCE (API/Event Entry):
   - Use APIGW resources, methods, and integrations.
   - Extract: { "source_type": "API", "service": "API Gateway", "endpoint": "<invoke_url>", "method": [...], "integration_type": "Lambda Proxy", "target_lambda": "func_name", "auth": "IAM", "request_schema": "..." }

2. INGESTION (Compute):
   - Use Function config and event source mappings.
   - Extract: { "ingestion_type": "Lambda Processing", "function_name": "name", "runtime": "...", "timeout": 3, "memory": 128, "handler": "...", "event_source": "API Gateway", "downstream_targets": [...], "transformation": "UNKNOWN" }

3. TARGET (Storage):
   - Use Bucket location and validated write patterns.
   - Extract: { "target_type": "Object Storage", "service": "S3", "buckets": [...], "write_pattern": "PUT", "partitioning": "UNKNOWN", "format": "JSON" }

Respond ONLY with a valid JSON object matching this schema exactly.
{
  "confidence": {"framework": 0.9, "source": 0.8},
  "expert_extraction": {
      "source": {...},
      "ingestion": {...},
      "target": {...}
  },
  "detailed_inventory": [
      { "id": "...", "service": "...", "region": "...", "config": { ... } }
  ],
  "pipelines": [...],
  "nodes": [...],
  "validation": { "summary_badge": "Status: Verified" }
}
"""


def llm_infer(
    payload: Any,
    rule_based_results: Dict[str, List[str]],
) -> Optional[Dict[str, Any]]:
    """
    Call the local Ollama API (DeepSeek-R1) and return structured inference JSON.
    Returns None if LLM is disabled or call fails.
    """
    if not settings.llm_enabled:
        logger.info("LLM inference skipped — LLM_ENABLED is false.")
        return None

    import httpx

    user_message = _build_user_message(payload, rule_based_results)

    try:
        # Ollama API supports formatting output as json
        ollama_url = f"{settings.ollama_base_url.rstrip('/')}/api/generate"
        logger.info(f"Pinging Local LLM at {ollama_url} using model {settings.llm_model}...")
        
        body: Dict[str, Any] = {
            "model": settings.llm_model,
            "prompt": f"{_SYSTEM_PROMPT}\n\n{user_message}",
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.1,
                "num_ctx": 4096,
                "num_predict": 8192,
            },
        }
        response = httpx.post(ollama_url, json=body, timeout=600.0)
        if response.status_code >= 400:
            # Older Ollama builds may reject ``format``; retry without it.
            body.pop("format", None)
            response = httpx.post(ollama_url, json=body, timeout=600.0)
        response.raise_for_status()
        raw_text = response.json().get("response", "").strip()
        logger.info("Local LLM generated response successfully.")
        parsed = _safe_parse_json(raw_text)
        if parsed is None:
            logger.warning("LLM returned text that could not be parsed as JSON; skipping inference overlay.")
        return parsed
    except Exception as exc:
        logger.warning(f"Local LLM inference failed: {exc}")
        return None


def llm_infer_data_pipeline_reasoning(report: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Ask the local LLM to explain why values were derived and why some remain missing.
    Returns a small JSON object or None.
    """
    if not settings.llm_enabled:
        logger.info("Data pipeline reasoning skipped — LLM_ENABLED is false.")
        return None

    import httpx

    prompt = (
        "You are a senior data pipeline reviewer. "
        "Given a structured DataPipeline extraction, produce a compact JSON object with keys "
        "'summary', 'field_reasoning', and 'confidence'. "
        "Field reasoning must explain why the extracted values are credible and why any missing fields remain missing. "
        "Do not invent fields outside the provided report.\n\n"
        f"{json.dumps(report, indent=2, default=str)}"
    )

    try:
        ollama_url = f"{settings.ollama_base_url.rstrip('/')}/api/generate"
        body: Dict[str, Any] = {
            "model": settings.llm_model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.1,
                "num_ctx": 4096,
                "num_predict": 1024,
            },
        }
        response = httpx.post(ollama_url, json=body, timeout=180.0)
        if response.status_code >= 400:
            body.pop("format", None)
            response = httpx.post(ollama_url, json=body, timeout=180.0)
        response.raise_for_status()
        raw_text = response.json().get("response", "").strip()
        parsed = _safe_parse_json(raw_text)
        if parsed is None:
            logger.warning("Local LLM reasoning output could not be parsed as JSON.")
        return parsed
    except Exception as exc:
        logger.warning(f"Local LLM data pipeline reasoning failed: {exc}")
        return None

def _build_user_message(payload: Any, rule_based: Dict[str, List[str]]) -> str:
    context = {
        "metadata": getattr(payload, "metadata", {}),
        "config": getattr(payload, "config", {}),
        "raw_json_sample": _truncate(getattr(payload, "raw_json", {})),
        "rule_based_results": rule_based
    }
    return (
        "System: You are an expert Cloud Data Architect. Analyze the provided multi-cloud inventory \n"
        "and identify data pipelines, ETL flows, and security posture. \n\n"
        "Key areas to highlight:\n"
        "- **S3 Connectivity**: Check for Public Access Blocks and Bucket Policies to surface security risks.\n"
        "- **Azure Fabric Integrated Flows**: Map Lakehouse OneLake paths to downstream ingestion.\n"
        "- **Service Dependencies**: How API Gateway, Lambda, and Functions orchestrate data.\n\n"
        "Output a JSON mapping strictly following the AnalyzeResponse model.\n"
        "Include a detailed 'architectural_narrative' that summarizes the entire landscape.\n\n"
        + json.dumps(context, indent=2, default=str)
    )

def _truncate(obj: Any, max_chars: int = 15000) -> Any:
    """Truncate large objects to avoid huge LLM prompts."""
    serialized = json.dumps(obj, default=str)
    if len(serialized) > max_chars:
        return serialized[:max_chars] + "... [truncated]"
    return obj

def _safe_parse_json(text: str) -> Optional[Dict[str, Any]]:
    # 1. Strip DeepSeek thoughts <think>...</think>
    import re
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    
    # 2. Extract JSON block from markdown fences or just raw text
    # This regex looks for ```json { ... } ``` or just { ... }
    json_match = re.search(r"(\{[\s\S]*\})", text)
    if json_match:
        text = json_match.group(1)
    else:
        text = text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()

    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError as exc:
        logger.warning(f"LLM response is not valid JSON: {exc} | raw={text[:400]}")
        return None
