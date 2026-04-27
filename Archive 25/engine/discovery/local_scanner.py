"""
Walk a local directory tree and infer frameworks, important paths, and a draft
ingestion-oriented config without manual YAML entry.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Filename / dirname markers → detector hints (aligned with engine detectors vocabulary)
_MARKER_FILES: Dict[str, List[str]] = {
    "dbt_project.yml": ["dbt Jobs", "dbt"],
    "dbt_project.yaml": ["dbt Jobs", "dbt"],
    "profiles.yml": ["dbt Jobs"],
    "great_expectations.yml": ["Great Expectations"],
    "prefect.yaml": ["Prefect"],
    "docker-compose.yml": ["Docker"],
    "docker-compose.yaml": ["Docker"],
    "azure-pipelines.yml": ["ADF Pipelines"],
    "buildkite.yml": ["Buildkite"],
    "terraform.tf": ["Terraform"],
    "serverless.yml": ["AWS Lambda ETL"],
    "template.yaml": ["AWS Lambda ETL"],
    "samconfig.toml": ["AWS Lambda ETL"],
    "build.gradle": ["Apache Spark Jobs"],
    "pom.xml": ["Apache Spark Jobs"],
    "pyproject.toml": ["Apache Airflow DAGs"],
    "setup.py": ["Apache Airflow DAGs"],
}

_DIR_MARKERS: Dict[str, List[str]] = {
    "dags": ["Apache Airflow DAGs"],
    "airflow": ["Apache Airflow DAGs"],
    ".github": ["GitHub Actions"],
    "expectations": ["Great Expectations"],
    "models": ["dbt Jobs"],
    "pipelines": ["ADF Pipelines", "Azure Data Factory"],
    "terraform": ["Terraform"],
    "infra": ["Terraform"],
    "glue": ["AWS Glue Jobs"],
    "snowflake": ["Snowflake"],
}

_SKIP_DIR_NAMES: Set[str] = {
    ".git",
    ".svn",
    ".hg",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    ".mypy_cache",
    ".pytest_cache",
    "dist",
    "build",
    "target",
    ".idea",
    ".vscode",
    "eggs",
    ".eggs",
}


def _allowed_roots(settings: Any) -> List[Path]:
    roots: List[Path] = [Path.cwd().resolve()]
    raw = getattr(settings, "pipeline_workspace_roots", None) or os.environ.get("PIPELINE_WORKSPACE_ROOTS")
    if raw:
        for part in raw.split(";"):
            p = part.strip()
            if p:
                roots.append(Path(p).resolve())
    return roots


def resolve_safe_root(root_path: str, settings: Any) -> Path:
    """Reject paths outside configured allow-roots (defaults to cwd)."""
    candidate = Path(root_path).expanduser().resolve()
    allowed = _allowed_roots(settings)
    for base in allowed:
        try:
            candidate.relative_to(base)
            return candidate
        except ValueError:
            continue
    raise ValueError(
        f"Path must be under one of: {', '.join(str(b) for b in allowed)}"
    )


def scan_local_workspace(
    root: Path,
    *,
    max_depth: int = 6,
    max_files_recorded: int = 400,
) -> Dict[str, Any]:
    """
    Inspect a folder tree; return structured discovery for detectors + UI.

    Output is designed to be merged into AnalysisPayload.raw_json["local_discovery"].
    """
    frameworks: Set[str] = set()
    important_files: List[str] = []
    important_dirs: List[str] = []
    pipeline_folders: List[str] = []
    evidence: List[str] = []

    root = root.resolve()
    if not root.is_dir():
        raise ValueError(f"Not a directory: {root}")

    def rel(p: Path) -> str:
        try:
            return str(p.relative_to(root)).replace("\\", "/")
        except ValueError:
            return str(p)

    for dirpath, dirnames, filenames in os.walk(root, topdown=True):
        current = Path(dirpath)
        depth = len(current.relative_to(root).parts) if current != root else 0
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIR_NAMES]
        if depth >= max_depth:
            dirnames[:] = []

        rdir = rel(current)
        if rdir != ".":
            low = current.name.lower()
            if low in _DIR_MARKERS:
                for tag in _DIR_MARKERS[low]:
                    frameworks.add(tag)
                important_dirs.append(rdir)
                pipeline_folders.append(rdir)
                evidence.append(f"Directory signal `{rdir}/` → {', '.join(_DIR_MARKERS[low])}")

        for name in filenames:
            if len(important_files) >= max_files_recorded:
                break
            full = current / name
            low = name.lower()

            if low in _MARKER_FILES:
                for tag in _MARKER_FILES[low]:
                    frameworks.add(tag)
                important_files.append(rel(full))
                evidence.append(f"File `{rel(full)}` → {', '.join(_MARKER_FILES[low])}")
                if any(low.endswith(ext) for ext in (".yml", ".yaml", ".tf", ".toml")):
                    pipeline_folders.append(str(Path(rel(full)).parent).replace("\\", "/"))

            elif current.name.lower() == "dags" and low.endswith(".py"):
                frameworks.add("Apache Airflow DAGs")
                important_files.append(rel(full))
                evidence.append(f"DAG candidate `{rel(full)}`")

            elif "glue" in rdir.lower() and low.endswith(".py"):
                frameworks.add("AWS Glue Jobs")
                important_files.append(rel(full))
                evidence.append(f"Glue-style script `{rel(full)}`")

        if len(important_files) >= max_files_recorded:
            evidence.append(f"Stopped recording file paths after {max_files_recorded} entries (cap).")
            break

    # De-duplicate paths
    important_dirs = list(dict.fromkeys(important_dirs))[:80]
    important_files = list(dict.fromkeys(important_files))[:max_files_recorded]
    pipeline_folders = list(dict.fromkeys([p for p in pipeline_folders if p]))[:40]

    generated: Dict[str, Any] = {
        "version": 1,
        "root": str(root),
        "ingestion_layers": [
            {
                "name": "discovered_repo",
                "type": "inferred_from_layout",
                "pipeline_folders": pipeline_folders,
                "config_files": [f for f in important_files if f.endswith((".yml", ".yaml", ".json", ".toml", ".tf"))],
            }
        ],
        "sources": [
            {
                "type": "local_filesystem",
                "base_path": str(root),
                "relevant_subpaths": important_dirs[:25],
            }
        ],
    }

    return {
        "frameworks": sorted(frameworks),
        "important_directories": important_dirs,
        "important_files": important_files,
        "pipeline_folders": pipeline_folders,
        "generated_ingestion_config": generated,
        "evidence": evidence[:200],
        "stats": {
            "important_file_count": len(important_files),
            "important_dir_count": len(important_dirs),
            "framework_hint_count": len(frameworks),
        },
    }
