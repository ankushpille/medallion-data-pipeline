from __future__ import annotations

from threading import RLock
from typing import Any, Dict, Optional, Tuple

_LOCK = RLock()
_AWS_CREDENTIALS: Dict[Tuple[str, str], Dict[str, Any]] = {}


def put_aws_credentials(client_name: str, bucket: str, credentials: Dict[str, Any], region: Optional[str] = None) -> None:
    if not client_name or not bucket or not credentials:
        return
    safe_creds = {
        "aws_access_key_id": credentials.get("access_key") or credentials.get("aws_access_key_id"),
        "aws_secret_access_key": credentials.get("secret_key") or credentials.get("aws_secret_access_key"),
        "aws_session_token": credentials.get("session_token") or credentials.get("aws_session_token"),
        "region_name": region or credentials.get("region") or credentials.get("aws_region") or "us-east-1",
        "role_arn": credentials.get("role_arn") or credentials.get("aws_role_arn"),
    }
    if not safe_creds["aws_access_key_id"] or not safe_creds["aws_secret_access_key"]:
        return
    key = (str(client_name).lower(), str(bucket).lower())
    with _LOCK:
        _AWS_CREDENTIALS[key] = safe_creds


def get_aws_credentials(client_name: str, bucket: str) -> Optional[Dict[str, Any]]:
    if not client_name or not bucket:
        return None
    key = (str(client_name).lower(), str(bucket).lower())
    with _LOCK:
        creds = _AWS_CREDENTIALS.get(key)
        return dict(creds) if creds else None
