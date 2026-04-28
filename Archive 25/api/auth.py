import json
import secrets
import time
from typing import Any, Dict, List
from urllib.parse import urlparse

import msal
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from loguru import logger

from core.settings import ENV_FILE, settings

router = APIRouter(tags=["Auth"])

_AUTH_FLOW_TTL_SECONDS = 600
_auth_flows: Dict[str, Dict[str, Any]] = {}
_auth_results: Dict[str, Dict[str, Any]] = {}


def _popup_message_html(origin: str, payload: Dict[str, Any], status_code: int = 200) -> HTMLResponse:
    message = payload.get("error") or "Microsoft sign-in completed. You can close this window."
    payload_json = json.dumps(payload)
    return HTMLResponse(
        f"""
        <html>
          <body>
            <script>
              if (window.opener) {{
                window.opener.postMessage({payload_json}, {origin!r});
                window.close();
              }} else {{
                document.body.innerText = {message!r};
              }}
            </script>
            <p>{message}</p>
          </body>
        </html>
        """,
        status_code=status_code,
    )


def _cleanup_expired_flows() -> None:
    cutoff = time.time() - _AUTH_FLOW_TTL_SECONDS
    expired = [state for state, item in _auth_flows.items() if item.get("created_at", 0) < cutoff]
    for state in expired:
        _auth_flows.pop(state, None)
    expired_results = [key for key, item in _auth_results.items() if item.get("created_at", 0) < cutoff]
    for key in expired_results:
        _auth_results.pop(key, None)


def _authority() -> str:
    tenant = (settings.AZURE_TENANT_ID or "common").strip()
    if settings.AZURE_AUTHORITY:
        return settings.AZURE_AUTHORITY.rstrip("/")
    return f"https://login.microsoftonline.com/{tenant}"


def _redirect_uri(request: Request) -> str:
    if settings.AZURE_REDIRECT_URI:
        return settings.AZURE_REDIRECT_URI.strip()
    return str(request.url_for("get_a_token"))


def _scopes_for(target: str) -> List[str]:
    raw = settings.AZURE_SSO_SCOPES_FABRIC if target == "fabric" else settings.AZURE_SSO_SCOPES_AZURE
    return [scope.strip() for scope in (raw or "").split(",") if scope.strip()]


def _frontend_origin(origin: str) -> str:
    parsed = urlparse(origin or "")
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return "*"


def _build_msal_app() -> msal.ConfidentialClientApplication:
    if not settings.AZURE_CLIENT_ID or not settings.AZURE_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Azure SSO is not configured. Set AZURE_CLIENT_ID and AZURE_CLIENT_SECRET before using Microsoft sign-in.",
        )
    return msal.ConfidentialClientApplication(
        client_id=settings.AZURE_CLIENT_ID,
        client_credential=settings.AZURE_CLIENT_SECRET,
        authority=_authority(),
    )


@router.get("/auth/microsoft/status")
async def microsoft_auth_status():
    azure_client_id_present = bool(settings.AZURE_CLIENT_ID)
    azure_client_secret_present = bool(settings.AZURE_CLIENT_SECRET)
    azure_tenant_id_present = bool(settings.AZURE_TENANT_ID)
    app_registration_configured = bool(azure_client_id_present and azure_client_secret_present)
    logger.info(
        "Microsoft auth status checked. env_file={} AZURE_CLIENT_ID present={} AZURE_CLIENT_SECRET present={} AZURE_TENANT_ID present={} AZURE_REDIRECT_URI={}",
        str(ENV_FILE),
        azure_client_id_present,
        azure_client_secret_present,
        azure_tenant_id_present,
        settings.AZURE_REDIRECT_URI or "",
    )
    return {
        "app_registration_configured": app_registration_configured,
        "azure_client_id_present": azure_client_id_present,
        "azure_client_secret_present": azure_client_secret_present,
        "azure_tenant_id_present": azure_tenant_id_present,
        "azure_redirect_uri": settings.AZURE_REDIRECT_URI or "",
        "env_file": str(ENV_FILE),
        "azure_local_session_supported": bool(settings.AZURE_ENABLE_LOCAL_SESSION_FALLBACK),
        "fabric_local_session_supported": bool(settings.FABRIC_ENABLE_LOCAL_SESSION_FALLBACK),
        "fabric_requires_token_or_app_registration": not bool(settings.FABRIC_ENABLE_LOCAL_SESSION_FALLBACK),
    }


@router.get("/auth/microsoft/login")
async def microsoft_login(
    request: Request,
    target: str = Query("azure"),
    origin: str = Query("http://localhost:3000"),
):
    target_key = (target or "azure").strip().lower()
    frontend_origin = _frontend_origin(origin)
    if target_key not in {"azure", "fabric"}:
        return _popup_message_html(frontend_origin, {
            "source": "dea-msal",
            "success": False,
            "target": target_key,
            "error": "Unsupported Microsoft SSO target.",
        }, status_code=400)

    scopes = _scopes_for(target_key)
    if not scopes:
        return _popup_message_html(frontend_origin, {
            "source": "dea-msal",
            "success": False,
            "target": target_key,
            "error": f"No Microsoft scopes configured for target '{target_key}'.",
        }, status_code=500)

    try:
        app = _build_msal_app()
    except HTTPException as exc:
        return _popup_message_html(frontend_origin, {
            "source": "dea-msal",
            "success": False,
            "target": target_key,
            "error": str(exc.detail),
        }, status_code=200)

    state = secrets.token_urlsafe(24)
    flow = app.initiate_auth_code_flow(
        scopes=scopes,
        redirect_uri=_redirect_uri(request),
        state=state,
    )
    _cleanup_expired_flows()
    _auth_flows[state] = {
        "created_at": time.time(),
        "flow": flow,
        "target": target_key,
        "origin": frontend_origin,
        "auth_request_id": request.query_params.get("auth_request_id", ""),
    }
    return RedirectResponse(flow["auth_uri"])


@router.get("/login")
async def microsoft_login_shortcut(
    request: Request,
    target: str = Query("fabric"),
    origin: str = Query("http://localhost:3000"),
):
    return await microsoft_login(request=request, target=target, origin=origin)


@router.get("/auth/microsoft/start")
async def microsoft_start(
    request: Request,
    target: str = Query("azure"),
    origin: str = Query("http://localhost:3000"),
):
    target_key = (target or "azure").strip().lower()
    frontend_origin = _frontend_origin(origin)
    if target_key not in {"azure", "fabric"}:
        raise HTTPException(status_code=400, detail="Unsupported Microsoft SSO target.")

    scopes = _scopes_for(target_key)
    if not scopes:
        raise HTTPException(status_code=500, detail=f"No Microsoft scopes configured for target '{target_key}'.")

    app = _build_msal_app()
    state = secrets.token_urlsafe(24)
    auth_request_id = secrets.token_urlsafe(18)
    flow = app.initiate_auth_code_flow(
        scopes=scopes,
        redirect_uri=_redirect_uri(request),
        state=state,
    )
    _cleanup_expired_flows()
    _auth_flows[state] = {
        "created_at": time.time(),
        "flow": flow,
        "target": target_key,
        "origin": frontend_origin,
        "auth_request_id": auth_request_id,
    }
    _auth_results[auth_request_id] = {
        "created_at": time.time(),
        "status": "pending",
        "target": target_key,
    }
    return {
        "auth_request_id": auth_request_id,
        "login_url": f"{flow['auth_uri']}&auth_request_id={auth_request_id}",
    }


@router.get("/auth/microsoft/result")
async def microsoft_result(auth_request_id: str):
    _cleanup_expired_flows()
    result = _auth_results.get(auth_request_id)
    if not result:
        return {"status": "unknown"}
    return result


@router.get("/getAToken", response_class=HTMLResponse, name="get_a_token")
async def get_a_token(request: Request):
    state = request.query_params.get("state", "")
    stored = _auth_flows.pop(state, None)
    if not stored:
        raise HTTPException(status_code=400, detail="Microsoft sign-in session expired or is invalid. Try again.")

    app = _build_msal_app()
    try:
        result = app.acquire_token_by_auth_code_flow(
            stored["flow"],
            dict(request.query_params),
        )
    except ValueError as exc:
        payload = {
            "source": "dea-msal",
            "success": False,
            "target": stored["target"],
            "error": str(exc),
        }
        if stored.get("auth_request_id"):
            _auth_results[stored["auth_request_id"]] = {
                "created_at": time.time(),
                "status": "error",
                **payload,
            }
        return _popup_message_html(stored["origin"], payload, status_code=400)

    if "error" in result:
        message = result.get("error_description") or result.get("error") or "Authentication failed."
        payload = {
            "source": "dea-msal",
            "success": False,
            "target": stored["target"],
            "error": message,
        }
        if stored.get("auth_request_id"):
            _auth_results[stored["auth_request_id"]] = {
                "created_at": time.time(),
                "status": "error",
                **payload,
            }
        return _popup_message_html(stored["origin"], payload, status_code=400)

    account = result.get("id_token_claims", {}) or {}
    access_token = result.get("access_token", "")
    payload = {
        "source": "dea-msal",
        "success": True,
        "target": stored["target"],
        "accessToken": access_token,
        "expiresOn": str(result.get("expires_on", "")),
        "account": {
            "username": str(account.get("preferred_username") or account.get("email") or ""),
            "name": str(account.get("name") or ""),
        },
    }
    if stored.get("auth_request_id"):
        _auth_results[stored["auth_request_id"]] = {
            "created_at": time.time(),
            "status": "success",
            **payload,
        }
    return _popup_message_html(stored["origin"], payload)
