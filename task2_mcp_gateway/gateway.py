"""Task 2: MCP security gateway (HTTP JSON-RPC reverse proxy).

Bearer token (HS256 JWT with a `role` claim) -> role. tools/list is forwarded
transparently; tools/call on admin_* tools requires role == admin, otherwise the
gateway answers with JSON-RPC error -32001 without contacting the downstream server.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response

log = logging.getLogger("mcp-gateway")

UNAUTHORIZED_TOOL = -32001
AUTH_REQUIRED = -32002
INVALID_REQUEST = -32600
PARSE_ERROR = -32700
UPSTREAM_ERROR = -32003
ADMIN_PREFIX = "admin_"


# ---------- token handling ----------
def _b64d(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def make_token(role: str, secret: str, ttl: int = 3600) -> str:
    """Helper to mint demo tokens."""
    h = _b64e(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    p = _b64e(json.dumps({"role": role, "exp": int(time.time()) + ttl}).encode())
    sig = _b64e(hmac.new(secret.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest())
    return f"{h}.{p}.{sig}"


def role_from_token(token: str, secret: str) -> str | None:
    """Return the role if the token is a valid, unexpired HS256 JWT; else None."""
    try:
        h, p, s = token.split(".")
        if json.loads(_b64d(h)).get("alg") != "HS256":  # blocks alg=none downgrade
            return None
        expected = hmac.new(secret.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _b64d(s)):
            return None
        claims = json.loads(_b64d(p))
        if "exp" in claims and claims["exp"] < time.time():
            return None
        role = claims.get("role")
        return role if isinstance(role, str) else None
    except Exception:
        return None


# ---------- JSON-RPC helpers ----------
def rpc_error(id_: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}


def is_admin_tool(name: Any) -> bool:
    return isinstance(name, str) and name.strip().lower().startswith(ADMIN_PREFIX)


def authorize(msg: Any, role: str) -> dict | None:
    """Return a JSON-RPC error dict if `msg` must be blocked, else None."""
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0" or not isinstance(msg.get("method"), str):
        return rpc_error(msg.get("id") if isinstance(msg, dict) else None, INVALID_REQUEST, "Invalid Request")
    if msg["method"] == "tools/call":
        params = msg.get("params")
        name = params.get("name") if isinstance(params, dict) else None
        if is_admin_tool(name) and role != "admin":
            return rpc_error(msg.get("id"), UNAUTHORIZED_TOOL, "Unauthorized Tool Call")
    return None


def create_app(downstream_url: str, jwt_secret: str | None = None,
               client: httpx.AsyncClient | None = None) -> FastAPI:
    secret = jwt_secret or os.environ.get("GATEWAY_JWT_SECRET", "dev-secret-change-me")
    app = FastAPI(title="MCP Gateway")
    http = client or httpx.AsyncClient(timeout=10.0)

    async def forward(payload: Any) -> Any:
        r = await http.post(downstream_url, json=payload, headers={"content-type": "application/json"})
        return r.json() if r.content else None

    @app.post("/mcp")
    async def mcp(request: Request) -> Response:
        auth = request.headers.get("authorization", "")
        scheme, _, token = auth.partition(" ")
        role = role_from_token(token.strip(), secret) if scheme.lower() == "bearer" else None
        if role is None:
            return _json(rpc_error(None, AUTH_REQUIRED, "Authentication required"), 401,
                         {"WWW-Authenticate": "Bearer"})
        try:
            body = await request.json()
        except Exception:
            return _json(rpc_error(None, PARSE_ERROR, "Parse error"), 400)

        is_batch = isinstance(body, list)
        msgs = body if is_batch else [body]
        if is_batch and not msgs:
            return _json(rpc_error(None, INVALID_REQUEST, "Invalid Request"), 400)

        blocked: list[dict] = []
        allowed: list[Any] = []
        for m in msgs:
            err = authorize(m, role)
            if err is None:
                allowed.append(m)
            elif err["error"]["code"] == UNAUTHORIZED_TOOL and "id" not in m:
                log.warning("dropped unauthorized notification role=%s", role)  # notifications get no reply
            else:
                log.warning("blocked role=%s method=%s", role, m.get("method") if isinstance(m, dict) else None)
                blocked.append(err)

        replies: list[Any] = list(blocked)
        if allowed:
            try:
                up = await forward(allowed if is_batch else allowed[0])
            except (httpx.HTTPError, ValueError):
                log.exception("downstream failure")
                first_id = allowed[0].get("id") if isinstance(allowed[0], dict) else None
                replies.append(rpc_error(first_id, UPSTREAM_ERROR, "Downstream server unavailable"))
                return _json(replies if is_batch else replies[0], 502)
            if up is None:  # downstream returned 202/no body (notification)
                if not replies:
                    return Response(status_code=202)
            elif isinstance(up, list):
                replies.extend(up)
            else:
                replies.append(up)

        if not replies:
            return Response(status_code=202)
        return _json(replies if is_batch else replies[0], 200)

    return app


def _json(obj: Any, status: int, headers: dict | None = None) -> Response:
    return Response(json.dumps(obj), status_code=status, media_type="application/json", headers=headers)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(os.environ.get("DOWNSTREAM_URL", "http://127.0.0.1:9000/mcp")),
                host="127.0.0.1", port=8000)
