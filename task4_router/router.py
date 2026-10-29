"""Resilient completion router: rate limit -> primary (3s budget) -> secondary on 429/timeout/5xx."""
from __future__ import annotations

import asyncio
import logging
import math
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from limiter import Denied, SlidingWindowLimiter

log = logging.getLogger("router")

DEFAULT_MAX_TOKENS = 256


# ---- provider abstraction ----
class UpstreamRateLimited(Exception): ...
class UpstreamError(Exception): ...        # 5xx / network / malformed -> failover-worthy
class UpstreamBadRequest(Exception): ...   # 4xx other than 429 -> our request is wrong, don't fail over


class Provider(Protocol):
    name: str
    async def complete(self, request: dict) -> dict: ...


class HttpProvider:
    """OpenAI-style HTTP completion provider."""
    def __init__(self, name: str, url: str, api_key: str = "", client: httpx.AsyncClient | None = None):
        self.name, self.url = name, url
        self._h = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._c = client or httpx.AsyncClient()

    async def complete(self, request: dict) -> dict:
        try:
            r = await self._c.post(self.url, json=request, headers=self._h)
        except httpx.HTTPError as e:
            raise UpstreamError(type(e).__name__) from e
        if r.status_code == 429:
            raise UpstreamRateLimited()
        if r.status_code >= 500:
            raise UpstreamError(f"status {r.status_code}")
        if r.status_code >= 400:
            raise UpstreamBadRequest(f"status {r.status_code}")
        try:
            return r.json()
        except ValueError as e:
            raise UpstreamError("bad json") from e


# ---- standardised errors ----
@dataclass
class GatewayError(Exception):
    status: int
    code: str
    message: str
    retry_after_s: int | None = None

    def payload(self, request_id: str) -> dict:
        body: dict[str, Any] = {"code": self.code, "message": self.message, "request_id": request_id}
        if self.retry_after_s is not None:
            body["retry_after_seconds"] = self.retry_after_s
        return {"error": body}


def estimate_tokens(request: dict) -> int:
    """~4 chars/token for the prompt + requested completion budget (worst case)."""
    prompt = request.get("prompt")
    if prompt is None:
        prompt = " ".join(str(m.get("content", "")) for m in request.get("messages", []) if isinstance(m, dict))
    max_tokens = request.get("max_tokens", DEFAULT_MAX_TOKENS)
    if not isinstance(max_tokens, int) or isinstance(max_tokens, bool) or max_tokens <= 0:
        raise GatewayError(400, "invalid_request", "max_tokens must be a positive integer")
    return math.ceil(len(str(prompt)) / 4) + max_tokens


class Router:
    def __init__(self, limiter: SlidingWindowLimiter, primary: Provider, secondary: Provider,
                 primary_timeout_s: float = 3.0, secondary_timeout_s: float = 10.0) -> None:
        self.limiter, self.primary, self.secondary = limiter, primary, secondary
        self.t1, self.t2 = primary_timeout_s, secondary_timeout_s

    async def _call(self, p: Provider, req: dict, timeout: float) -> dict:
        # asyncio.timeout cancels the in-flight call on expiry, so a late primary response is discarded.
        async with asyncio.timeout(timeout):
            return await p.complete(req)

    async def handle(self, api_key: str, request: dict, request_id: str | None = None) -> dict:
        rid = request_id or uuid.uuid4().hex
        try:
            est = estimate_tokens(request)
            res = await self.limiter.reserve(api_key, est)
            if isinstance(res, Denied):
                raise GatewayError(429, "rate_limit_exceeded", "Token rate limit exceeded for this API key",
                                   retry_after_s=math.ceil(res.retry_after_ms / 1000))
            try:
                data, served_by = await self._route(request, rid)
            except BaseException:
                await self.limiter.release(res)  # nothing consumed (also on client cancellation)
                raise
            actual = (data.get("usage") or {}).get("total_tokens")
            if isinstance(actual, int) and actual >= 0:
                await self.limiter.settle(res, actual)
            data = dict(data)
            data["gateway"] = {"request_id": rid, "served_by": served_by}
            return data
        except GatewayError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("internal error request_id=%s", rid)  # details stay server-side
            raise GatewayError(500, "internal_error", "Internal gateway error") from None

    async def _route(self, req: dict, rid: str) -> tuple[dict, str]:
        try:
            return await self._call(self.primary, req, self.t1), "primary"
        except UpstreamBadRequest:
            raise GatewayError(400, "invalid_request", "The request was rejected by the model provider") from None
        except (UpstreamRateLimited, TimeoutError, UpstreamError) as e:
            log.warning("primary failed (%s) request_id=%s; failing over", type(e).__name__, rid)
        try:
            return await self._call(self.secondary, req, self.t2), "secondary"
        except UpstreamBadRequest:
            raise GatewayError(400, "invalid_request", "The request was rejected by the model provider") from None
        except (UpstreamRateLimited, TimeoutError, UpstreamError) as e:
            log.error("secondary failed (%s) request_id=%s", type(e).__name__, rid)
            raise GatewayError(503, "upstream_unavailable",
                               "Model providers are temporarily unavailable. Please retry shortly.",
                               retry_after_s=5) from None
