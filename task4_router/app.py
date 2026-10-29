"""HTTP front-end: POST /v1/completions  (Authorization: Bearer <tenant api key>)."""
from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from limiter import SlidingWindowLimiter
from router import GatewayError, HttpProvider, Router


def create_app(router: Router | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        r = router
        if r is None:
            lim = SlidingWindowLimiter(os.environ.get("RATE_DB", "ratelimit.db"))
            r = Router(lim, HttpProvider("primary", os.environ["PRIMARY_URL"], os.environ.get("PRIMARY_KEY", "")),
                       HttpProvider("secondary", os.environ["SECONDARY_URL"], os.environ.get("SECONDARY_KEY", "")))
        await r.limiter.open()
        app.state.router = r
        yield
        await r.limiter.close()

    app = FastAPI(title="LLM Router", lifespan=lifespan)

    @app.post("/v1/completions")
    async def completions(request: Request):
        rid = uuid.uuid4().hex
        try:
            auth = request.headers.get("authorization", "")
            scheme, _, key = auth.partition(" ")
            if scheme.lower() != "bearer" or not key.strip():
                raise GatewayError(401, "unauthorized", "Missing or invalid API key")
            try:
                body = await request.json()
            except Exception:
                body = None
            if not isinstance(body, dict):
                raise GatewayError(400, "invalid_request", "Request body must be a JSON object")
            data = await app.state.router.handle(key.strip(), body, rid)
            return JSONResponse(data)
        except GatewayError as e:
            headers = {"Retry-After": str(e.retry_after_s)} if e.retry_after_s is not None else {}
            return JSONResponse(e.payload(rid), status_code=e.status, headers=headers)

    return app
