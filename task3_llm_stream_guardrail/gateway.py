"""Task 3: LLM gateway proxy that streams an OpenAI-compatible SSE response and redacts PII on the fly."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from redactor import StreamRedactor, redact

log = logging.getLogger("llm-gateway")


def _sse(obj: Any) -> bytes:
    return b"data: " + (obj if isinstance(obj, bytes) else json.dumps(obj).encode()) + b"\n\n"


def _chunk(template: dict, index: int, content: str) -> dict:
    return {**{k: v for k, v in template.items() if k != "choices"},
            "choices": [{"index": index, "delta": {"content": content}, "finish_reason": None}]}


def create_app(upstream_url: str, api_key: str | None = None, client: httpx.AsyncClient | None = None) -> FastAPI:
    app = FastAPI(title="LLM Gateway")
    http = client or httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0))
    key = api_key or os.environ.get("UPSTREAM_API_KEY", "")
    headers = {"Authorization": f"Bearer {key}"} if key else {}

    def err(status: int, msg: str) -> JSONResponse:
        return JSONResponse({"error": {"type": "gateway_error", "message": msg}}, status_code=status)

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        try:
            body = await request.json()
            assert isinstance(body, dict)
        except Exception:
            return err(400, "Request body must be a JSON object")

        if not body.get("stream"):
            try:
                r = await http.post(upstream_url, json=body, headers=headers)
                r.raise_for_status()
                data = r.json()
            except Exception:
                log.exception("upstream failure")
                return err(502, "Upstream provider error")
            for c in data.get("choices", []):
                m = c.get("message") or {}
                if isinstance(m.get("content"), str):
                    m["content"] = redact(m["content"])
            return JSONResponse(data)

        # Open upstream before replying so we can return a proper status on failure.
        req = http.build_request("POST", upstream_url, json=body, headers=headers)
        try:
            up = await http.send(req, stream=True)
            if up.status_code != 200:
                await up.aclose()
                return err(502, "Upstream provider error")
        except httpx.HTTPError:
            log.exception("upstream connect failure")
            return err(502, "Upstream provider unavailable")

        return StreamingResponse(_proxy(up), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app


async def _proxy(up: httpx.Response) -> AsyncIterator[bytes]:
    redactors: dict[int, StreamRedactor] = {}
    template: dict = {}
    try:
        async for line in up.aiter_lines():  # handles lines split across network chunks
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                for idx, r in redactors.items():  # flush held-back tails before closing
                    tail = r.flush()
                    if tail:
                        yield _sse(_chunk(template, idx, tail))
                yield b"data: [DONE]\n\n"
                return
            try:
                evt = json.loads(data)
            except json.JSONDecodeError:
                continue  # drop malformed events rather than leak unredacted bytes
            template = evt
            for ch in evt.get("choices", []):
                idx = ch.get("index", 0)
                red = redactors.setdefault(idx, StreamRedactor())
                delta = ch.get("delta") or {}
                content = delta.get("content")
                if isinstance(content, str):
                    delta["content"] = red.feed(content)
                if ch.get("finish_reason"):
                    tail = red.flush()
                    if tail:  # emit held tail *before* the finish event
                        yield _sse(_chunk(evt, idx, tail))
                # skip events that became empty and carry no other info
                if delta.get("content") == "" and not ch.get("finish_reason") and set(delta) <= {"content"}:
                    ch["_skip"] = True
            evt["choices"] = [c for c in evt.get("choices", []) if not c.pop("_skip", False)]
            if evt["choices"]:
                yield _sse(evt)
    finally:
        await up.aclose()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(os.environ.get("UPSTREAM_URL", "http://127.0.0.1:9100/v1/chat/completions")),
                host="127.0.0.1", port=8100)
