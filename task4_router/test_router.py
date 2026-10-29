import asyncio
import json
import os
import sqlite3

import httpx
import pytest

from limiter import SlidingWindowLimiter, Reservation, Denied
from router import (GatewayError, Router, UpstreamBadRequest, UpstreamError, UpstreamRateLimited, estimate_tokens)
from app import create_app


class Fake:
    def __init__(self, name, behaviour="ok", delay=0.0, tokens=100):
        self.name, self.behaviour, self.delay, self.tokens, self.calls = name, behaviour, delay, tokens, 0

    async def complete(self, req):
        self.calls += 1
        await asyncio.sleep(self.delay)
        if self.behaviour == "429": raise UpstreamRateLimited()
        if self.behaviour == "500": raise UpstreamError("Traceback (most recent call last): secret-host:5432")
        if self.behaviour == "400": raise UpstreamBadRequest()
        if self.behaviour == "boom": raise RuntimeError("db password=hunter2 at /srv/app/x.py line 3")
        return {"choices": [{"text": f"from {self.name}"}], "usage": {"total_tokens": self.tokens}}


class Clock:
    def __init__(self): self.t = 1_000_000
    def __call__(self): return self.t


@pytest.fixture
async def lim(tmp_path):
    clk = Clock()
    l = SlidingWindowLimiter(str(tmp_path / "rl.db"), clock_ms=clk)
    l.clk = clk
    await l.open()
    yield l
    await l.close()


def req(**kw): return {"prompt": "hi", "max_tokens": 100, **kw}


# ---------- limiter ----------
async def test_sliding_window_and_eviction(lim):
    assert isinstance(await lim.reserve("k", 30_000), Reservation)
    lim.clk.t += 30_000
    assert isinstance(await lim.reserve("k", 20_000), Reservation)
    d = await lim.reserve("k", 1)
    assert isinstance(d, Denied) and d.used == 50_000
    assert d.retry_after_ms == 30_000                 # first 30k expires 30s from now
    lim.clk.t += 30_000                                # t0 row ages out exactly at window boundary
    assert isinstance(await lim.reserve("k", 30_000), Reservation)
    assert await lim.usage("k") == 50_000
    # eviction physically removes rows
    lim.clk.t += 120_000
    await lim.reserve("k", 1)
    n = sqlite3.connect(lim.path).execute("select count(*) from token_usage").fetchone()[0]
    assert n == 1


async def test_tenants_isolated_and_keys_hashed(lim):
    await lim.reserve("a", 50_000)
    assert isinstance(await lim.reserve("b", 50_000), Reservation)
    raw = sqlite3.connect(lim.path).execute("select tenant from token_usage").fetchall()
    assert all("a" != r[0] and len(r[0]) == 64 for r in raw)


async def test_concurrent_reservations_never_oversubscribe(lim):
    results = await asyncio.gather(*(lim.reserve("k", 1000) for _ in range(120)))
    assert sum(isinstance(r, Reservation) for r in results) == 50
    assert await lim.usage("k") == 50_000


async def test_oversized_single_request_denied(lim):
    d = await lim.reserve("k", 60_000)
    assert isinstance(d, Denied) and d.retry_after_ms == 60_000


async def test_persists_on_disk(tmp_path):
    p = str(tmp_path / "x.db"); clk = Clock()
    a = SlidingWindowLimiter(p, clock_ms=clk); await a.open(); await a.reserve("k", 40_000); await a.close()
    b = SlidingWindowLimiter(p, clock_ms=clk); await b.open()
    assert isinstance(await b.reserve("k", 20_000), Denied); await b.close()


# ---------- router ----------
async def test_primary_success_settles_actual_usage(lim):
    r = Router(lim, Fake("p", tokens=77), Fake("s"))
    out = await r.handle("k", req())
    assert out["gateway"]["served_by"] == "primary" and await lim.usage("k") == 77


async def test_failover_on_429(lim):
    p, s = Fake("p", "429"), Fake("s")
    out = await Router(lim, p, s).handle("k", req())
    assert out["gateway"]["served_by"] == "secondary" and p.calls == 1 and s.calls == 1
    assert await lim.usage("k") == 100                 # charged once, not twice


async def test_failover_on_timeout_3000ms(lim):
    p, s = Fake("p", delay=5), Fake("s")
    t0 = asyncio.get_running_loop().time()
    out = await Router(lim, p, s).handle("k", req())
    dt = asyncio.get_running_loop().time() - t0
    assert out["gateway"]["served_by"] == "secondary" and 2.9 < dt < 3.5


async def test_failover_on_5xx(lim):
    out = await Router(lim, Fake("p", "500"), Fake("s")).handle("k", req())
    assert out["gateway"]["served_by"] == "secondary"


async def test_both_fail_sanitized_and_budget_refunded(lim):
    r = Router(lim, Fake("p", "429"), Fake("s", "500"))
    with pytest.raises(GatewayError) as ei:
        await r.handle("k", req(), "rid1")
    e = ei.value
    body = json.dumps(e.payload("rid1"))
    assert e.status == 503 and "Traceback" not in body and "secret-host" not in body
    assert await lim.usage("k") == 0


async def test_unexpected_exception_sanitized(lim):
    r = Router(lim, Fake("p", "boom"), Fake("s"))
    with pytest.raises(GatewayError) as ei:
        await r.handle("k", req())
    assert ei.value.status == 500 and "hunter2" not in json.dumps(ei.value.payload("x"))
    assert await lim.usage("k") == 0


async def test_bad_request_no_failover(lim):
    s = Fake("s")
    with pytest.raises(GatewayError) as ei:
        await Router(lim, Fake("p", "400"), s).handle("k", req())
    assert ei.value.status == 400 and s.calls == 0


async def test_rate_limited_never_calls_providers(lim):
    p = Fake("p")
    r = Router(lim, p, Fake("s"))
    with pytest.raises(GatewayError) as ei:
        await r.handle("k", {"prompt": "x", "max_tokens": 60_000})
    assert ei.value.status == 429 and ei.value.retry_after_s == 60 and p.calls == 0


async def test_cancellation_refunds(lim):
    r = Router(lim, Fake("p", delay=2), Fake("s"))
    t = asyncio.create_task(r.handle("k", req()))
    await asyncio.sleep(0.1); t.cancel()
    with pytest.raises(asyncio.CancelledError): await t
    assert await lim.usage("k") == 0


def test_estimate_validation():
    assert estimate_tokens({"messages": [{"content": "abcdefgh"}], "max_tokens": 10}) == 12
    for bad in (0, -1, "5", True, 1.5):
        with pytest.raises(GatewayError): estimate_tokens({"prompt": "x", "max_tokens": bad})


# ---------- HTTP layer ----------
async def test_http_end_to_end(tmp_path):
    lim = SlidingWindowLimiter(str(tmp_path / "h.db"))
    app = create_app(Router(lim, Fake("p", "429"), Fake("s", tokens=40_000)))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
            h = {"Authorization": "Bearer tenant-1"}
            r = await c.post("/v1/completions", json=req(), headers=h)
            assert r.status_code == 200 and r.json()["gateway"]["served_by"] == "secondary"
            r = await c.post("/v1/completions", json={"prompt": "x", "max_tokens": 20_000}, headers=h)
            assert r.status_code == 429 and r.json()["error"]["code"] == "rate_limit_exceeded"
            assert int(r.headers["retry-after"]) > 0
            assert (await c.post("/v1/completions", json=req())).status_code == 401
            assert (await c.post("/v1/completions", content=b"[1]", headers=h)).status_code == 400
