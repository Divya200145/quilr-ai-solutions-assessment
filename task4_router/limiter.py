"""Token-aware sliding-window rate limiter backed by on-disk SQLite.

Each admitted request inserts one row (tenant_hash, ts_ms, tokens). The window is the last
`window_ms`; the check-and-insert runs inside a BEGIN IMMEDIATE transaction so concurrent
requests (and even multiple processes sharing the file) can never oversubscribe the budget.
Expired rows are evicted on every check, so the table stays proportional to in-window traffic.
Tenant API keys are stored only as SHA-256 hashes.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Callable

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS token_usage(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  tokens INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_tenant_ts ON token_usage(tenant, ts_ms);
"""


@dataclass(frozen=True)
class Reservation:
    id: int
    tokens: int


@dataclass(frozen=True)
class Denied:
    retry_after_ms: int
    used: int


def tenant_id(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


class SlidingWindowLimiter:
    def __init__(self, path: str, limit: int = 50_000, window_ms: int = 60_000,
                 clock_ms: Callable[[], int] | None = None) -> None:
        self.path, self.limit, self.window_ms = path, limit, window_ms
        self._now = clock_ms or (lambda: int(time.time() * 1000))
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()  # serialises txns on our single connection

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self.path, isolation_level=None)  # autocommit; we manage txns
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._db.executescript(SCHEMA)

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def reserve(self, api_key: str, tokens: int) -> Reservation | Denied:
        """Atomically evict expired rows, check the budget, and record the reservation."""
        assert self._db
        t, now = tenant_id(api_key), self._now()
        cutoff = now - self.window_ms
        async with self._lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                await self._db.execute("DELETE FROM token_usage WHERE tenant=? AND ts_ms<=?", (t, cutoff))
                cur = await self._db.execute("SELECT COALESCE(SUM(tokens),0) FROM token_usage WHERE tenant=?", (t,))
                used = (await cur.fetchone())[0]
                if used + tokens > self.limit:
                    retry = await self._retry_after(t, now, used + tokens - self.limit)
                    await self._db.execute("COMMIT")
                    return Denied(retry_after_ms=retry, used=used)
                cur = await self._db.execute("INSERT INTO token_usage(tenant, ts_ms, tokens) VALUES (?,?,?)",
                                             (t, now, tokens))
                await self._db.execute("COMMIT")
                return Reservation(id=cur.lastrowid, tokens=tokens)
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    async def _retry_after(self, t: str, now: int, needed: int) -> int:
        """Earliest time at which enough old rows expire to free `needed` tokens."""
        cur = await self._db.execute("SELECT ts_ms, tokens FROM token_usage WHERE tenant=? ORDER BY ts_ms", (t,))
        freed = 0
        async for ts, tok in cur:
            freed += tok
            if freed >= needed:
                return max(1, ts + self.window_ms - now)
        return self.window_ms  # request alone exceeds the limit

    async def settle(self, r: Reservation, actual_tokens: int) -> None:
        """Replace the estimate with the provider-reported usage."""
        assert self._db
        async with self._lock:
            await self._db.execute("UPDATE token_usage SET tokens=? WHERE id=?", (actual_tokens, r.id))

    async def release(self, r: Reservation) -> None:
        """Refund a reservation (request failed, nothing was consumed)."""
        assert self._db
        async with self._lock:
            await self._db.execute("DELETE FROM token_usage WHERE id=?", (r.id,))

    async def usage(self, api_key: str) -> int:
        assert self._db
        t, cutoff = tenant_id(api_key), self._now() - self.window_ms
        async with self._lock:
            cur = await self._db.execute(
                "SELECT COALESCE(SUM(tokens),0) FROM token_usage WHERE tenant=? AND ts_ms>?", (t, cutoff))
            return (await cur.fetchone())[0]
