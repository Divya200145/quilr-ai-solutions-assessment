# AI Field Developer / Solutions Engineer Assessment

Four independent Python 3.11+ solutions. Each folder has its own tests.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
for d in task1_mcp_server task2_mcp_gateway task3_llm_stream_guardrail task4_router; do (cd $d && python -m pytest -q); done
```
(Run each task's tests from inside its folder; modules import each other by bare name.)

## Task 1 – MCP server (`task1_mcp_server/server.py`)
Official `mcp` SDK, stdio transport, tools `get_customer_record` and `trigger_refund`.
- **Validation:** Pydantic models (`strict`, `extra=forbid`): `CUST-\d{5}` fullmatch (rejects trailing newline, lowercase),
  `amount` > 0, finite, not bool/string, `reason` >= 10 chars.
- **Protocol errors:** a raw `CallToolRequest` handler is registered so bad params raise `McpError(-32602)` and reach the client as a
  true JSON-RPC error (the SDK's `@call_tool` decorator would convert them into `isError` results). Unknown tool / missing arguments -> -32602.
  Business misses (unknown customer, refund > balance) are valid requests, so they return `isError: true` results.
- **STDIO isolation:** no `print`; logging configured to `sys.stderr`. The test spawns the server, parses every stdout line as JSON-RPC and checks logs are on stderr.

## Task 2 – MCP gateway (`task2_mcp_gateway/gateway.py`)
FastAPI + httpx reverse proxy at `POST /mcp`.
- Bearer token = HS256 JWT with `role` claim (signature + `exp` checked, `alg: none` rejected). `make_token()` mints demo tokens.
- `tools/list` is forwarded untouched. `tools/call` with `params.name` starting `admin_` (case/whitespace-insensitive) needs `role == admin`,
  otherwise the gateway answers `-32001 Unauthorized Tool Call` **without contacting downstream**.
- Also handled: JSON-RPC batches (mixed allowed/blocked), notifications (202), parse/invalid-request errors, missing/invalid token (401), downstream failure (502, sanitized).

## Task 3 – Streaming PII guardrail (`task3_llm_stream_guardrail/`)
OpenAI-compatible SSE proxy (`/v1/chat/completions`) that redacts emails, SSNs and Luhn-valid card numbers as `[REDACTED]`.
- `StreamRedactor` forwards text immediately and holds back **only the trailing run** that could be the start of a pattern split across chunks
  (email-token chars / digit-space-hyphen run), hard-capped at 320 chars. Memory is O(cap + chunk), regexes only scan held tail + new delta.
- Tail is flushed before the finish event / `[DONE]`. Multi-choice streams keep one redactor per choice. Malformed events are dropped (fail closed).
- Tests: streamed output equals whole-string redaction for 300 random chunkings; end-to-end test against a live mock provider checks incremental delivery and TTFT.
- Trade-off: a trailing space/digit run is delayed by one chunk; the cap bounds worst-case delay for pathological non-PII tokens.

## Task 4 – Rate-limit + fallback router (`task4_router/`)
- `limiter.py`: sliding window (50,000 tokens / 60 s per tenant) in on-disk SQLite (WAL). Check-and-reserve is one `BEGIN IMMEDIATE`
  transaction, so concurrency can't oversubscribe (test: 120 concurrent x 1000 tokens -> exactly 50 admitted). Expired rows are deleted on each check;
  keys stored as SHA-256. Reservation is estimated (~4 chars/token + `max_tokens`), then settled to provider-reported usage, or refunded on failure/cancellation.
- `router.py`: primary has a 3000 ms budget via `asyncio.timeout` (cancels the call; late replies are discarded). 429, timeout, 5xx/network errors fail over once to the
  secondary; 4xx request errors do not. Usage is charged once.
- Errors: `{"error": {"code", "message", "request_id", "retry_after_seconds?"}}` with fixed messages; upstream exceptions/stack traces are only logged server-side. 429 includes `Retry-After`.
- `app.py`: `POST /v1/completions` with `Authorization: Bearer <tenant key>`. Run with `PRIMARY_URL`, `SECONDARY_URL`, `RATE_DB` env vars.
