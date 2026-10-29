import json
import random
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from redactor import StreamRedactor, redact, MAX_HOLD

TEXT = ("Contact john.doe@example.com now; SSN 123-45-6789. Card 4111 1111 1111 1111 and 4111-1111-1111-1111, "
        "also 4111111111111111. Not PII: order 12345, phone-ish 555-1234, id 1234 5678, version 1.2.3, "
        "mail: bob.smith+tag@sub.mail.co.uk. Date 2024-01-02. Ends with a@b.io")


def stream_it(text, sizes):
    r, out, i = StreamRedactor(), [], 0
    while i < len(text):
        n = sizes()
        out.append(r.feed(text[i:i + n]))
        i += n
    out.append(r.flush())
    return "".join(out)


def test_reference_redaction():
    out = redact(TEXT)
    assert "john.doe" not in out and "123-45-6789" not in out and "4111" not in out
    assert "bob.smith" not in out and "a@b.io" not in out
    assert "12345" in out and "555-1234" in out and "2024-01-02" in out and "1234 5678" in out


def test_stream_equals_reference_for_any_chunking():
    ref = redact(TEXT)
    rnd = random.Random(7)
    for _ in range(300):
        assert stream_it(TEXT, lambda: rnd.randint(1, 9)) == ref
    for n in (1, 2, 3, 5, 50, len(TEXT)):
        assert stream_it(TEXT, lambda: n) == ref


def test_non_pii_text_not_delayed_and_memory_bounded():
    r = StreamRedactor()
    assert r.feed("Hello world, ") == "Hello world,"  # emitted immediately; only the trailing space is held
    assert r.flush() == " "
    big = "x" * 100_000
    got = r.feed(big)
    assert len(r._buf) <= MAX_HOLD and len(got) >= 100_000 - MAX_HOLD


def _free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


@pytest.fixture(scope="module")
def servers():
    import os
    os.environ["MOCK_DELAY"] = "0.15"
    import importlib, mock_provider
    importlib.reload(mock_provider)
    from gateway import create_app
    pp, gp = _free_port(), _free_port()
    gw = create_app(f"http://127.0.0.1:{pp}/v1/chat/completions")
    ss = []
    for app, port in ((mock_provider.app, pp), (gw, gp)):
        s = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
        threading.Thread(target=s.run, daemon=True).start(); ss.append(s)
    for port in (pp, gp):
        for _ in range(100):
            try: socket.create_connection(("127.0.0.1", port), 0.1).close(); break
            except OSError: time.sleep(0.05)
    yield gp
    for s in ss: s.should_exit = True


def test_e2e_stream_redacts_and_streams_incrementally(servers):
    url = f"http://127.0.0.1:{servers}/v1/chat/completions"
    t0, first, text, n_events = time.perf_counter(), None, "", 0
    with httpx.stream("POST", url, json={"stream": True, "messages": []}, timeout=20) as r:
        assert r.status_code == 200
        for line in r.iter_lines():
            if not line.startswith("data:") or line.endswith("[DONE]"):
                continue
            evt = json.loads(line[5:])
            c = evt["choices"][0]["delta"].get("content")
            if c:
                first = first or time.perf_counter() - t0
                text += c; n_events += 1
    assert "john.doe" not in text and "123-45-6789" not in text and "4111" not in text
    assert text.count("[REDACTED]") == 3 and "Order 12345 is fine." in text and text.endswith(" Bye")
    assert n_events >= 4          # not collapsed into one buffered response
    assert first < 0.6            # first token well before the ~0.9s full upstream stream


def test_non_stream_redacted():
    import mock_provider
    from gateway import create_app
    up = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_provider.app), base_url="http://m")
    app = create_app("http://m/v1/chat/completions", client=up)
    import asyncio
    async def go():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://g") as c:
            return (await c.post("/v1/chat/completions", json={"messages": []})).json()
    d = asyncio.run(go())
    assert "[REDACTED]" in d["choices"][0]["message"]["content"] and "john" not in d["choices"][0]["message"]["content"]
