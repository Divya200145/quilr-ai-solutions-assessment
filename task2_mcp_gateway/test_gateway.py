import httpx
import pytest

import mock_downstream as ds
from gateway import create_app, make_token

SECRET = "test-secret"


@pytest.fixture
def client():
    ds.CALLS.clear()
    down = httpx.AsyncClient(transport=httpx.ASGITransport(app=ds.app), base_url="http://ds")
    app = create_app("http://ds/mcp", SECRET, client=down)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


def hdr(role, secret=SECRET):
    return {"Authorization": f"Bearer {make_token(role, secret)}"}


def call(name, id_=1):
    return {"jsonrpc": "2.0", "id": id_, "method": "tools/call", "params": {"name": name, "arguments": {}}}


async def test_list_forwarded(client):
    r = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers=hdr("viewer"))
    assert r.status_code == 200 and len(r.json()["result"]["tools"]) == 2
    assert len(ds.CALLS) == 1


async def test_viewer_blocked_without_downstream_call(client):
    r = await client.post("/mcp", json=call("admin_reset_key"), headers=hdr("viewer"))
    assert r.json() == {"jsonrpc": "2.0", "id": 1, "error": {"code": -32001, "message": "Unauthorized Tool Call"}}
    assert ds.CALLS == []


async def test_admin_allowed(client):
    r = await client.post("/mcp", json=call("admin_reset_key"), headers=hdr("admin"))
    assert r.json()["result"]["content"][0]["text"] == "ran admin_reset_key"


async def test_viewer_non_admin_tool_allowed(client):
    r = await client.post("/mcp", json=call("get_status"), headers=hdr("viewer"))
    assert "result" in r.json()


async def test_case_bypass_blocked(client):
    r = await client.post("/mcp", json=call("ADMIN_reset_key"), headers=hdr("viewer"))
    assert r.json()["error"]["code"] == -32001 and ds.CALLS == []


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer junk"}, {"Authorization": "Basic abc"},
                                     hdr("admin", "wrong-secret")])
async def test_bad_auth(client, headers):
    r = await client.post("/mcp", json=call("get_status"), headers=headers)
    assert r.status_code == 401 and ds.CALLS == []


async def test_alg_none_rejected(client):
    import base64, json
    e = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()
    tok = f'{e({"alg": "none"})}.{e({"role": "admin"})}.'
    r = await client.post("/mcp", json=call("admin_reset_key"), headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 401 and ds.CALLS == []


async def test_batch_mixed(client):
    batch = [call("get_status", 1), call("admin_reset_key", 2)]
    r = await client.post("/mcp", json=batch, headers=hdr("viewer"))
    by_id = {x["id"]: x for x in r.json()}
    assert "result" in by_id[1] and by_id[2]["error"]["code"] == -32001
    assert [c["params"]["name"] for c in ds.CALLS] == ["get_status"]


async def test_malformed(client):
    r = await client.post("/mcp", content=b"{nope", headers={**hdr("admin"), "content-type": "application/json"})
    assert r.status_code == 400 and r.json()["error"]["code"] == -32700
    r = await client.post("/mcp", json={"foo": 1}, headers=hdr("admin"))
    assert r.json()["error"]["code"] == -32600


async def test_notification_forwarded(client):
    r = await client.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}, headers=hdr("viewer"))
    assert r.status_code == 202
