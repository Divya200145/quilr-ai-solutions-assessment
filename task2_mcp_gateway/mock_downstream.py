"""Minimal mock downstream MCP server (HTTP JSON-RPC). Records calls so tests can assert it was/wasn't hit."""
from fastapi import FastAPI, Request

app = FastAPI()
CALLS: list[dict] = []


def handle(m: dict):
    CALLS.append(m)
    if "id" not in m:
        return None
    if m["method"] == "tools/list":
        tools = [{"name": n, "inputSchema": {"type": "object"}} for n in ("get_status", "admin_reset_key")]
        return {"jsonrpc": "2.0", "id": m["id"], "result": {"tools": tools}}
    if m["method"] == "tools/call":
        return {"jsonrpc": "2.0", "id": m["id"],
                "result": {"content": [{"type": "text", "text": f"ran {m['params']['name']}"}], "isError": False}}
    return {"jsonrpc": "2.0", "id": m["id"], "error": {"code": -32601, "message": "Method not found"}}


@app.post("/mcp")
async def mcp(request: Request):
    body = await request.json()
    if isinstance(body, list):
        return [r for r in (handle(m) for m in body) if r is not None]
    return handle(body)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=9000)
