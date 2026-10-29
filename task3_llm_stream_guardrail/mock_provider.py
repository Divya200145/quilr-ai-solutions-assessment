"""Mock OpenAI-compatible streaming provider that splits PII across chunks."""
import asyncio
import json
import os

from fastapi import FastAPI
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI()
DELAY = float(os.environ.get("MOCK_DELAY", "0"))
CHUNKS = ["Hello! Mail me at jo", "hn.doe@exam", "ple.com or call. SSN 123-4", "5-6789, card 4111 11",
          "11 1111 1111 ok. Order 12345 is fine.", " Bye"]


def evt(content=None, finish=None, role=None):
    d = {}
    if role:
        d["role"] = role
    if content is not None:
        d["content"] = content
    return {"id": "x1", "object": "chat.completion.chunk", "model": "mock",
            "choices": [{"index": 0, "delta": d, "finish_reason": finish}]}


@app.post("/v1/chat/completions")
async def chat(body: dict):
    if not body.get("stream"):
        return JSONResponse({"choices": [{"index": 0, "message": {"role": "assistant", "content": "".join(CHUNKS)}}]})

    async def gen():
        yield f"data: {json.dumps(evt(role='assistant', content=''))}\n\n"
        for c in CHUNKS:
            await asyncio.sleep(DELAY)
            yield f"data: {json.dumps(evt(c))}\n\n"
        yield f"data: {json.dumps(evt(finish='stop'))}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=9100)
