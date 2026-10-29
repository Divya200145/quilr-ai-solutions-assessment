"""Task 1: MCP server over stdio with strict validation.

stdout is reserved for JSON-RPC. All logging goes to stderr.
Invalid params -> JSON-RPC error -32602 (not a tool-level isError result).
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
from typing import Any

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# Logging: stderr only. Never print() to stdout.
logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("customer-mcp")

CUSTOMER_ID_RE = re.compile(r"^CUST-\d{5}$")

# Mock data store
CUSTOMERS = {
    "CUST-00001": {"customer_id": "CUST-00001", "name": "Ada Lovelace", "balance": 120.50},
    "CUST-00002": {"customer_id": "CUST-00002", "name": "Alan Turing", "balance": 75.00},
}


def _check_id(v: str) -> str:
    if not CUSTOMER_ID_RE.fullmatch(v):
        raise ValueError("customer_id must match CUST-XXXXX (five digits)")
    return v


class GetCustomerRecordArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    customer_id: str

    _v = field_validator("customer_id")(_check_id)


class TriggerRefundArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    customer_id: str
    # allow_inf_nan=False rejects NaN/Infinity; gt=0 rejects zero/negative.
    amount: float = Field(gt=0, allow_inf_nan=False)
    reason: str = Field(min_length=10)

    _v = field_validator("customer_id")(_check_id)

    @field_validator("amount", mode="before")
    @classmethod
    def _no_bool(cls, v: Any) -> Any:
        # bool is a subclass of int; reject it explicitly. Accept int or float.
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ValueError("amount must be a number")
        return float(v)

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, v: str) -> str:
        if len(v.strip()) < 10:
            raise ValueError("reason must contain at least 10 non-whitespace-padded characters")
        return v


TOOLS = [
    types.Tool(
        name="get_customer_record",
        description="Fetch a customer record by id (format CUST-XXXXX).",
        inputSchema={
            "type": "object",
            "properties": {"customer_id": {"type": "string", "pattern": r"^CUST-\d{5}$"}},
            "required": ["customer_id"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="trigger_refund",
        description="Trigger a refund for a customer.",
        inputSchema={
            "type": "object",
            "properties": {
                "customer_id": {"type": "string", "pattern": r"^CUST-\d{5}$"},
                "amount": {"type": "number", "exclusiveMinimum": 0},
                "reason": {"type": "string", "minLength": 10},
            },
            "required": ["customer_id", "amount", "reason"],
            "additionalProperties": False,
        },
    ),
]

server: Server = Server("customer-mcp")


def _invalid_params(err: ValidationError) -> McpError:
    details = [
        {"field": ".".join(str(p) for p in e["loc"]) or "(root)", "message": e["msg"]}
        for e in err.errors(include_url=False, include_input=False, include_context=False)
    ]
    return McpError(types.ErrorData(code=types.INVALID_PARAMS,
                                    message="Invalid params", data={"errors": details}))


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return TOOLS


async def call_tool_handler(req: types.CallToolRequest) -> types.ServerResult:
    """Raw handler (instead of @server.call_tool) so protocol errors surface as
    JSON-RPC errors rather than being wrapped into isError tool results."""
    name = req.params.name
    args = req.params.arguments
    log.info("tools/call name=%s", name)

    if name not in ("get_customer_record", "trigger_refund"):
        raise McpError(types.ErrorData(code=types.INVALID_PARAMS, message=f"Unknown tool: {name}"))
    if args is None or not isinstance(args, dict):
        raise McpError(types.ErrorData(code=types.INVALID_PARAMS, message="Invalid params: arguments must be an object"))

    try:
        if name == "get_customer_record":
            a = GetCustomerRecordArgs.model_validate(args)
            rec = CUSTOMERS.get(a.customer_id)
            if rec is None:
                # Valid request, domain-level miss -> tool result error, not protocol error.
                return _result(f"Customer {a.customer_id} not found", is_error=True)
            return _result(_json(rec))
        a = TriggerRefundArgs.model_validate(args)
    except ValidationError as e:
        raise _invalid_params(e) from None

    rec = CUSTOMERS.get(a.customer_id)
    if rec is None:
        return _result(f"Customer {a.customer_id} not found", is_error=True)
    if a.amount > rec["balance"]:
        return _result("Refund amount exceeds available balance", is_error=True)
    rec["balance"] = round(rec["balance"] - a.amount, 2)
    log.info("refund issued customer=%s amount=%.2f", a.customer_id, a.amount)
    return _result(_json({"status": "refunded", "customer_id": a.customer_id,
                          "amount": a.amount, "remaining_balance": rec["balance"]}))


def _json(o: Any) -> str:
    import json
    return json.dumps(o)


def _result(text: str, is_error: bool = False) -> types.ServerResult:
    return types.ServerResult(types.CallToolResult(
        content=[types.TextContent(type="text", text=text)], isError=is_error))


server.request_handlers[types.CallToolRequest] = call_tool_handler


async def main() -> None:
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
