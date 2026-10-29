"""Spawns the server as a subprocess and talks raw JSON-RPC over stdio.
Asserts every stdout line is valid JSON-RPC (stdout isolation) and error codes are right."""
import json
import subprocess
import sys
import pathlib

SERVER = str(pathlib.Path(__file__).with_name("server.py"))


def run(messages):
    init = [
        {"jsonrpc": "2.0", "id": 0, "method": "initialize",
         "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
    ]
    p = subprocess.Popen([sys.executable, SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True)
    wanted = {m["id"] for m in messages} | {0}
    for m in init + messages:
        p.stdin.write(json.dumps(m) + "\n")
    p.stdin.flush()
    out = {}
    # Keep stdin open until every response arrived (closing early would drop in-flight replies).
    while wanted - out.keys():
        line = p.stdout.readline()
        assert line, "server closed stdout early"
        o = json.loads(line)  # raises if stdout is polluted with non-JSON
        assert o.get("jsonrpc") == "2.0", o
        if "id" in o:
            out[o["id"]] = o
    p.stdin.close()
    p.wait(timeout=10)
    rest = p.stdout.read()
    for l in rest.splitlines():
        if l.strip():
            json.loads(l)
    return out, p.stderr.read()


def call(i, name, args):
    return {"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": {"name": name, "arguments": args}}


def test_all():
    cases = [
        call(1, "get_customer_record", {"customer_id": "CUST-00001"}),
        call(2, "get_customer_record", {"customer_id": "cust-00001"}),
        call(3, "get_customer_record", {"customer_id": "CUST-1234"}),
        call(4, "get_customer_record", {"customer_id": "CUST-123456"}),
        call(5, "get_customer_record", {"customer_id": "CUST-00001\n"}),
        call(6, "get_customer_record", {"customer_id": 123}),
        call(7, "get_customer_record", {}),
        call(8, "trigger_refund", {"customer_id": "CUST-00001", "amount": 10.5, "reason": "Damaged item received"}),
        call(9, "trigger_refund", {"customer_id": "CUST-00001", "amount": -5, "reason": "Damaged item received"}),
        call(10, "trigger_refund", {"customer_id": "CUST-00001", "amount": 0, "reason": "Damaged item received"}),
        call(11, "trigger_refund", {"customer_id": "CUST-00001", "amount": 5, "reason": "short"}),
        call(12, "trigger_refund", {"customer_id": "CUST-00001", "amount": "5", "reason": "Damaged item received"}),
        call(13, "trigger_refund", {"customer_id": "CUST-00001", "amount": True, "reason": "Damaged item received"}),
        call(14, "trigger_refund", {"customer_id": "CUST-00001", "amount": 5, "reason": "Damaged item received", "x": 1}),
        call(15, "nope", {}),
        call(16, "get_customer_record", {"customer_id": "CUST-99999"}),
        {"jsonrpc": "2.0", "id": 17, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 18, "method": "tools/call", "params": {"name": "trigger_refund"}},
    ]
    res, err = run(cases)
    assert json.loads(res[1]["result"]["content"][0]["text"])["name"] == "Ada Lovelace"
    for i in (2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 18):
        assert res[i]["error"]["code"] == -32602, (i, res[i])
    assert res[8]["result"]["isError"] is False
    assert res[16]["result"]["isError"] is True
    assert {t["name"] for t in res[17]["result"]["tools"]} == {"get_customer_record", "trigger_refund"}
    assert "tools/call" in err  # logs landed on stderr
