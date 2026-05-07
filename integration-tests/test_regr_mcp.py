"""Regression tests for the trex MCP server.

Covers:
- POST /trex/mcp without an Accept header returns 415 (used to reset
  the connection because the streamable-HTTP transport rejected the
  request at the socket layer).
- The trexdb-list-tables tool actually applies its databaseName /
  schemaName filter args (used to silently ignore them).

These tests assume the trex container (trexsql-trex-1) is already
running and reachable on http://localhost:8001 with the seeded admin
user (admin@trex.local / password).
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
import pytest

BASE_URL = "http://localhost:8001"
ADMIN_EMAIL = "admin@trex.local"
ADMIN_PASSWORD = "password"


def _http(
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: float = 10.0,
) -> tuple[int, dict[str, str], bytes]:
    """Minimal HTTP client that returns (status, headers, body) for any code.

    urllib.request raises on 4xx/5xx, which makes status assertions awkward —
    catch HTTPError and unpack it the same way as a successful response.
    """
    req = urllib.request.Request(
        BASE_URL + path,
        method=method,
        data=body,
        headers=headers or {},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


@pytest.fixture(scope="module")
def admin_token() -> str:
    body = json.dumps({"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}).encode()
    status, _hdrs, raw = _http(
        "POST",
        "/trex/auth/v1/token?grant_type=password",
        headers={"Content-Type": "application/json"},
        body=body,
    )
    assert status == 200, f"login failed: {status} {raw[:200]!r}"
    return json.loads(raw)["access_token"]


@pytest.fixture(scope="module")
def mcp_api_key(admin_token):
    """Provision a short-lived MCP API key, then revoke it on teardown."""
    body = json.dumps({"name": f"regr-mcp-{uuid.uuid4().hex[:8]}"}).encode()
    status, _h, raw = _http(
        "POST",
        "/trex/api/api-keys",
        headers={
            "Authorization": f"Bearer {admin_token}",
            "Content-Type": "application/json",
        },
        body=body,
    )
    assert status == 200, f"api key create failed: {status} {raw[:200]!r}"
    payload = json.loads(raw)
    key_id = payload["id"]
    api_key = payload["key"]
    yield api_key
    # Cleanup: revoke
    _http(
        "DELETE",
        f"/trex/api/api-keys/{key_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )


def _mcp_init(api_key: str) -> str:
    """Initialize an MCP session. Returns the mcp-session-id."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "regr-mcp", "version": "1.0"},
        },
    }
    status, headers, _raw = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        body=json.dumps(payload).encode(),
    )
    assert status == 200, f"initialize failed: {status}"
    # Header keys are case-insensitive in HTTPMessage but dict() lowercases vary;
    # search defensively.
    session = None
    for k, v in headers.items():
        if k.lower() == "mcp-session-id":
            session = v
            break
    assert session, f"missing mcp-session-id; headers={headers}"

    # Send the initialized notification so subsequent tools/call works.
    _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "mcp-session-id": session,
        },
        body=json.dumps(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}
        ).encode(),
    )
    return session


def _mcp_call_tool(api_key: str, session: str, name: str, arguments: dict) -> list[dict]:
    """Call an MCP tool and return the parsed result rows (assumes JSON-text content)."""
    payload = {
        "jsonrpc": "2.0",
        "id": 99,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    status, _h, raw = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "mcp-session-id": session,
        },
        body=json.dumps(payload).encode(),
        timeout=30.0,
    )
    assert status == 200, f"tool call failed: {status} {raw[:200]!r}"
    # Streamable HTTP returns SSE; pluck the data: line and parse.
    text = raw.decode("utf-8", errors="replace")
    data_line = next(
        (ln[5:].strip() for ln in text.splitlines() if ln.startswith("data:")),
        None,
    )
    assert data_line, f"no data: line in SSE response: {text[:300]!r}"
    msg = json.loads(data_line)
    inner = msg["result"]["content"][0]["text"]
    return json.loads(inner)


def test_accept_header_required():
    """No Accept header should yield a clean 415 instead of a connection reset."""
    # Use a raw socket so we can omit Accept entirely (urllib refuses to drop it
    # cleanly across versions).
    body = b"{}"
    req = (
        b"POST /trex/mcp HTTP/1.1\r\n"
        b"Host: localhost:8001\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: close\r\n"
        b"\r\n" + body
    )
    sock = socket.create_connection(("localhost", 8001), timeout=5)
    try:
        sock.sendall(req)
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
    finally:
        sock.close()
    assert raw, "server reset the connection instead of replying"
    status_line = raw.split(b"\r\n", 1)[0].decode("latin-1", errors="replace")
    assert "415" in status_line, f"expected 415 status, got: {status_line!r}"
    # Body should be JSON explaining the requirement.
    _hdrs, _, body_bytes = raw.partition(b"\r\n\r\n")
    payload = json.loads(body_bytes.decode("utf-8", errors="replace"))
    assert "Accept" in payload.get("message", "") or "Accept" in payload.get("error", "")


def test_trexdb_filter_database(mcp_api_key):
    session = _mcp_init(mcp_api_key)
    rows = _mcp_call_tool(
        mcp_api_key,
        session,
        "trexdb-list-tables",
        {"databaseName": "_config"},
    )
    assert rows, "expected at least one table in the _config database"
    bad = [r for r in rows if r.get("databaseName") != "_config"]
    assert not bad, f"filter ignored — got rows from other DBs: {bad[:3]}"


def test_trexdb_filter_schema(mcp_api_key):
    session = _mcp_init(mcp_api_key)
    # Find a real schema first by listing all tables, then filter on it.
    all_rows = _mcp_call_tool(mcp_api_key, session, "trexdb-list-tables", {})
    assert all_rows, "trexdb-list-tables returned nothing — cannot test schema filter"
    # information_schema is present in every duckdb database so it's a stable target.
    target_schema = "information_schema"
    assert any(r.get("schemaName") == target_schema for r in all_rows), (
        f"expected to find a row in schema={target_schema!r}, got schemas="
        f"{set(r.get('schemaName') for r in all_rows[:50])}"
    )

    filtered = _mcp_call_tool(
        mcp_api_key,
        session,
        "trexdb-list-tables",
        {"schemaName": target_schema},
    )
    assert filtered, "schema filter returned no rows"
    bad = [r for r in filtered if r.get("schemaName") != target_schema]
    assert not bad, f"filter ignored — got rows from other schemas: {bad[:3]}"
