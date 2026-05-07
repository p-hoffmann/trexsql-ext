"""Regression tests for the trex MCP server protocol surface.

Complements ``test_regr_mcp.py`` (which covers Accept-header handling and
the ``trexdb-list-tables`` filter args). This module focuses on protocol
correctness:

  * ``initialize`` round-trip — response shape (``protocolVersion``,
    ``serverInfo``, ``capabilities``).
  * ``notifications/initialized`` — no response payload, no server error.
  * Session lifecycle — ``Mcp-Session-Id`` is captured from initialize and
    a subsequent ``tools/list`` requires it.
  * Expired / bogus session ID — server returns a structured 4xx, NOT a
    connection reset.
  * Malformed JSON-RPC payload (missing ``method``) — server returns a
    JSON-RPC error envelope, not a 500 / hang / SIGTERM.
  * ``tools/call`` for ``trexdb-execute-sql`` returning a TIMESTAMPTZ
    result — cross-checks the pgwire timestamptz fix from the same release
    train.

Assumes the trex container (trexsql-trex-1) is already running and
reachable on http://localhost:8001 with the seeded admin user
(admin@trex.local / password).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
import uuid

import pytest


BASE_URL = "http://localhost:8001"
ADMIN_EMAIL = "admin@trex.local"
ADMIN_PASSWORD = "password"

JSON_ACCEPT = "application/json, text/event-stream"


def _http(
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: float = 10.0,
) -> tuple[int, dict[str, str], bytes]:
    """Minimal HTTP client returning (status, headers, body) for any code.

    urllib raises on 4xx/5xx, which makes status assertions awkward. Catch
    HTTPError and unpack it the same way as a 200 response.
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


def _trex_available() -> bool:
    try:
        _http("GET", "/trex/health/v1", timeout=2.0)
        return True
    except Exception:
        try:
            # Fall back to checking the auth endpoint (returns 400 without body
            # but the server is reachable).
            _http("GET", "/", timeout=2.0)
            return True
        except Exception:
            return False


def _get_session_header(headers: dict[str, str]) -> str | None:
    for k, v in headers.items():
        if k.lower() == "mcp-session-id":
            return v
    return None


def _parse_sse(raw: bytes) -> dict | None:
    """Pluck the JSON payload out of an SSE ``data:`` line, if present.

    The streamable-HTTP transport returns ``text/event-stream`` for most
    JSON-RPC responses. For pure JSON responses (e.g. error envelopes) the
    payload is delivered directly.
    """
    text = raw.decode("utf-8", errors="replace")
    data_line = next(
        (ln[5:].strip() for ln in text.splitlines() if ln.startswith("data:")),
        None,
    )
    if data_line:
        try:
            return json.loads(data_line)
        except json.JSONDecodeError:
            return None
    # Fall back to plain JSON.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _require_trex():
    if not _trex_available():
        pytest.skip(f"trex not reachable at {BASE_URL}")


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
    """Issue an MCP API key at module start, revoke it on teardown."""
    body = json.dumps({"name": f"regr-mcp-proto-{uuid.uuid4().hex[:8]}"}).encode()
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
    _http(
        "DELETE",
        f"/trex/api/api-keys/{key_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )


def _initialize(api_key: str) -> tuple[dict, str]:
    """Send ``initialize``, return (parsed-response-dict, session-id)."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "regr-mcp-proto", "version": "1.0"},
        },
    }
    status, headers, raw = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": JSON_ACCEPT,
        },
        body=json.dumps(payload).encode(),
    )
    assert status == 200, f"initialize failed: {status} {raw[:200]!r}"
    msg = _parse_sse(raw)
    assert msg is not None, f"could not parse initialize response: {raw[:300]!r}"
    session = _get_session_header(headers)
    assert session, f"missing mcp-session-id; headers={headers}"
    return msg, session


def _notify_initialized(api_key: str, session: str) -> tuple[int, bytes]:
    """Send the ``notifications/initialized`` notification.

    A notification has no ``id`` and expects an empty response body (HTTP
    202 / 200, transport-dependent).
    """
    status, _h, raw = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": JSON_ACCEPT,
            "mcp-session-id": session,
        },
        body=json.dumps(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}
        ).encode(),
    )
    return status, raw


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


def test_initialize_response_shape(mcp_api_key):
    """``initialize`` returns protocolVersion, serverInfo, capabilities."""
    msg, session = _initialize(mcp_api_key)
    assert session, "mcp-session-id header missing on initialize response"
    assert msg.get("jsonrpc") == "2.0"
    assert "result" in msg, f"missing result envelope: {msg!r}"
    result = msg["result"]
    assert "protocolVersion" in result, f"missing protocolVersion: {result!r}"
    assert isinstance(result["protocolVersion"], str)
    assert "serverInfo" in result, f"missing serverInfo: {result!r}"
    server_info = result["serverInfo"]
    assert "name" in server_info
    assert "version" in server_info
    assert "capabilities" in result, f"missing capabilities: {result!r}"
    # Capabilities is an object (possibly empty); just type-check.
    assert isinstance(result["capabilities"], dict)


def test_notifications_initialized_no_error(mcp_api_key):
    """``notifications/initialized`` produces no JSON-RPC error."""
    _msg, session = _initialize(mcp_api_key)
    status, raw = _notify_initialized(mcp_api_key, session)
    # Notifications get 200 or 202; either is fine. Anything 4xx/5xx is a bug.
    assert 200 <= status < 300, (
        f"notifications/initialized returned {status}: {raw[:200]!r}"
    )
    # Body, if any, must NOT be a JSON-RPC error envelope.
    if raw.strip():
        msg = _parse_sse(raw)
        if msg is not None:
            assert "error" not in msg, (
                f"unexpected error from notification: {msg!r}"
            )


def test_session_required_for_tools_list(mcp_api_key):
    """``tools/list`` succeeds with a valid session and fails without one."""
    _msg, session = _initialize(mcp_api_key)
    _notify_initialized(mcp_api_key, session)

    # With session: should succeed.
    payload = {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    status_ok, _h, raw_ok = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {mcp_api_key}",
            "Content-Type": "application/json",
            "Accept": JSON_ACCEPT,
            "mcp-session-id": session,
        },
        body=json.dumps(payload).encode(),
    )
    assert status_ok == 200, (
        f"tools/list with session failed: {status_ok} {raw_ok[:200]!r}"
    )
    msg_ok = _parse_sse(raw_ok)
    assert msg_ok is not None and "result" in msg_ok, (
        f"tools/list result envelope missing: {raw_ok[:300]!r}"
    )
    tools = msg_ok["result"].get("tools", [])
    assert isinstance(tools, list)
    # Spot-check that the trexdb tools we know exist are advertised.
    tool_names = {t.get("name") for t in tools}
    assert any("trexdb" in n for n in tool_names if n), (
        f"expected at least one trexdb-* tool, got {tool_names}"
    )

    # Without session: should NOT succeed (4xx). Crucially: not a 5xx, not a
    # connection reset.
    status_bad, _h2, raw_bad = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {mcp_api_key}",
            "Content-Type": "application/json",
            "Accept": JSON_ACCEPT,
        },
        body=json.dumps(payload).encode(),
    )
    assert 400 <= status_bad < 500, (
        f"expected 4xx without session, got {status_bad}: {raw_bad[:200]!r}"
    )


def test_bogus_session_id_clean_4xx(mcp_api_key):
    """A bogus / expired session ID yields a clean 4xx, not a connection reset."""
    payload = {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}
    status, _h, raw = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {mcp_api_key}",
            "Content-Type": "application/json",
            "Accept": JSON_ACCEPT,
            "mcp-session-id": "not-a-real-session-" + uuid.uuid4().hex,
        },
        body=json.dumps(payload).encode(),
    )
    assert 400 <= status < 500, (
        f"expected 4xx for bogus session, got {status}: {raw[:200]!r}"
    )
    # Server should still be live afterwards: a follow-up legitimate
    # initialize round-trips fine.
    _msg, session = _initialize(mcp_api_key)
    assert session


def test_malformed_jsonrpc_missing_method(mcp_api_key):
    """A JSON-RPC request without ``method`` returns a structured error.

    Must NOT 500, hang, or kill the connection.
    """
    _msg, session = _initialize(mcp_api_key)
    _notify_initialized(mcp_api_key, session)

    # Missing ``method`` — invalid per JSON-RPC 2.0.
    bad_payload = {"jsonrpc": "2.0", "id": 4, "params": {}}
    status, _h, raw = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {mcp_api_key}",
            "Content-Type": "application/json",
            "Accept": JSON_ACCEPT,
            "mcp-session-id": session,
        },
        body=json.dumps(bad_payload).encode(),
        timeout=10.0,
    )
    # Acceptable: 200 with a JSON-RPC error envelope, or a 4xx with a
    # structured body. Unacceptable: 5xx or empty body.
    assert status < 500, f"server 5xx'd on malformed JSON-RPC: {status} {raw[:200]!r}"
    assert raw, "server returned empty body for malformed JSON-RPC"
    if status == 200:
        msg = _parse_sse(raw)
        assert msg is not None, f"unparseable response: {raw[:300]!r}"
        assert "error" in msg, (
            f"expected JSON-RPC error envelope, got: {msg!r}"
        )
        err = msg["error"]
        assert "code" in err and "message" in err, (
            f"malformed error envelope: {err!r}"
        )

    # Server must still be live: subsequent initialize round-trips.
    _msg2, session2 = _initialize(mcp_api_key)
    assert session2


def test_tools_call_execute_sql_timestamptz(mcp_api_key):
    """``tools/call trexdb-execute-sql`` with a TIMESTAMPTZ result.

    Cross-checks the pgwire timestamptz encoder fix — the same query path
    used to SIGTERM trex when serving a TIMESTAMPTZ column. The MCP layer
    sits on top of the same engine, so a regression here would also break
    the wire-protocol path.
    """
    _msg, session = _initialize(mcp_api_key)
    _notify_initialized(mcp_api_key, session)

    payload = {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "trexdb-execute-sql",
            "arguments": {
                "sql": (
                    "SELECT NOW() AS now_tz, "
                    "TIMESTAMPTZ '2026-05-07 12:34:56+00' AS fixed_tz, "
                    "1 AS heartbeat"
                ),
            },
        },
    }
    status, _h, raw = _http(
        "POST",
        "/trex/mcp",
        headers={
            "Authorization": f"Bearer {mcp_api_key}",
            "Content-Type": "application/json",
            "Accept": JSON_ACCEPT,
            "mcp-session-id": session,
        },
        body=json.dumps(payload).encode(),
        timeout=30.0,
    )
    assert status == 200, (
        f"tools/call execute-sql failed: {status} {raw[:300]!r}"
    )
    msg = _parse_sse(raw)
    assert msg is not None, f"unparseable tool response: {raw[:300]!r}"
    assert "error" not in msg, f"tool returned error: {msg.get('error')!r}"
    assert "result" in msg, f"missing result envelope: {msg!r}"
    content = msg["result"].get("content")
    assert content, f"empty content: {msg!r}"
    inner_text = content[0].get("text")
    assert inner_text, f"no text in content[0]: {content!r}"
    # The execute-sql tool returns its rows as a JSON-encoded string.
    rows = json.loads(inner_text)
    assert isinstance(rows, list) and len(rows) == 1, (
        f"expected exactly 1 row, got {rows!r}"
    )
    row = rows[0]
    # All three columns must be populated. The TIMESTAMPTZ values can be
    # serialised either as strings or epoch-style numbers depending on the
    # tool's encoder; either is fine — we just need them to be non-null.
    assert row.get("now_tz") is not None, f"now_tz missing: {row!r}"
    assert row.get("fixed_tz") is not None, f"fixed_tz missing: {row!r}"
    assert row.get("heartbeat") in (1, "1"), f"heartbeat unexpected: {row!r}"

    # Final liveness check: a second initialize must still succeed (proves
    # trex didn't SIGTERM during the timestamptz tool call).
    _msg2, session2 = _initialize(mcp_api_key)
    assert session2
