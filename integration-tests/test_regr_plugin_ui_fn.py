"""Regression test: UI route shadowed by function route on the same scope prefix.

When a plugin contributes both `trex.functions.api[]` and `trex.ui.routes[]`
mounted at the same URL prefix (e.g. `/plugins/<scope>/notes`), the function
worker's `app.all('/plugins/.../*', ...)` catch-all used to swallow static-file
requests, leaving the UI unreachable. The fix registers UI static routes BEFORE
the function catch-all and matches both the bare function source and any
sub-path so that:

  GET /plugins/regr/ui-fn-test/index.html → UI HTML
  GET /plugins/regr/ui-fn-test/           → UI HTML (index.html)
  GET /plugins/regr/ui-fn-test/list       → function JSON

The test scaffolds a tiny `@regr/ui-fn-test` plugin into the running trex
container, restarts the container, then asserts both UI and function paths
return their expected payloads.

Assumes the trex container (`trexsql-trex-1`) is already running and reachable
at http://localhost:8001 with the seeded admin user
(admin@trex.local / password).
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

CONTAINER = "trexsql-trex-1"
BASE_URL = "http://localhost:8001"
PLUGIN_DIR = "/usr/src/plugins-dev/@regr/ui-fn-test"
PLUGIN_PATH = "/plugins/regr/ui-fn-test"

ADMIN_EMAIL = "admin@trex.local"
ADMIN_PASSWORD = "password"


PACKAGE_JSON = json.dumps(
    {
        "name": "@regr/ui-fn-test",
        "version": "0.0.1",
        "trex": {
            "functions": {
                # `function` points to a DIRECTORY containing index.ts —
                # the edge runtime expects a service path, not a file.
                "api": [
                    {"source": "/ui-fn-test/list", "function": "/fn"}
                ]
            },
            "ui": {"routes": [{"path": "/ui-fn-test", "dir": "dist"}]},
        },
    },
    indent=2,
)

INDEX_HTML = "<html><body>hit:ui</body></html>\n"

FN_TS = (
    'Deno.serve(() => new Response(JSON.stringify({hit:"fn"}), '
    '{headers:{"content-type":"application/json"}}));\n'
)


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], check=check, capture_output=True, text=True
    )


def _docker_exec(*args: str, user: str = "root") -> subprocess.CompletedProcess:
    return _docker("exec", "-u", user, CONTAINER, *args)


def _write_in_container(path: str, contents: str) -> None:
    """Write a file inside the container via `sh -c 'cat > file'`."""
    proc = subprocess.run(
        ["docker", "exec", "-i", "-u", "root", CONTAINER, "sh", "-c",
         f"cat > {path}"],
        input=contents,
        text=True,
        capture_output=True,
        check=True,
    )
    assert proc.returncode == 0, proc.stderr


def _wait_for_ready(timeout: float = 180.0) -> None:
    """Poll until the trex Express server on 8001 answers."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            req = urllib.request.Request(BASE_URL + "/", method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status in (200, 301, 302, 401, 404):
                    return
        except urllib.error.HTTPError as e:
            if e.code in (200, 301, 302, 401, 404):
                return
            last_err = e
        except Exception as e:
            last_err = e
        time.sleep(2)
    raise RuntimeError(f"trex did not become ready in {timeout}s; last={last_err!r}")


def _http(
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: float = 30.0,
) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(
        BASE_URL + path, method=method, data=body, headers=headers or {}
    )
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


@pytest.fixture(scope="module")
def test_plugin():
    """Scaffold the test plugin, restart trex, yield, then clean up."""
    # Create directory structure.
    _docker_exec(
        "mkdir", "-p", f"{PLUGIN_DIR}/dist", f"{PLUGIN_DIR}/fn"
    )

    # Write files.
    _write_in_container(f"{PLUGIN_DIR}/package.json", PACKAGE_JSON)
    _write_in_container(f"{PLUGIN_DIR}/dist/index.html", INDEX_HTML)
    _write_in_container(f"{PLUGIN_DIR}/fn/index.ts", FN_TS)

    # Restart so the plugin loader picks up the new plugin.
    _docker("restart", CONTAINER)
    _wait_for_ready()

    yield

    # Cleanup: remove plugin dir and restart so the registry is clean.
    _docker_exec("rm", "-rf", f"{PLUGIN_DIR}")
    # Best-effort cleanup of the @regr scope dir if empty.
    _docker_exec("sh", "-c", "rmdir /usr/src/plugins-dev/@regr 2>/dev/null || true")


def _admin_token() -> str:
    body = json.dumps({"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}).encode()
    status, _h, raw = _http(
        "POST",
        "/trex/auth/v1/token?grant_type=password",
        headers={"Content-Type": "application/json"},
        body=body,
        timeout=10.0,
    )
    assert status == 200, f"login failed: {status} {raw[:200]!r}"
    return json.loads(raw)["access_token"]


def test_ui_serves_index_html(test_plugin):
    status, headers, raw = _http("GET", f"{PLUGIN_PATH}/index.html")
    assert status == 200, f"expected 200, got {status} {raw[:200]!r}"
    assert b"hit:ui" in raw, f"unexpected UI body: {raw[:200]!r}"
    ctype = next(
        (v for k, v in headers.items() if k.lower() == "content-type"), ""
    )
    assert "text/html" in ctype, f"unexpected content-type: {ctype}"


def test_ui_serves_index_for_bare_prefix(test_plugin):
    status, _h, raw = _http("GET", f"{PLUGIN_PATH}/")
    assert status == 200, f"expected 200, got {status} {raw[:200]!r}"
    assert b"hit:ui" in raw, f"unexpected UI body: {raw[:200]!r}"


def test_function_route_reachable(test_plugin):
    """The function route at /plugins/regr/ui-fn-test/list must NOT be shadowed
    by the UI's static handler. Without auth the route returns 401 (the
    plugin-authz middleware), which is enough to prove the route was matched
    by the function `app.all`. With an admin token we should get 200 + the
    function JSON `{"hit":"fn"}`.
    """
    # Without auth — confirms the route is wired up but auth-gated.
    status, _h, raw = _http("GET", f"{PLUGIN_PATH}/list")
    assert status in (200, 401), (
        f"expected function route to be matched (200 or 401), got "
        f"{status} {raw[:200]!r}"
    )

    # With admin auth — confirms the function actually runs.
    token = _admin_token()
    status, _h, raw = _http(
        "GET",
        f"{PLUGIN_PATH}/list",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )
    assert status == 200, f"expected 200 from function, got {status} {raw[:200]!r}"
    payload = json.loads(raw)
    assert payload == {"hit": "fn"}, f"unexpected function body: {payload!r}"
