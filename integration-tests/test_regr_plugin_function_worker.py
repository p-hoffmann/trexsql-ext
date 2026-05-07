"""Regression test: Deno workspace declaration in /usr/src/deno.json broke
plugin function workers.

Background
----------
Plugin function workers boot a Deno isolate that loads its own
`deno.json` (the per-plugin one). Until recently, the Dockerfile shipped
`/usr/src/deno.json` with a `"workspace"` member that listed every
sub-package. When the worker tried to load a plugin's `deno.json` that
was NOT a workspace member (which is true for any plugin scaffolded into
`/usr/src/plugins-dev/...` at runtime) Deno aborted with::

    Config file must be a member of the workspace

This made every `/plugins/<scope>/<fn>` route 500. The fix is purely
build-side: the runtime Dockerfile rewrites `/usr/src/deno.json` to
strip the `workspace` key (`{"nodeModulesDir":"auto"}` is what survives).

This test scaffolds a minimal `@regr/wsp-test` plugin into the running
container and asserts that the function route responds 200 with the
expected JSON. If `/usr/src/deno.json` were to regress and re-introduce
the workspace key, the worker spawn would fail and the assertion below
would catch it.

Assumptions
-----------
* The trex container `trexsql-trex-1` is up on http://localhost:8001 with
  the seeded admin user (`admin@trex.local` / `password`).
* `docker compose` works from /home/ph/code/trexsql.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request

import pytest

CONTAINER = "trexsql-trex-1"
COMPOSE_DIR = "/home/ph/code/trexsql"
BASE_URL = "http://localhost:8001"

PLUGIN_DIR = "/usr/src/plugins-dev/@regr/wsp-test"
PLUGIN_PATH = "/plugins/regr/wsp-test"

ADMIN_EMAIL = "admin@trex.local"
ADMIN_PASSWORD = "password"


# We intentionally hit the function URL with a trailing slash
# (`/plugins/regr/wsp-test/`); to make that match the function route we
# anchor the source at `/wsp-test`. With Express's default (non-strict)
# routing the trailing slash maps to the same handler.
PACKAGE_JSON = json.dumps(
    {
        "name": "@regr/wsp-test",
        "version": "0.0.1",
        "trex": {
            "functions": {
                "api": [
                    {"source": "/wsp-test", "function": "/functions"}
                ]
            }
        },
    },
    indent=2,
)

# A plugin-local deno.json — the very file that, when combined with the
# old workspace declaration in /usr/src/deno.json, would have triggered
# `Config file must be a member of the workspace`.
DENO_JSON = json.dumps({"nodeModulesDir": "manual"}, indent=2)

FUNCTIONS_INDEX_TS = (
    'Deno.serve(() => Response.json({ok: true}));\n'
)


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], check=check, capture_output=True, text=True
    )


def _docker_exec(*args: str, user: str = "root") -> subprocess.CompletedProcess:
    return _docker("exec", "-u", user, CONTAINER, *args)


def _write_in_container(path: str, contents: str) -> None:
    proc = subprocess.run(
        [
            "docker", "exec", "-i", "-u", "root", CONTAINER,
            "sh", "-c", f"cat > {path}",
        ],
        input=contents,
        text=True,
        capture_output=True,
        check=True,
    )
    assert proc.returncode == 0, proc.stderr


def _wait_for_ready(timeout: float = 180.0) -> None:
    """Poll /trex/api/settings/public until it 200s."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            req = urllib.request.Request(
                BASE_URL + "/trex/api/settings/public", method="GET"
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status == 200:
                    return
        except urllib.error.HTTPError as e:
            if e.code == 200:
                return
            last_err = e
        except Exception as e:
            last_err = e
        time.sleep(2)
    raise RuntimeError(
        f"trex did not become ready in {timeout}s; last={last_err!r}"
    )


def _http(
    method: str,
    path: str,
    *,
    headers: dict | None = None,
    body: bytes | None = None,
    timeout: float = 30.0,
):
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
    """Scaffold @regr/wsp-test, restart trex once, yield, clean up + restart."""
    # Pre-clean any stale copy.
    _docker_exec("rm", "-rf", PLUGIN_DIR)
    _docker_exec("mkdir", "-p", f"{PLUGIN_DIR}/functions")
    _write_in_container(f"{PLUGIN_DIR}/package.json", PACKAGE_JSON)
    _write_in_container(f"{PLUGIN_DIR}/deno.json", DENO_JSON)
    _write_in_container(
        f"{PLUGIN_DIR}/functions/index.ts", FUNCTIONS_INDEX_TS
    )
    _docker_exec("chown", "-R", "node:node", PLUGIN_DIR)

    subprocess.run(
        ["docker", "compose", "restart", "trex"],
        cwd=COMPOSE_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    _wait_for_ready()

    yield

    _docker_exec("rm", "-rf", PLUGIN_DIR)
    _docker_exec(
        "sh", "-c",
        "rmdir /usr/src/plugins-dev/@regr 2>/dev/null || true",
    )
    subprocess.run(
        ["docker", "compose", "restart", "trex"],
        cwd=COMPOSE_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    _wait_for_ready()


def _admin_token() -> str:
    body = json.dumps(
        {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    ).encode()
    status, _h, raw = _http(
        "POST",
        "/trex/auth/v1/token?grant_type=password",
        headers={"Content-Type": "application/json"},
        body=body,
        timeout=10.0,
    )
    assert status == 200, f"login failed: {status} {raw[:200]!r}"
    return json.loads(raw)["access_token"]


def test_function_worker_serves_ok(test_plugin):
    """The plugin function worker boots and answers 200 + {"ok": true}.

    This is the load-bearing assertion for the workspace bug fix: with the
    old image, the worker would die with `Config file must be a member of
    the workspace` and this would 500.
    """
    token = _admin_token()
    status, _h, raw = _http(
        "GET",
        f"{PLUGIN_PATH}/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )
    assert status == 200, (
        f"expected 200 from function worker (workspace bug regression?), "
        f"got {status} {raw[:300]!r}"
    )
    payload = json.loads(raw)
    assert payload == {"ok": True}, f"unexpected body: {payload!r}"


def test_runtime_deno_json_has_no_workspace(test_plugin):
    """Belt-and-braces: directly inspect /usr/src/deno.json in the runtime
    image and confirm the `workspace` key is absent. This pins the build-
    side fix that the previous test exercises behaviorally."""
    res = _docker_exec("cat", "/usr/src/deno.json")
    assert res.returncode == 0, res.stderr
    parsed = json.loads(res.stdout)
    assert "workspace" not in parsed, (
        "/usr/src/deno.json still declares `workspace` — function workers "
        f"will fail to load plugin deno.json files. Got: {parsed!r}"
    )
