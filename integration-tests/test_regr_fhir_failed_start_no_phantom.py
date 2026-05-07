"""Regression test: failed FHIR server start must not leave a phantom entry.

Bug: register_server() in start_fhir_server() was called from the outer
function AFTER spawning the worker thread. If the worker then failed
internally (init_fhir_meta, load_default_definitions,
load_search_parameters, or TcpListener::bind), the thread returned Err
but the registry entry persisted. trex_fhir_status() would list a
phantom server with no underlying thread.

Fix: the spawned thread now always calls
ServerRegistry::deregister_server(host, port) on its way out — Ok or Err.
A startup gate ensures the thread doesn't run its body until the parent
has finished register_server, so the deregister-on-exit guard never
fires before the entry is created.

This test triggers the failure path by trying to bind a port that is
already taken by another listener. After the failed start,
trex_fhir_status() must not list the failed (host, port) row.
"""
import socket

import pytest


def test_fhir_failed_bind_no_phantom_in_status(node_factory):
    """Bind error inside the FHIR worker thread must not leave a phantom."""
    node = node_factory(load_db=False, load_fhir=True)

    host = "127.0.0.1"

    # Reserve a port from the OS, hold the socket open, and try to start
    # FHIR on the same port. TcpListener::bind inside the worker will
    # return EADDRINUSE; the worker thread errors out; the registry entry
    # must not survive.
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind((host, 0))
    blocker.listen(1)
    blocked_port = blocker.getsockname()[1]

    try:
        # The current start path returns the spawn-success string
        # ("Started FHIR R4 server on ...") synchronously even when the
        # background bind fails — the bind happens inside the spawned
        # thread which the parent doesn't await. We allow either outcome
        # at the SQL level; what matters is the post-state of the
        # registry, which we assert below.
        node.execute(
            f"SELECT trex_fhir_start('{host}', {blocked_port}, 'fhir')",
            timeout=60,
        )

        # Give the worker thread time to attempt + fail the bind and run
        # its deregister-on-exit guard. The thread does init_fhir_meta +
        # load_default_definitions + load_search_parameters first, which
        # can take a few seconds in debug builds.
        import time

        deadline = time.time() + 30
        last_status = None
        while time.time() < deadline:
            status_rows = node.execute(
                "SELECT * FROM trex_fhir_status()", timeout=15
            )
            last_status = status_rows
            # Any row containing our blocked port is a phantom.
            phantoms = [
                row
                for row in status_rows
                if any(str(blocked_port) == str(cell) for cell in row)
            ]
            if not phantoms:
                # No phantom. We're done — pass.
                return
            time.sleep(0.5)

        pytest.fail(
            "Phantom FHIR server entry persisted after failed start; "
            f"last trex_fhir_status() result: {last_status!r}"
        )
    finally:
        blocker.close()


def test_fhir_failed_start_then_clean_start_succeeds(node_factory):
    """After a failed start, a fresh start on a different port works.

    Sanity check that the cleanup path also restores the registry to a
    state where new servers can be registered without spurious "already
    running" errors.
    """
    node = node_factory(load_db=False, load_fhir=True)
    host = "127.0.0.1"

    # 1. Provoke a failed start by holding the port.
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind((host, 0))
    blocker.listen(1)
    blocked_port = blocker.getsockname()[1]
    try:
        node.execute(
            f"SELECT trex_fhir_start('{host}', {blocked_port}, 'fhir')",
            timeout=60,
        )
        # Wait briefly for the deregister-on-exit guard to run.
        import time

        time.sleep(2)
    finally:
        blocker.close()

    # 2. Now start FHIR on a free port. The registry must accept it.
    good_port = 28391
    started = node.execute(
        f"SELECT trex_fhir_start('{host}', {good_port}, 'fhir')",
        timeout=60,
    )
    assert "Started" in started[0][0], started

    # 3. Clean up so we don't leak a server into other tests.
    stopped = node.execute(
        f"SELECT trex_fhir_stop('{host}', {good_port})", timeout=60
    )
    assert any(
        kw in stopped[0][0] for kw in ("Stopped", "Shutdown")
    ), stopped
