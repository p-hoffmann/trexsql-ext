"""TPM standalone tests.

Verifies that the tpm extension can load and execute npm package manager
functions: info, resolve, tree, install, and list.
"""

import pytest


def test_tpm_hello(node_factory):
    """tpm() returns greeting string."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT * FROM tpm('Sam')")
    assert len(result) == 1
    assert result[0][0] == "TPM Sam \U0001f4e6"


def test_tpm_info_package(node_factory):
    """tpm_info() returns JSON with correct package name."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(package_info, '$.name') "
        "FROM tpm_info('is-number')"
    )
    assert len(result) == 1
    assert result[0][0] == "is-number"


def test_tpm_info_scoped(node_factory):
    """tpm_info() works with scoped packages."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(package_info, '$.name') "
        "FROM tpm_info('@types/node')"
    )
    assert len(result) == 1
    assert result[0][0] == "@types/node"


def test_tpm_info_nonexistent(node_factory):
    """tpm_info() returns error JSON for non-existent packages."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(package_info, '$.error') "
        "FROM tpm_info('this-package-does-not-exist-xyz123')"
    )
    assert len(result) == 1
    assert result[0][0].startswith("Package not found")


def test_tpm_resolve_exact(node_factory):
    """tpm_resolve() resolves exact version."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(resolve_info, '$.resolved_version') "
        "FROM tpm_resolve('is-number@7.0.0')"
    )
    assert len(result) == 1
    assert result[0][0] == "7.0.0"


def test_tpm_resolve_semver(node_factory):
    """tpm_resolve() resolves caret semver range."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(resolve_info, '$.resolved_version') "
        "FROM tpm_resolve('is-number@^7.0.0')"
    )
    assert len(result) == 1
    assert result[0][0] == "7.0.0"


def test_tpm_resolve_tarball(node_factory):
    """tpm_resolve() returns a tarball URL."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(resolve_info, '$.tarball_url') "
        "FROM tpm_resolve('is-number')"
    )
    assert len(result) == 1
    assert result[0][0].startswith("https://registry.npmjs.org/")


def test_tpm_tree(node_factory):
    """tpm_tree() returns rows with package name in tree output."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(tree_info, '$.package') "
        "FROM tpm_tree('is-number@7.0.0') LIMIT 1"
    )
    assert len(result) == 1
    assert result[0][0] == "is-number"


def test_tpm_install(node_factory, tmp_path):
    """tpm_install() installs a package and returns success JSON."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    install_dir = str(tmp_path / "node_modules")

    result = node.execute(
        f"SELECT json_extract_string(install_results, '$.package'), "
        f"json_extract_string(install_results, '$.version'), "
        f"json_extract_string(install_results, '$.success') "
        f"FROM tpm_install('is-number@7.0.0', '{install_dir}')"
    )
    assert len(result) == 1
    assert result[0][0] == "is-number"
    assert result[0][1] == "7.0.0"
    assert result[0][2] == "true"
