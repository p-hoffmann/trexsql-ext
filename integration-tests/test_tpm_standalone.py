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
        "LIKE 'Package not found%' "
        "FROM tpm_info('this-package-does-not-exist-xyz123')"
    )
    assert len(result) == 1
    assert result[0][0] == "true"


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
        "LIKE 'https://registry.npmjs.org/%' "
        "FROM tpm_resolve('is-number')"
    )
    assert len(result) == 1
    assert result[0][0] == "true"


def test_tpm_tree(node_factory):
    """tpm_tree() returns rows with package name in tree output."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT json_extract_string(tree_info, '$.package') "
        "FROM tpm_tree('is-number@7.0.0') LIMIT 1"
    )
    assert len(result) == 1
    assert result[0][0] == "is-number"


def test_tpm_install_and_list(node_factory, tmp_path):
    """tpm_install() installs a package, tpm_list() lists it back."""
    node = node_factory(load_tpm=True, load_flight=False, load_swarm=False)
    install_dir = str(tmp_path / "node_modules")

    # Install a small package
    node.execute(
        f"SELECT * FROM tpm_install('is-number@7.0.0', '{install_dir}')"
    )

    # List installed packages
    result = node.execute(
        f"SELECT json_extract_string(list_info, '$.name') "
        f"FROM tpm_list('{install_dir}')"
    )
    assert len(result) >= 1
    names = [row[0] for row in result]
    assert "is-number" in names
