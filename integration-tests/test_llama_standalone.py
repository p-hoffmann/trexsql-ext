"""Llama standalone tests.

Verifies that the llama extension can load, report status/GPU info, and
(with network access) download a small model, generate text, chat, and unload.
"""

import os
import shutil
import pytest
from conftest import LLAMA_EXT, REPO_ROOT, Node, alloc_ports

MODEL_URL = "https://huggingface.co/aladar/TinyLLama-v0-GGUF/resolve/main/TinyLLama-v0.Q8_0.gguf"
MODEL_FILENAME = "tiny-test.gguf"
MODEL_NAME = "tiny-test"
MODEL_LOAD_CONFIG = '{"n_ctx": 512, "n_gpu_layers": 0, "num_threads": 1}'

# Known locations where the model may already exist
_KNOWN_MODEL_PATHS = [
    os.path.expanduser("~/.local/share/duckdb-llama/models/tiny-test.gguf"),
    os.path.join(REPO_ROOT, "llama/models/tiny-test.gguf"),
]

# Where the download function places models (relative to child process CWD)
_LOCAL_MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
_LOCAL_MODEL_PATH = os.path.join(_LOCAL_MODELS_DIR, MODEL_FILENAME)


def _ensure_model_available():
    """Ensure the model file exists at ./models/tiny-test.gguf.

    If the file already exists locally, return its path.
    If it exists at a known location, copy it.
    Otherwise return None (download will be attempted).
    """
    if os.path.exists(_LOCAL_MODEL_PATH):
        return _LOCAL_MODEL_PATH

    for known_path in _KNOWN_MODEL_PATHS:
        if os.path.exists(known_path):
            os.makedirs(_LOCAL_MODELS_DIR, exist_ok=True)
            shutil.copy2(known_path, _LOCAL_MODEL_PATH)
            return _LOCAL_MODEL_PATH

    return None


# ---------------------------------------------------------------------------
# Model-free tests (fast, no network)
# ---------------------------------------------------------------------------


def test_llama_load_and_status(node_factory):
    """Extension loads and llama_status() returns status info."""
    node = node_factory(load_llama=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT llama_status()")
    assert len(result) == 1
    text = result[0][0]
    assert text is not None
    assert len(text) > 0


def test_llama_gpu_info(node_factory):
    """llama_gpu_info() reports GPU support information."""
    node = node_factory(load_llama=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT llama_gpu_info()")
    assert len(result) == 1
    text = result[0][0]
    assert "gpu_available" in text or "devices" in text


def test_llama_list_loaded_empty(node_factory):
    """llama_list_loaded() returns non-null with no models loaded."""
    node = node_factory(load_llama=True, load_flight=False, load_swarm=False)
    result = node.execute("SELECT llama_list_loaded()")
    assert len(result) == 1
    assert result[0][0] is not None


def test_llama_generate_no_model_error(node_factory):
    """llama_generate() with nonexistent model returns error string."""
    node = node_factory(load_llama=True, load_flight=False, load_swarm=False)
    result = node.execute(
        "SELECT llama_generate('nonexistent', 'hi', '{}')"
    )
    assert len(result) == 1
    text = result[0][0].lower()
    assert "not found" in text or "not loaded" in text or "error" in text


# ---------------------------------------------------------------------------
# Model download tests (requires network, ~6MB)
#
# These tests share a single node process so the downloaded/loaded model
# persists across them.  They MUST run in order.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def llama_node():
    """Module-scoped node with llama loaded (shared across download tests)."""
    gossip_port, flight_port, pgwire_port, trexas_port = alloc_ports()
    node = Node([LLAMA_EXT], gossip_port, flight_port, pgwire_port, trexas_port)
    yield node
    node.close()


class TestLlamaWithModel:
    """Ordered tests that download, load, use, and unload a model."""

    def test_llama_download_and_load(self, llama_node):
        """Download TinyLLama (or use cached copy) and load it for inference."""
        model_path = _ensure_model_available()

        if model_path is None:
            # No cached model — attempt download via the extension
            result = llama_node.execute(
                f"SELECT llama_download_model('{MODEL_URL}', '{MODEL_FILENAME}', '{{}}')"
            )
            download_status = result[0][0]
            assert "success" in download_status or "already_exists" in download_status
            model_path = f"./models/{MODEL_FILENAME}"

        # Load model
        result = llama_node.execute(
            f"SELECT llama_load_model('{model_path}', '{MODEL_LOAD_CONFIG}')"
        )
        assert "success" in result[0][0]

        # Verify loaded
        result = llama_node.execute("SELECT llama_list_loaded()")
        assert MODEL_NAME in result[0][0] or MODEL_FILENAME in result[0][0]

    def test_llama_generate(self, llama_node):
        """llama_generate() produces non-empty output."""
        result = llama_node.execute(
            f"SELECT llama_generate('{MODEL_NAME}', 'Once', "
            f"'{{\"max_tokens\": 1, \"temperature\": 0.1}}')"
        )
        assert len(result) == 1
        assert result[0][0] is not None
        assert len(result[0][0]) > 0

    def test_llama_chat(self, llama_node):
        """llama_chat() produces non-empty response."""
        result = llama_node.execute(
            f"SELECT llama_chat('{MODEL_NAME}', "
            f"'[{{\"role\": \"user\", \"content\": \"Hi\"}}]', "
            f"'{{\"max_tokens\": 3}}')"
        )
        assert len(result) == 1
        assert result[0][0] is not None
        assert len(result[0][0]) > 0

    def test_llama_unload(self, llama_node):
        """llama_unload_model() succeeds."""
        llama_node.execute(
            f"SELECT llama_unload_model('{MODEL_NAME}')"
        )
