"""Unit tests for the Ferrite Python SDK."""

from __future__ import annotations

import struct
from unittest.mock import MagicMock, patch

import pytest

from ferrite.client import Ferrite
from ferrite.vector import VectorStore


@pytest.fixture
def client() -> Ferrite:
    """Create a Ferrite client with a mocked connection."""
    with patch.object(Ferrite, "execute_command", return_value="OK") as mock_exec:
        c = Ferrite()
        c._mock_exec = mock_exec  # type: ignore[attr-defined]
        yield c  # type: ignore[misc]


class TestFerrite:
    def test_create_index(self, client: Ferrite) -> None:
        result = client.create_index("idx", "HASH", "doc:", "title", "TEXT")
        assert result == "OK"
        client._mock_exec.assert_called_once_with(  # type: ignore[attr-defined]
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
        )

    def test_search(self, client: Ferrite) -> None:
        client.search("idx", "@title:hello", LIMIT=[0, 10])
        client._mock_exec.assert_called_once_with(  # type: ignore[attr-defined]
            "FT.SEARCH", "idx", "@title:hello", "LIMIT", 0, 10,
        )

    def test_add_vector(self, client: Ferrite) -> None:
        vec = [1.0, 2.0, 3.0]
        client.add_vector("idx", "doc1", vec, {"title": "hello"})
        expected_blob = struct.pack("<3f", *vec)
        client._mock_exec.assert_called_once_with(  # type: ignore[attr-defined]
            "FT.ADD", "idx", "doc1", 1.0, "FIELDS",
            "__vector", expected_blob, "title", "hello",
        )

    def test_plugin_list(self, client: Ferrite) -> None:
        client.plugin_list()
        client._mock_exec.assert_called_once_with("PLUGIN", "LIST")  # type: ignore[attr-defined]

    def test_plugin_install(self, client: Ferrite) -> None:
        client.plugin_install("myplugin", path="/opt/plugins")
        client._mock_exec.assert_called_once_with(  # type: ignore[attr-defined]
            "PLUGIN", "INSTALL", "myplugin", "PATH", "/opt/plugins",
        )

    def test_encode_vector(self) -> None:
        vec = [1.0, 0.0, -1.0]
        encoded = Ferrite._encode_vector(vec)
        assert len(encoded) == 12  # 3 floats × 4 bytes
        assert struct.unpack("<3f", encoded) == (1.0, 0.0, -1.0)


class TestVectorStore:
    def test_create_index(self, client: Ferrite) -> None:
        store = VectorStore(client)
        store.create_index("emb", dimension=128, metric="L2")
        client._mock_exec.assert_called_once()  # type: ignore[attr-defined]
        args = client._mock_exec.call_args[0]  # type: ignore[attr-defined]
        assert args[0] == "FT.CREATE"
        assert "128" in args
        assert "L2" in args

    def test_delete(self, client: Ferrite) -> None:
        with patch.object(client, "delete", return_value=1) as mock_del:
            store = VectorStore(client)
            result = store.delete("emb", "doc:1")
            assert result == 1
            mock_del.assert_called_once_with("doc:1")
