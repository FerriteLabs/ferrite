"""Unit tests for the Ferrite Python client.

These tests mock the underlying ``redis.Redis`` connection so they run without
a live Ferrite server.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ferrite.client import (
    AsyncFerriteClient,
    DocumentResult,
    FerriteClient,
    GraphResult,
    QueryResult,
    SemanticSearchResult,
    TimeSeriesSample,
    TimeTravelEntry,
    VectorSearchResult,
)
from ferrite.exceptions import (
    FerriteCommandError,
    FerriteConnectionError,
    FerriteSerializationError,
)


# -----------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------


@pytest.fixture()
def mock_redis():
    """Return a MagicMock that replaces ``redis.Redis``."""
    with patch("ferrite.client.redis.Redis") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        yield instance


@pytest.fixture()
def client(mock_redis):
    """Return a :class:`FerriteClient` backed by a mocked Redis connection."""
    return FerriteClient()


# -----------------------------------------------------------------------
# Connection / lifecycle
# -----------------------------------------------------------------------


class TestLifecycle:
    def test_ping_success(self, client, mock_redis):
        mock_redis.ping.return_value = True
        assert client.ping() is True

    def test_ping_connection_error(self, client, mock_redis):
        import redis as _redis

        mock_redis.ping.side_effect = _redis.exceptions.ConnectionError("refused")
        with pytest.raises(FerriteConnectionError, match="refused"):
            client.ping()

    def test_close(self, client, mock_redis):
        client.close()
        mock_redis.close.assert_called_once()

    def test_context_manager(self, mock_redis):
        with FerriteClient() as c:
            assert c is not None
        mock_redis.close.assert_called_once()


# -----------------------------------------------------------------------
# Standard Redis pass-through
# -----------------------------------------------------------------------


class TestStandardCommands:
    def test_set_get(self, client, mock_redis):
        mock_redis.set.return_value = True
        assert client.set("key", "value") is True
        mock_redis.set.assert_called_once_with(
            "key", "value", ex=None, px=None, nx=False, xx=False
        )

        mock_redis.get.return_value = "value"
        assert client.get("key") == "value"

    def test_set_with_ttl(self, client, mock_redis):
        mock_redis.set.return_value = True
        client.set("key", "value", ex=60)
        mock_redis.set.assert_called_once_with(
            "key", "value", ex=60, px=None, nx=False, xx=False
        )

    def test_delete(self, client, mock_redis):
        mock_redis.delete.return_value = 2
        assert client.delete("k1", "k2") == 2

    def test_exists(self, client, mock_redis):
        mock_redis.exists.return_value = 1
        assert client.exists("key") == 1


# -----------------------------------------------------------------------
# Vector search
# -----------------------------------------------------------------------


class TestVectorSearch:
    def test_vector_create_index(self, client, mock_redis):
        mock_redis.execute_command.return_value = "OK"
        client.vector_create_index("idx", 384, "cosine", "hnsw")
        mock_redis.execute_command.assert_called_once_with(
            "VECTOR.INDEX.CREATE",
            "idx",
            "DIM", "384",
            "DISTANCE", "COSINE",
            "TYPE", "HNSW",
        )

    def test_vector_set(self, client, mock_redis):
        mock_redis.execute_command.return_value = "OK"
        vec = [0.1, 0.2, 0.3]
        client.vector_set("idx", "doc:1", vec, metadata={"tag": "test"})
        args = mock_redis.execute_command.call_args[0]
        assert args[0] == "VECTOR.ADD"
        assert args[1] == "idx"
        assert args[2] == "doc:1"
        assert json.loads(args[3]) == vec
        assert json.loads(args[4]) == {"tag": "test"}

    def test_vector_search(self, client, mock_redis):
        mock_redis.execute_command.return_value = [
            "doc:1", "0.95", '{"tag": "test"}',
            "doc:2", "0.80", None,
        ]
        results = client.vector_search("idx", [0.1, 0.2], top_k=2)
        assert len(results) == 2
        assert isinstance(results[0], VectorSearchResult)
        assert results[0].id == "doc:1"
        assert results[0].score == 0.95
        assert results[0].metadata == {"tag": "test"}
        assert results[1].id == "doc:2"
        assert results[1].metadata is None

    def test_vector_knn(self, client, mock_redis):
        mock_redis.execute_command.return_value = ["doc:1", "0.9", None]
        results = client.vector_knn("idx", [0.1], k=1)
        assert len(results) == 1


# -----------------------------------------------------------------------
# Semantic caching
# -----------------------------------------------------------------------


class TestSemanticCaching:
    def test_semantic_set(self, client, mock_redis):
        mock_redis.execute_command.return_value = "OK"
        client.semantic_set("capital of France?", "Paris")
        mock_redis.execute_command.assert_called_once_with(
            "SEMANTIC.SET", "capital of France?", "Paris"
        )

    def test_semantic_search_single_result(self, client, mock_redis):
        mock_redis.execute_command.return_value = "Paris"
        results = client.semantic_search("France capital?", 0.85)
        assert len(results) == 1
        assert results[0].text == "Paris"

    def test_semantic_search_multiple_results(self, client, mock_redis):
        mock_redis.execute_command.return_value = [
            "id1", "0.92", "Paris is the capital",
            "id2", "0.87", "France's capital is Paris",
        ]
        results = client.semantic_search("capital of France?")
        assert len(results) == 2
        assert results[0].score == 0.92

    def test_semantic_search_empty(self, client, mock_redis):
        mock_redis.execute_command.return_value = None
        results = client.semantic_search("unknown query")
        assert results == []


# -----------------------------------------------------------------------
# Time series
# -----------------------------------------------------------------------


class TestTimeSeries:
    def test_ts_add(self, client, mock_redis):
        mock_redis.execute_command.return_value = 1700000000
        ts = client.ts_add("temp:room1", "*", 23.5)
        assert ts == 1700000000

    def test_ts_add_with_labels(self, client, mock_redis):
        mock_redis.execute_command.return_value = 1700000000
        client.ts_add("temp:room1", "*", 23.5, labels={"loc": "office"})
        args = mock_redis.execute_command.call_args[0]
        assert "LABELS" in args
        assert "loc" in args
        assert "office" in args

    def test_ts_range(self, client, mock_redis):
        mock_redis.execute_command.return_value = [
            [1700000000, "23.5"],
            [1700000060, "24.0"],
        ]
        samples = client.ts_range("temp:room1", "-", "+")
        assert len(samples) == 2
        assert isinstance(samples[0], TimeSeriesSample)
        assert samples[0].timestamp == 1700000000
        assert samples[0].value == 23.5

    def test_ts_get(self, client, mock_redis):
        mock_redis.execute_command.return_value = [1700000000, "23.5"]
        sample = client.ts_get("temp:room1")
        assert sample is not None
        assert sample.value == 23.5

    def test_ts_get_empty(self, client, mock_redis):
        mock_redis.execute_command.return_value = None
        assert client.ts_get("empty") is None


# -----------------------------------------------------------------------
# Document store
# -----------------------------------------------------------------------


class TestDocumentStore:
    def test_doc_set(self, client, mock_redis):
        mock_redis.execute_command.return_value = "OK"
        doc = {"title": "Hello", "author": "Alice"}
        client.doc_set("articles", "a:1", doc)
        args = mock_redis.execute_command.call_args[0]
        assert args[0] == "DOC.INSERT"
        assert json.loads(args[3]) == doc

    def test_doc_set_serialization_error(self, client, mock_redis):
        with pytest.raises(FerriteSerializationError):
            client.doc_set("col", "id", {"bad": set()})  # type: ignore[arg-type]

    def test_doc_search(self, client, mock_redis):
        mock_redis.execute_command.return_value = [
            ["a:1", '{"title":"Hello"}'],
        ]
        results = client.doc_search("articles", {"author": "Alice"})
        assert len(results) == 1
        assert isinstance(results[0], DocumentResult)
        assert results[0].document["title"] == "Hello"


# -----------------------------------------------------------------------
# Graph
# -----------------------------------------------------------------------


class TestGraph:
    def test_graph_query(self, client, mock_redis):
        mock_redis.execute_command.return_value = [
            ["name", "age"],
            ["Bob", "28"],
        ]
        result = client.graph_query(
            "social",
            "MATCH (a:User)-[:FOLLOWS]->(b) RETURN b.name, b.age",
        )
        assert isinstance(result, GraphResult)
        assert result.columns == ["name", "age"]
        assert len(result.rows) == 1


# -----------------------------------------------------------------------
# FerriteQL
# -----------------------------------------------------------------------


class TestFerriteQL:
    def test_query(self, client, mock_redis):
        mock_redis.execute_command.return_value = [
            ["name"],
            ["Alice"],
            ["Bob"],
        ]
        result = client.query(
            "FROM users:* WHERE $.active = true SELECT $.name"
        )
        assert isinstance(result, QueryResult)
        assert result.count == 2
        assert result.columns == ["name"]


# -----------------------------------------------------------------------
# Time travel
# -----------------------------------------------------------------------


class TestTimeTravel:
    def test_time_travel(self, client, mock_redis):
        mock_redis.execute_command.return_value = "old_value"
        val = client.time_travel("mykey", "-1h")
        assert val == "old_value"

    def test_time_travel_missing(self, client, mock_redis):
        mock_redis.execute_command.return_value = None
        assert client.time_travel("missing", "-1h") is None

    def test_history(self, client, mock_redis):
        mock_redis.execute_command.return_value = [
            [1700000000, "v1"],
            [1700003600, "v2"],
        ]
        entries = client.history("mykey", "-24h")
        assert len(entries) == 2
        assert isinstance(entries[0], TimeTravelEntry)
        assert entries[1].value == "v2"


# -----------------------------------------------------------------------
# Error wrapping
# -----------------------------------------------------------------------


class TestErrorWrapping:
    def test_timeout_error(self, client, mock_redis):
        import redis as _redis

        mock_redis.execute_command.side_effect = _redis.exceptions.TimeoutError("slow")
        from ferrite.exceptions import FerriteTimeoutError

        with pytest.raises(FerriteTimeoutError, match="slow"):
            client.vector_create_index("idx", 384)

    def test_command_error(self, client, mock_redis):
        import redis as _redis

        mock_redis.execute_command.side_effect = _redis.exceptions.ResponseError(
            "ERR unknown command"
        )
        with pytest.raises(FerriteCommandError, match="unknown command"):
            client.query("bad query")
