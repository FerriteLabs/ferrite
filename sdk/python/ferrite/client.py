"""Synchronous and asynchronous Ferrite client implementations.

Both :class:`FerriteClient` (sync) and :class:`AsyncFerriteClient` (async)
wrap the standard ``redis-py`` driver and add typed helper methods for every
Ferrite extension command: vector search, semantic caching, time series,
document store, graph queries, FerriteQL, and time-travel queries.

Standard Redis commands are available directly since the Ferrite client
exposes a ``redis`` property that returns the underlying ``redis.Redis``
(or ``redis.asyncio.Redis``) instance.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import redis
import redis.asyncio as aioredis

from ferrite.exceptions import (
    FerriteCommandError,
    FerriteConnectionError,
    FerriteSerializationError,
    FerriteTimeoutError,
)

# ---------------------------------------------------------------------------
# Data classes for structured results
# ---------------------------------------------------------------------------


@dataclass
class VectorSearchResult:
    """A single result from a vector similarity search."""

    id: str
    score: float
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SemanticSearchResult:
    """A single result from a semantic search."""

    id: str
    score: float
    text: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class TimeSeriesSample:
    """A single time-series data point."""

    timestamp: int
    value: float


@dataclass
class DocumentResult:
    """A single document returned from a document store query."""

    id: str
    document: Dict[str, Any] = field(default_factory=dict)
    score: Optional[float] = None


@dataclass
class GraphResult:
    """The result set from a graph query."""

    columns: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)


@dataclass
class QueryResult:
    """The result set from a FerriteQL query."""

    columns: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)
    count: int = 0


@dataclass
class TimeTravelEntry:
    """A historical version of a key's value."""

    timestamp: int
    value: str


# ---------------------------------------------------------------------------
# Synchronous client
# ---------------------------------------------------------------------------


class FerriteClient:
    """Synchronous Ferrite client.

    Wraps ``redis.Redis`` and adds Ferrite-specific extension commands.
    All standard Redis commands are available through :attr:`redis`.

    Parameters
    ----------
    host : str
        Server hostname.  Defaults to ``"localhost"``.
    port : int
        Server port.  Defaults to ``6379``.
    password : str or None
        Authentication password.
    username : str or None
        ACL username.  Defaults to ``"default"``.
    db : int
        Database index.  Defaults to ``0``.
    ssl : bool
        Whether to connect with TLS.  Defaults to ``False``.
    ssl_ca_certs : str or None
        Path to the CA certificate file for TLS verification.
    socket_timeout : float or None
        Timeout in seconds for socket reads/writes.
    socket_connect_timeout : float or None
        Timeout in seconds for the initial connection.
    decode_responses : bool
        Decode binary responses to strings.  Defaults to ``True``.

    Examples
    --------
    >>> client = FerriteClient()
    >>> client.set("key", "value")
    True
    >>> client.get("key")
    'value'
    >>> client.close()
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        username: Optional[str] = None,
        db: int = 0,
        ssl: bool = False,
        ssl_ca_certs: Optional[str] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        decode_responses: bool = True,
        **kwargs: Any,
    ) -> None:
        try:
            self._redis = redis.Redis(
                host=host,
                port=port,
                password=password,
                username=username,
                db=db,
                ssl=ssl,
                ssl_ca_certs=ssl_ca_certs,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                decode_responses=decode_responses,
                **kwargs,
            )
        except redis.exceptions.ConnectionError as exc:
            raise FerriteConnectionError(str(exc)) from exc

    # -- context manager -----------------------------------------------------

    def __enter__(self) -> "FerriteClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # -- lifecycle -----------------------------------------------------------

    @property
    def redis(self) -> redis.Redis:
        """Return the underlying ``redis.Redis`` instance for standard
        Redis commands."""
        return self._redis

    def close(self) -> None:
        """Close the connection."""
        self._redis.close()

    def ping(self) -> bool:
        """Check connectivity to the server.

        Returns
        -------
        bool
            ``True`` if the server responds with PONG.

        Raises
        ------
        FerriteConnectionError
            If the server is unreachable.
        """
        try:
            return self._redis.ping()
        except redis.exceptions.ConnectionError as exc:
            raise FerriteConnectionError(str(exc)) from exc

    # -- standard Redis pass-through ----------------------------------------

    def set(
        self,
        name: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Optional[bool]:
        """Set the value of a key.

        Parameters
        ----------
        name : str
            The key name.
        value : Any
            The value to store.
        ex : int or None
            TTL in seconds.
        px : int or None
            TTL in milliseconds.
        nx : bool
            Only set if the key does not exist.
        xx : bool
            Only set if the key already exists.
        """
        return self._redis.set(name, value, ex=ex, px=px, nx=nx, xx=xx)

    def get(self, name: str) -> Optional[str]:
        """Get the value of a key.

        Returns ``None`` if the key does not exist.
        """
        return self._redis.get(name)

    def delete(self, *names: str) -> int:
        """Delete one or more keys.  Returns the number of keys deleted."""
        return self._redis.delete(*names)

    def exists(self, *names: str) -> int:
        """Return the number of specified keys that exist."""
        return self._redis.exists(*names)

    # -- vector search -------------------------------------------------------

    def vector_create_index(
        self,
        index: str,
        dimensions: int,
        distance: str = "cosine",
        index_type: str = "hnsw",
    ) -> None:
        """Create a vector index.

        Parameters
        ----------
        index : str
            Name of the index.
        dimensions : int
            Dimensionality of vectors stored in this index.
        distance : str
            Distance metric: ``"cosine"``, ``"l2"``, or ``"ip"``.
        index_type : str
            Index algorithm: ``"hnsw"``, ``"ivf"``, or ``"flat"``.

        Examples
        --------
        >>> client.vector_create_index("embeddings", 384, "cosine", "hnsw")
        """
        self._execute(
            "VECTOR.INDEX.CREATE",
            index,
            "DIM", str(dimensions),
            "DISTANCE", distance.upper(),
            "TYPE", index_type.upper(),
        )

    def vector_set(
        self,
        index: str,
        id: str,
        vector: Sequence[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add a vector to an index.

        Parameters
        ----------
        index : str
            The index to add to.
        id : str
            Unique identifier for this vector.
        vector : sequence of float
            The vector data.
        metadata : dict or None
            Optional JSON metadata stored alongside the vector.
        """
        args = [index, id, json.dumps(list(vector))]
        if metadata is not None:
            args.append(json.dumps(metadata))
        self._execute("VECTOR.ADD", *args)

    def vector_search(
        self,
        index: str,
        query_vector: Sequence[float],
        top_k: int = 10,
        filter: Optional[str] = None,
    ) -> List[VectorSearchResult]:
        """Perform a similarity search.

        Parameters
        ----------
        index : str
            The index to search.
        query_vector : sequence of float
            The query vector.
        top_k : int
            Maximum number of results.
        filter : str or None
            Optional metadata filter expression.

        Returns
        -------
        list of VectorSearchResult
        """
        args = [index, json.dumps(list(query_vector)), "TOP", str(top_k)]
        if filter:
            args.extend(["FILTER", filter])
        raw = self._execute("VECTOR.SEARCH", *args)
        return _parse_vector_results(raw)

    def vector_knn(
        self,
        index: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> List[VectorSearchResult]:
        """Shorthand for :meth:`vector_search` with only ``top_k``."""
        return self.vector_search(index, query_vector, top_k=k)

    # -- semantic caching ----------------------------------------------------

    def semantic_set(self, text: str, value: str) -> None:
        """Store a value with a semantic (natural-language) key.

        Parameters
        ----------
        text : str
            The natural-language key.
        value : str
            The cached value.
        """
        self._execute("SEMANTIC.SET", text, value)

    def semantic_search(
        self, text: str, threshold: float = 0.85
    ) -> List[SemanticSearchResult]:
        """Retrieve cached values whose semantic key is similar to *text*.

        Parameters
        ----------
        text : str
            The query text.
        threshold : float
            Minimum similarity score (0.0--1.0).

        Returns
        -------
        list of SemanticSearchResult
        """
        raw = self._execute("SEMANTIC.GET", text, str(threshold))
        return _parse_semantic_results(raw)

    # -- time series ---------------------------------------------------------

    def ts_add(
        self,
        key: str,
        timestamp: str = "*",
        value: float = 0.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> int:
        """Append a sample to a time-series key.

        Parameters
        ----------
        key : str
            The time-series key.
        timestamp : str
            Timestamp in milliseconds or ``"*"`` for server auto-assign.
        value : float
            The sample value.
        labels : dict or None
            Optional labels to attach.

        Returns
        -------
        int
            The timestamp assigned by the server.
        """
        args = [key, timestamp, str(value)]
        if labels:
            args.append("LABELS")
            for k, v in labels.items():
                args.extend([k, v])
        raw = self._execute("TS.ADD", *args)
        return int(raw) if raw is not None else 0

    def ts_range(
        self,
        key: str,
        from_ts: str = "-",
        to_ts: str = "+",
    ) -> List[TimeSeriesSample]:
        """Query a range of samples from a time-series key.

        Parameters
        ----------
        key : str
            The time-series key.
        from_ts : str
            Start timestamp (``"-"`` for earliest).
        to_ts : str
            End timestamp (``"+"`` for latest).

        Returns
        -------
        list of TimeSeriesSample
        """
        raw = self._execute("TS.RANGE", key, from_ts, to_ts)
        return _parse_ts_samples(raw)

    def ts_get(self, key: str) -> Optional[TimeSeriesSample]:
        """Fetch the latest sample for a time-series key.

        Returns ``None`` if the key has no data.
        """
        raw = self._execute("TS.GET", key)
        if raw is None:
            return None
        if isinstance(raw, (list, tuple)) and len(raw) >= 2:
            return TimeSeriesSample(timestamp=int(raw[0]), value=float(raw[1]))
        return None

    # -- document store ------------------------------------------------------

    def doc_set(
        self,
        collection: str,
        id: str,
        document: Dict[str, Any],
    ) -> None:
        """Insert or replace a JSON document.

        Parameters
        ----------
        collection : str
            The collection name.
        id : str
            Unique document identifier.
        document : dict
            The JSON document to store.
        """
        try:
            doc_json = json.dumps(document)
        except (TypeError, ValueError) as exc:
            raise FerriteSerializationError(str(exc)) from exc
        self._execute("DOC.INSERT", collection, id, doc_json)

    def doc_search(
        self,
        collection: str,
        query: Dict[str, Any],
    ) -> List[DocumentResult]:
        """Search documents in a collection.

        Parameters
        ----------
        collection : str
            The collection name.
        query : dict
            A JSON query object.

        Returns
        -------
        list of DocumentResult
        """
        try:
            query_json = json.dumps(query)
        except (TypeError, ValueError) as exc:
            raise FerriteSerializationError(str(exc)) from exc
        raw = self._execute("DOC.FIND", collection, query_json)
        return _parse_document_results(raw)

    # -- graph ---------------------------------------------------------------

    def graph_query(self, graph: str, cypher: str) -> GraphResult:
        """Execute a Cypher-like graph query.

        Parameters
        ----------
        graph : str
            The graph name.
        cypher : str
            The query string.

        Returns
        -------
        GraphResult
        """
        raw = self._execute("GRAPH.QUERY", graph, cypher)
        return _parse_graph_result(raw)

    # -- FerriteQL -----------------------------------------------------------

    def query(self, ferrite_ql: str) -> QueryResult:
        """Execute a FerriteQL query.

        Parameters
        ----------
        ferrite_ql : str
            The FerriteQL query string, e.g.
            ``"FROM users:* WHERE $.active = true SELECT $.name LIMIT 10"``.

        Returns
        -------
        QueryResult
        """
        raw = self._execute("QUERY", ferrite_ql)
        return _parse_query_result(raw)

    # -- time travel ---------------------------------------------------------

    def time_travel(self, key: str, as_of: str) -> Optional[str]:
        """Read a key's value at a past point in time.

        Parameters
        ----------
        key : str
            The key name.
        as_of : str
            A human-friendly offset such as ``"-1h"`` or a Unix timestamp in
            milliseconds.

        Returns
        -------
        str or None
            The value at that point in time, or ``None`` if the key did not
            exist.

        Examples
        --------
        >>> client.time_travel("mykey", "-1h")
        'old_value'
        """
        raw = self._execute("GET", key, "AS", "OF", as_of)
        return raw if raw is not None else None

    def history(self, key: str, since: str) -> List[TimeTravelEntry]:
        """Retrieve the mutation history of a key.

        Parameters
        ----------
        key : str
            The key name.
        since : str
            How far back to look, e.g. ``"-24h"``.

        Returns
        -------
        list of TimeTravelEntry
        """
        raw = self._execute("HISTORY", key, "SINCE", since)
        return _parse_history_results(raw)

    # -- internal helpers ----------------------------------------------------

    def _execute(self, *args: str) -> Any:
        """Execute a raw command against the Redis connection."""
        try:
            return self._redis.execute_command(*args)
        except redis.exceptions.TimeoutError as exc:
            raise FerriteTimeoutError(str(exc)) from exc
        except redis.exceptions.ConnectionError as exc:
            raise FerriteConnectionError(str(exc)) from exc
        except redis.exceptions.ResponseError as exc:
            raise FerriteCommandError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Asynchronous client
# ---------------------------------------------------------------------------


class AsyncFerriteClient:
    """Asynchronous Ferrite client.

    Mirrors the API of :class:`FerriteClient` but every I/O method is a
    coroutine.  Wraps ``redis.asyncio.Redis``.

    Parameters
    ----------
    host : str
        Server hostname.  Defaults to ``"localhost"``.
    port : int
        Server port.  Defaults to ``6379``.
    password : str or None
        Authentication password.
    username : str or None
        ACL username.
    db : int
        Database index.
    ssl : bool
        Whether to connect with TLS.
    ssl_ca_certs : str or None
        Path to the CA certificate file for TLS verification.
    socket_timeout : float or None
        Timeout in seconds for socket reads/writes.
    socket_connect_timeout : float or None
        Timeout in seconds for the initial connection.
    decode_responses : bool
        Decode binary responses to strings.

    Examples
    --------
    >>> async with AsyncFerriteClient() as client:
    ...     await client.set("key", "value")
    ...     print(await client.get("key"))
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        username: Optional[str] = None,
        db: int = 0,
        ssl: bool = False,
        ssl_ca_certs: Optional[str] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        decode_responses: bool = True,
        **kwargs: Any,
    ) -> None:
        self._redis = aioredis.Redis(
            host=host,
            port=port,
            password=password,
            username=username,
            db=db,
            ssl=ssl,
            ssl_ca_certs=ssl_ca_certs,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            decode_responses=decode_responses,
            **kwargs,
        )

    # -- context manager -----------------------------------------------------

    async def __aenter__(self) -> "AsyncFerriteClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # -- lifecycle -----------------------------------------------------------

    @property
    def redis(self) -> aioredis.Redis:
        """Return the underlying ``redis.asyncio.Redis`` instance."""
        return self._redis

    async def close(self) -> None:
        """Close the connection."""
        await self._redis.aclose()

    async def ping(self) -> bool:
        """Check connectivity to the server."""
        try:
            return await self._redis.ping()
        except redis.exceptions.ConnectionError as exc:
            raise FerriteConnectionError(str(exc)) from exc

    # -- standard Redis pass-through ----------------------------------------

    async def set(
        self,
        name: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Optional[bool]:
        """Set the value of a key."""
        return await self._redis.set(name, value, ex=ex, px=px, nx=nx, xx=xx)

    async def get(self, name: str) -> Optional[str]:
        """Get the value of a key."""
        return await self._redis.get(name)

    async def delete(self, *names: str) -> int:
        """Delete one or more keys."""
        return await self._redis.delete(*names)

    async def exists(self, *names: str) -> int:
        """Return the number of specified keys that exist."""
        return await self._redis.exists(*names)

    # -- vector search -------------------------------------------------------

    async def vector_create_index(
        self,
        index: str,
        dimensions: int,
        distance: str = "cosine",
        index_type: str = "hnsw",
    ) -> None:
        """Create a vector index."""
        await self._execute(
            "VECTOR.INDEX.CREATE",
            index,
            "DIM", str(dimensions),
            "DISTANCE", distance.upper(),
            "TYPE", index_type.upper(),
        )

    async def vector_set(
        self,
        index: str,
        id: str,
        vector: Sequence[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add a vector to an index."""
        args = [index, id, json.dumps(list(vector))]
        if metadata is not None:
            args.append(json.dumps(metadata))
        await self._execute("VECTOR.ADD", *args)

    async def vector_search(
        self,
        index: str,
        query_vector: Sequence[float],
        top_k: int = 10,
        filter: Optional[str] = None,
    ) -> List[VectorSearchResult]:
        """Perform a similarity search."""
        args = [index, json.dumps(list(query_vector)), "TOP", str(top_k)]
        if filter:
            args.extend(["FILTER", filter])
        raw = await self._execute("VECTOR.SEARCH", *args)
        return _parse_vector_results(raw)

    async def vector_knn(
        self,
        index: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> List[VectorSearchResult]:
        """Shorthand for :meth:`vector_search` with only ``top_k``."""
        return await self.vector_search(index, query_vector, top_k=k)

    # -- semantic caching ----------------------------------------------------

    async def semantic_set(self, text: str, value: str) -> None:
        """Store a value with a semantic key."""
        await self._execute("SEMANTIC.SET", text, value)

    async def semantic_search(
        self, text: str, threshold: float = 0.85
    ) -> List[SemanticSearchResult]:
        """Retrieve cached values whose semantic key is similar to *text*."""
        raw = await self._execute("SEMANTIC.GET", text, str(threshold))
        return _parse_semantic_results(raw)

    # -- time series ---------------------------------------------------------

    async def ts_add(
        self,
        key: str,
        timestamp: str = "*",
        value: float = 0.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> int:
        """Append a sample to a time-series key."""
        args = [key, timestamp, str(value)]
        if labels:
            args.append("LABELS")
            for k, v in labels.items():
                args.extend([k, v])
        raw = await self._execute("TS.ADD", *args)
        return int(raw) if raw is not None else 0

    async def ts_range(
        self,
        key: str,
        from_ts: str = "-",
        to_ts: str = "+",
    ) -> List[TimeSeriesSample]:
        """Query a range of samples from a time-series key."""
        raw = await self._execute("TS.RANGE", key, from_ts, to_ts)
        return _parse_ts_samples(raw)

    async def ts_get(self, key: str) -> Optional[TimeSeriesSample]:
        """Fetch the latest sample for a time-series key."""
        raw = await self._execute("TS.GET", key)
        if raw is None:
            return None
        if isinstance(raw, (list, tuple)) and len(raw) >= 2:
            return TimeSeriesSample(timestamp=int(raw[0]), value=float(raw[1]))
        return None

    # -- document store ------------------------------------------------------

    async def doc_set(
        self,
        collection: str,
        id: str,
        document: Dict[str, Any],
    ) -> None:
        """Insert or replace a JSON document."""
        try:
            doc_json = json.dumps(document)
        except (TypeError, ValueError) as exc:
            raise FerriteSerializationError(str(exc)) from exc
        await self._execute("DOC.INSERT", collection, id, doc_json)

    async def doc_search(
        self,
        collection: str,
        query: Dict[str, Any],
    ) -> List[DocumentResult]:
        """Search documents in a collection."""
        try:
            query_json = json.dumps(query)
        except (TypeError, ValueError) as exc:
            raise FerriteSerializationError(str(exc)) from exc
        raw = await self._execute("DOC.FIND", collection, query_json)
        return _parse_document_results(raw)

    # -- graph ---------------------------------------------------------------

    async def graph_query(self, graph: str, cypher: str) -> GraphResult:
        """Execute a Cypher-like graph query."""
        raw = await self._execute("GRAPH.QUERY", graph, cypher)
        return _parse_graph_result(raw)

    # -- FerriteQL -----------------------------------------------------------

    async def query(self, ferrite_ql: str) -> QueryResult:
        """Execute a FerriteQL query."""
        raw = await self._execute("QUERY", ferrite_ql)
        return _parse_query_result(raw)

    # -- time travel ---------------------------------------------------------

    async def time_travel(self, key: str, as_of: str) -> Optional[str]:
        """Read a key's value at a past point in time."""
        raw = await self._execute("GET", key, "AS", "OF", as_of)
        return raw if raw is not None else None

    async def history(self, key: str, since: str) -> List[TimeTravelEntry]:
        """Retrieve the mutation history of a key."""
        raw = await self._execute("HISTORY", key, "SINCE", since)
        return _parse_history_results(raw)

    # -- internal helpers ----------------------------------------------------

    async def _execute(self, *args: str) -> Any:
        """Execute a raw command against the async Redis connection."""
        try:
            return await self._redis.execute_command(*args)
        except redis.exceptions.TimeoutError as exc:
            raise FerriteTimeoutError(str(exc)) from exc
        except redis.exceptions.ConnectionError as exc:
            raise FerriteConnectionError(str(exc)) from exc
        except redis.exceptions.ResponseError as exc:
            raise FerriteCommandError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Shared result parsers
# ---------------------------------------------------------------------------


def _parse_vector_results(raw: Any) -> List[VectorSearchResult]:
    """Parse the raw response from VECTOR.SEARCH into typed results."""
    results: List[VectorSearchResult] = []
    if not isinstance(raw, (list, tuple)):
        return results
    i = 0
    while i < len(raw):
        vid = str(raw[i])
        score = float(raw[i + 1]) if i + 1 < len(raw) else 0.0
        metadata = None
        if i + 2 < len(raw) and raw[i + 2] is not None:
            try:
                metadata = json.loads(str(raw[i + 2]))
            except (json.JSONDecodeError, TypeError):
                metadata = None
        results.append(VectorSearchResult(id=vid, score=score, metadata=metadata))
        i += 3
    return results


def _parse_semantic_results(raw: Any) -> List[SemanticSearchResult]:
    """Parse the raw response from SEMANTIC.GET into typed results."""
    results: List[SemanticSearchResult] = []
    if raw is None:
        return results
    if isinstance(raw, str):
        results.append(
            SemanticSearchResult(id="", score=1.0, text=raw, metadata=None)
        )
        return results
    if not isinstance(raw, (list, tuple)):
        return results
    i = 0
    while i < len(raw):
        sid = str(raw[i])
        score = float(raw[i + 1]) if i + 1 < len(raw) else 0.0
        text = str(raw[i + 2]) if i + 2 < len(raw) else None
        results.append(
            SemanticSearchResult(id=sid, score=score, text=text, metadata=None)
        )
        i += 3
    return results


def _parse_ts_samples(raw: Any) -> List[TimeSeriesSample]:
    """Parse the raw response from TS.RANGE into typed samples."""
    samples: List[TimeSeriesSample] = []
    if not isinstance(raw, (list, tuple)):
        return samples
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            samples.append(
                TimeSeriesSample(timestamp=int(item[0]), value=float(item[1]))
            )
    return samples


def _parse_document_results(raw: Any) -> List[DocumentResult]:
    """Parse the raw response from DOC.FIND into typed results."""
    results: List[DocumentResult] = []
    if not isinstance(raw, (list, tuple)):
        return results
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            did = str(item[0])
            try:
                document = json.loads(str(item[1]))
            except (json.JSONDecodeError, TypeError):
                document = {}
            results.append(DocumentResult(id=did, document=document))
    return results


def _parse_graph_result(raw: Any) -> GraphResult:
    """Parse the raw response from GRAPH.QUERY into a typed result."""
    columns: List[str] = []
    rows: List[List[Any]] = []
    if isinstance(raw, (list, tuple)):
        if raw and isinstance(raw[0], (list, tuple)):
            columns = [str(c) for c in raw[0]]
        for item in raw[1:]:
            if isinstance(item, (list, tuple)):
                rows.append([str(v) for v in item])
    return GraphResult(columns=columns, rows=rows)


def _parse_query_result(raw: Any) -> QueryResult:
    """Parse the raw response from QUERY into a typed result."""
    columns: List[str] = []
    rows: List[List[Any]] = []
    if isinstance(raw, (list, tuple)):
        if raw and isinstance(raw[0], (list, tuple)):
            columns = [str(c) for c in raw[0]]
        for item in raw[1:]:
            if isinstance(item, (list, tuple)):
                rows.append([str(v) for v in item])
    return QueryResult(columns=columns, rows=rows, count=len(rows))


def _parse_history_results(raw: Any) -> List[TimeTravelEntry]:
    """Parse the raw response from HISTORY into typed entries."""
    entries: List[TimeTravelEntry] = []
    if not isinstance(raw, (list, tuple)):
        return entries
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            entries.append(
                TimeTravelEntry(timestamp=int(item[0]), value=str(item[1]))
            )
    return entries
