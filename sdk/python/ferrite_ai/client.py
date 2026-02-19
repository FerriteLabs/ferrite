"""Core Ferrite AI client for semantic caching operations.

Wraps Ferrite's ``SEMANTIC.SET`` / ``SEMANTIC.GET`` / ``SEMANTIC.DEL`` commands
and adds convenience helpers for cache statistics.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import redis


@dataclass
class SemanticResult:
    """A single semantic cache hit."""

    query: str
    response: str
    similarity: float
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SemanticStats:
    """Aggregate statistics for semantic cache usage."""

    hit_count: int = 0
    miss_count: int = 0
    entry_count: int = 0
    avg_similarity: float = 0.0

    @property
    def hit_rate(self) -> float:
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0


class FerriteClient:
    """High-level client for Ferrite semantic caching.

    Parameters
    ----------
    host : str
        Ferrite server hostname (default ``"127.0.0.1"``).
    port : int
        Ferrite server port (default ``6380``).
    password : str | None
        Optional authentication password.
    db : int
        Redis database index (default ``0``).
    namespace : str
        Key prefix used to isolate cache entries (default ``"sem"``).
    decode_responses : bool
        Whether to decode byte responses to strings (default ``True``).
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        password: Optional[str] = None,
        db: int = 0,
        namespace: str = "sem",
        decode_responses: bool = True,
    ) -> None:
        self._client = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=decode_responses,
        )
        self.namespace = namespace
        self._hits = 0
        self._misses = 0

    # -- lifecycle -----------------------------------------------------------

    @property
    def redis(self) -> redis.Redis:
        """Return the underlying ``redis.Redis`` instance."""
        return self._client

    def close(self) -> None:
        """Close the connection."""
        self._client.close()

    def ping(self) -> bool:
        """Check connectivity."""
        return self._client.ping()

    # -- semantic operations -------------------------------------------------

    def semantic_set(
        self,
        query: str,
        response: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl: Optional[int] = None,
    ) -> None:
        """Store a query/response pair in the semantic cache.

        Parameters
        ----------
        query : str
            The natural-language query used as the semantic key.
        response : str
            The LLM response to cache.
        metadata : dict | None
            Optional JSON metadata stored alongside the entry.
        ttl : int | None
            Time-to-live in seconds.  ``None`` means no expiry.
        """
        args: list[str] = [self.namespace, query, response]
        if metadata is not None:
            args.extend(["METADATA", json.dumps(metadata)])
        if ttl is not None:
            args.extend(["TTL", str(ttl)])
        self._client.execute_command("SEMANTIC.SET", *args)

    def semantic_get(
        self,
        query: str,
        threshold: float = 0.85,
    ) -> Optional[SemanticResult]:
        """Look up a cached response by semantic similarity.

        Parameters
        ----------
        query : str
            The query to search for.
        threshold : float
            Minimum cosine similarity (0.0–1.0) to qualify as a hit.

        Returns
        -------
        SemanticResult | None
            The best matching cache entry, or ``None`` on a miss.
        """
        raw = self._client.execute_command(
            "SEMANTIC.GET", self.namespace, query, "THRESHOLD", str(threshold),
        )
        if raw is None or (isinstance(raw, list) and len(raw) == 0):
            self._misses += 1
            return None

        self._hits += 1
        return self._parse_result(raw)

    def semantic_delete(self, query: str) -> int:
        """Delete a cached entry by its original query text.

        Returns the number of entries removed.
        """
        result = self._client.execute_command(
            "SEMANTIC.DEL", self.namespace, query,
        )
        return int(result) if result else 0

    def semantic_stats(self) -> SemanticStats:
        """Return cache hit/miss statistics and entry count."""
        try:
            raw = self._client.execute_command("SEMANTIC.STATS", self.namespace)
            entry_count = 0
            avg_similarity = 0.0
            if isinstance(raw, list):
                info = self._pairs_to_dict(raw)
                entry_count = int(info.get("entries", 0))
                avg_similarity = float(info.get("avg_similarity", 0.0))
        except Exception:
            entry_count = 0
            avg_similarity = 0.0

        return SemanticStats(
            hit_count=self._hits,
            miss_count=self._misses,
            entry_count=entry_count,
            avg_similarity=avg_similarity,
        )

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _parse_result(raw: Any) -> SemanticResult:
        """Parse a ``SEMANTIC.GET`` response into a :class:`SemanticResult`."""
        if isinstance(raw, list):
            pairs = {}
            for i in range(0, len(raw), 2):
                k = raw[i] if isinstance(raw[i], str) else raw[i].decode()
                v = raw[i + 1] if isinstance(raw[i + 1], str) else raw[i + 1].decode()
                pairs[k] = v

            metadata = None
            if "metadata" in pairs:
                try:
                    metadata = json.loads(pairs["metadata"])
                except (json.JSONDecodeError, TypeError):
                    metadata = {"raw": pairs["metadata"]}

            return SemanticResult(
                query=pairs.get("query", ""),
                response=pairs.get("response", ""),
                similarity=float(pairs.get("similarity", 0.0)),
                metadata=metadata,
            )

        return SemanticResult(query="", response=str(raw), similarity=1.0)

    @staticmethod
    def _pairs_to_dict(pairs: list) -> Dict[str, str]:
        d: Dict[str, str] = {}
        for i in range(0, len(pairs), 2):
            k = pairs[i] if isinstance(pairs[i], str) else pairs[i].decode()
            v = pairs[i + 1] if isinstance(pairs[i + 1], str) else pairs[i + 1].decode()
            d[k] = v
        return d
