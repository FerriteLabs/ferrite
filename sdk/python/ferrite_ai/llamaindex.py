"""LlamaIndex integration — use Ferrite as a semantic cache for LLM calls.

Usage::

    from ferrite_ai.llamaindex import FerriteSemanticCache

    cache = FerriteSemanticCache()
    cache.put("What is Ferrite?", "Ferrite is a high-performance KV store.")
    result = cache.get("Tell me about Ferrite")
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from ferrite_ai.client import FerriteClient


class FerriteSemanticCache:
    """LlamaIndex-compatible semantic cache backed by Ferrite.

    Implements the ``get`` / ``put`` pattern expected by LlamaIndex's
    caching infrastructure.

    Parameters
    ----------
    host : str
        Ferrite host (default ``"127.0.0.1"``).
    port : int
        Ferrite port (default ``6380``).
    similarity_threshold : float
        Minimum similarity for cache hits (default ``0.85``).
    ttl : int | None
        Time-to-live in seconds for cached entries.
    namespace : str
        Key prefix for cache isolation (default ``"li_cache"``).
    client : FerriteClient | None
        Pre-configured :class:`FerriteClient`.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        similarity_threshold: float = 0.85,
        ttl: Optional[int] = None,
        namespace: str = "li_cache",
        client: Optional[FerriteClient] = None,
    ) -> None:
        self._client = client or FerriteClient(
            host=host, port=port, namespace=namespace,
        )
        self.similarity_threshold = similarity_threshold
        self.ttl = ttl

    def get(self, query: str) -> Optional[str]:
        """Retrieve a cached response for *query*.

        Returns the cached string on a semantic hit, or ``None``.
        """
        result = self._client.semantic_get(
            query, threshold=self.similarity_threshold,
        )
        if result is None:
            return None
        return result.response

    def put(self, query: str, response: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Store a query/response pair in the cache."""
        self._client.semantic_set(
            query=query,
            response=response,
            metadata=metadata,
            ttl=self.ttl,
        )

    def delete(self, query: str) -> int:
        """Remove a cached entry."""
        return self._client.semantic_delete(query)

    def clear(self) -> None:
        """Flush all entries in this namespace (best-effort)."""
        try:
            self._client.redis.execute_command(
                "SEMANTIC.FLUSH", self._client.namespace,
            )
        except Exception:
            pass
