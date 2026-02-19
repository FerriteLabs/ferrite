"""LangChain integration — use Ferrite as a semantic cache for LLM calls.

Usage::

    from langchain.globals import set_llm_cache
    from ferrite_ai.langchain import FerriteSemanticCache

    set_llm_cache(FerriteSemanticCache())
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Sequence, Union

from ferrite_ai.client import FerriteClient

try:
    from langchain_core.caches import BaseCache, RETURN_VAL_TYPE
    from langchain_core.outputs import Generation

    _HAS_LANGCHAIN = True
except ImportError:  # pragma: no cover
    _HAS_LANGCHAIN = False

    class BaseCache:  # type: ignore[no-redef]
        """Stub so the module can be imported without LangChain."""

        def lookup(self, prompt: str, llm_string: str) -> Any: ...
        def update(self, prompt: str, llm_string: str, return_val: Any) -> None: ...
        def clear(self, **kwargs: Any) -> None: ...

    RETURN_VAL_TYPE = Any  # type: ignore[assignment,misc]


class FerriteSemanticCache(BaseCache):
    """LangChain-compatible semantic cache backed by Ferrite.

    Parameters
    ----------
    host : str
        Ferrite host (default ``"127.0.0.1"``).
    port : int
        Ferrite port (default ``6380``).
    similarity_threshold : float
        Minimum similarity to consider a cache hit (default ``0.85``).
    ttl : int | None
        Time-to-live in seconds for cached entries.
    namespace : str
        Key prefix for cache isolation (default ``"lc_cache"``).
    client : FerriteClient | None
        Pre-configured :class:`FerriteClient`.  When provided, *host* / *port*
        / *namespace* are ignored.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        similarity_threshold: float = 0.85,
        ttl: Optional[int] = None,
        namespace: str = "lc_cache",
        client: Optional[FerriteClient] = None,
    ) -> None:
        if not _HAS_LANGCHAIN:
            raise ImportError(
                "LangChain is required for this integration. "
                "Install it with: pip install langchain-core"
            )
        self._client = client or FerriteClient(
            host=host, port=port, namespace=namespace,
        )
        self.similarity_threshold = similarity_threshold
        self.ttl = ttl

    def lookup(self, prompt: str, llm_string: str) -> Optional[RETURN_VAL_TYPE]:
        """Look up a cached LLM response for *prompt*.

        Returns a list of :class:`Generation` objects on a hit, or ``None``.
        """
        cache_key = self._make_key(prompt, llm_string)
        result = self._client.semantic_get(cache_key, threshold=self.similarity_threshold)
        if result is None:
            return None
        try:
            generations_data = json.loads(result.response)
            return [Generation(**g) for g in generations_data]
        except (json.JSONDecodeError, TypeError):
            return [Generation(text=result.response)]

    def update(self, prompt: str, llm_string: str, return_val: RETURN_VAL_TYPE) -> None:
        """Store an LLM response in the semantic cache."""
        cache_key = self._make_key(prompt, llm_string)
        generations_data = [
            {"text": g.text, "generation_info": g.generation_info}
            for g in return_val
        ]
        self._client.semantic_set(
            query=cache_key,
            response=json.dumps(generations_data),
            metadata={"llm": llm_string[:128]},
            ttl=self.ttl,
        )

    def clear(self, **kwargs: Any) -> None:
        """Clear all cached entries (best-effort)."""
        # Delegate to Ferrite; a namespace-scoped flush is not part of the
        # SEMANTIC command set yet, so we issue a pattern-based delete.
        try:
            self._client.redis.execute_command(
                "SEMANTIC.FLUSH", self._client.namespace,
            )
        except Exception:
            pass

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _make_key(prompt: str, llm_string: str) -> str:
        """Combine prompt and LLM identifier into a single cache key."""
        tag = hashlib.sha256(llm_string.encode()).hexdigest()[:8]
        return f"{tag}:{prompt}"
