"""OpenAI SDK wrapper — transparently cache chat completions via Ferrite.

Usage::

    from ferrite_ai.openai_wrapper import cached_completion

    response = cached_completion(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "What is Ferrite?"}],
    )
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from ferrite_ai.client import FerriteClient

try:
    import openai

    _HAS_OPENAI = True
except ImportError:  # pragma: no cover
    _HAS_OPENAI = False


def cached_completion(
    *,
    model: str = "gpt-4o-mini",
    messages: List[Dict[str, str]],
    temperature: float = 0,
    threshold: float = 0.85,
    ttl: Optional[int] = 3600,
    ferrite_host: str = "127.0.0.1",
    ferrite_port: int = 6380,
    namespace: str = "openai_cache",
    client: Optional[FerriteClient] = None,
    **openai_kwargs: Any,
) -> Dict[str, Any]:
    """Call ``openai.chat.completions.create`` with transparent semantic caching.

    On a cache hit the OpenAI API is *not* called and the cached response is
    returned immediately, saving both latency and cost.

    Parameters
    ----------
    model : str
        OpenAI model name.
    messages : list[dict]
        Chat messages in the OpenAI format.
    temperature : float
        Sampling temperature (default ``0`` for deterministic caching).
    threshold : float
        Semantic similarity threshold for cache lookup (default ``0.85``).
    ttl : int | None
        Cache entry TTL in seconds (default ``3600``).
    ferrite_host / ferrite_port : str / int
        Ferrite connection details.
    namespace : str
        Cache namespace.
    client : FerriteClient | None
        Pre-configured Ferrite client.
    **openai_kwargs
        Additional keyword arguments forwarded to the OpenAI API.

    Returns
    -------
    dict
        The chat completion response dict with an extra ``_cache_hit`` boolean.
    """
    if not _HAS_OPENAI:
        raise ImportError(
            "The openai package is required. Install it with: pip install openai"
        )

    fc = client or FerriteClient(
        host=ferrite_host, port=ferrite_port, namespace=namespace,
    )

    # Build a semantic key from the user message(s)
    query_text = _extract_query(messages)
    cache_key = f"{model}:{query_text}"

    # Try cache
    result = fc.semantic_get(cache_key, threshold=threshold)
    if result is not None:
        try:
            cached = json.loads(result.response)
            cached["_cache_hit"] = True
            return cached
        except (json.JSONDecodeError, TypeError):
            pass

    # Cache miss — call OpenAI
    response = openai.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        **openai_kwargs,
    )
    response_dict = response.model_dump()

    # Store in cache
    fc.semantic_set(
        query=cache_key,
        response=json.dumps(response_dict),
        metadata={"model": model},
        ttl=ttl,
    )

    response_dict["_cache_hit"] = False
    return response_dict


def _extract_query(messages: List[Dict[str, str]]) -> str:
    """Extract user messages to form the semantic key."""
    user_parts = [m["content"] for m in messages if m.get("role") == "user"]
    return " ".join(user_parts) if user_parts else str(messages)
