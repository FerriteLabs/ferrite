"""ferrite-ai — Semantic caching SDKs for LLM frameworks.

Provides ready-made integrations for LangChain, LlamaIndex, and OpenAI
backed by Ferrite's vector similarity search engine.

Usage::

    from ferrite_ai import FerriteClient

    client = FerriteClient()
    client.semantic_set("What is Python?", "Python is a programming language.")
    result = client.semantic_get("Tell me about Python")
"""

from ferrite_ai.client import FerriteClient

__all__ = ["FerriteClient"]
__version__ = "0.1.0"
