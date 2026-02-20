"""
Example: Using Ferrite as a semantic cache for LangChain LLM calls.

Ferrite supports the Redis protocol, so you can use it as a drop-in replacement
for Redis in LangChain's caching infrastructure. This example shows how to:

1. Connect to Ferrite using the standard Redis client
2. Set up semantic caching for LLM calls
3. Reduce LLM API costs by caching semantically similar queries

Prerequisites:
    pip install langchain langchain-openai redis numpy

Usage:
    # Start Ferrite server
    cargo run --release

    # Set your OpenAI API key
    export OPENAI_API_KEY="sk-..."

    # Run this example
    python examples/langchain_cache.py
"""

import os
import time
import hashlib
import json
import struct

import numpy as np
import redis


# ==============================================================================
# Configuration
# ==============================================================================

FERRITE_HOST = os.environ.get("FERRITE_HOST", "127.0.0.1")
FERRITE_PORT = int(os.environ.get("FERRITE_PORT", "6380"))
SIMILARITY_THRESHOLD = float(os.environ.get("CACHE_THRESHOLD", "0.85"))

# The embedding dimension depends on the model:
#   - all-MiniLM-L6-v2: 384
#   - text-embedding-3-small: 1536
#   - text-embedding-ada-002: 1536
EMBEDDING_DIM = 384  # Using a small local model for this example

INDEX_NAME = "llm_cache"


# ==============================================================================
# Ferrite Semantic Cache (standalone, no LangChain dependency)
# ==============================================================================

class FerriteSemanticCache:
    """
    A semantic cache backed by Ferrite's vector search capabilities.

    Uses Ferrite's FT.CREATE / FT.SEARCH (RedisSearch-compatible) commands
    for vector similarity search, enabling semantic caching of LLM responses.
    """

    def __init__(
        self,
        host: str = FERRITE_HOST,
        port: int = FERRITE_PORT,
        index_name: str = INDEX_NAME,
        dimension: int = EMBEDDING_DIM,
        threshold: float = SIMILARITY_THRESHOLD,
    ):
        self.client = redis.Redis(host=host, port=port, decode_responses=False)
        self.index_name = index_name
        self.dimension = dimension
        self.threshold = threshold
        self._ensure_index()

    def _ensure_index(self):
        """Create the vector search index if it does not exist."""
        try:
            self.client.execute_command(
                "FT.CREATE", self.index_name,
                "ON", "HASH",
                "PREFIX", "1", "cache:",
                "SCHEMA",
                "embedding", "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32",
                "DIM", str(self.dimension),
                "DISTANCE_METRIC", "COSINE",
                "query", "TEXT",
                "response", "TEXT",
            )
            print(f"[Ferrite] Created index '{self.index_name}'")
        except redis.exceptions.ResponseError as e:
            if "Index already exists" in str(e):
                print(f"[Ferrite] Index '{self.index_name}' already exists")
            else:
                raise

    def _embedding_to_bytes(self, embedding: np.ndarray) -> bytes:
        """Convert a numpy embedding to bytes for Redis storage."""
        return embedding.astype(np.float32).tobytes()

    def _make_key(self, query: str) -> str:
        """Generate a deterministic cache key from the query text."""
        h = hashlib.sha256(query.encode()).hexdigest()[:16]
        return f"cache:{h}"

    def get(self, query_embedding: np.ndarray) -> dict | None:
        """
        Look up a cached response by semantic similarity.

        Args:
            query_embedding: The embedding vector of the query.

        Returns:
            A dict with 'query', 'response', and 'similarity' if a cache hit
            is found, or None on a miss.
        """
        blob = self._embedding_to_bytes(query_embedding)

        try:
            results = self.client.execute_command(
                "FT.SEARCH", self.index_name,
                f"*=>[KNN 1 @embedding $query_vec AS similarity]",
                "PARAMS", "2", "query_vec", blob,
                "RETURN", "3", "query", "response", "similarity",
                "DIALECT", "2",
            )

            # Parse FT.SEARCH results: [total, key1, [field, value, ...], ...]
            if results and results[0] > 0:
                fields = results[2]
                field_dict = {}
                for i in range(0, len(fields), 2):
                    key = fields[i].decode() if isinstance(fields[i], bytes) else fields[i]
                    val = fields[i + 1].decode() if isinstance(fields[i + 1], bytes) else fields[i + 1]
                    field_dict[key] = val

                similarity = 1.0 - float(field_dict.get("similarity", "1.0"))
                if similarity >= self.threshold:
                    return {
                        "query": field_dict.get("query", ""),
                        "response": field_dict.get("response", ""),
                        "similarity": similarity,
                    }
        except Exception as e:
            print(f"[Ferrite] Search error: {e}")

        return None

    def set(self, query: str, response: str, embedding: np.ndarray, ttl: int = 3600):
        """
        Store a query-response pair with its embedding.

        Args:
            query: The original query text.
            response: The LLM response to cache.
            embedding: The embedding vector of the query.
            ttl: Time-to-live in seconds (default: 1 hour).
        """
        key = self._make_key(query)
        blob = self._embedding_to_bytes(embedding)

        pipe = self.client.pipeline()
        pipe.hset(key, mapping={
            "query": query,
            "response": response,
            "embedding": blob,
        })
        if ttl > 0:
            pipe.expire(key, ttl)
        pipe.execute()

    def stats(self) -> dict:
        """Get index statistics."""
        try:
            info = self.client.execute_command("FT.INFO", self.index_name)
            return {"raw": info}
        except Exception:
            return {}


# ==============================================================================
# LangChain Integration
# ==============================================================================

def langchain_example():
    """
    Demonstrates using Ferrite as a semantic cache with LangChain.

    This requires:
        pip install langchain langchain-openai

    If these packages are not installed, a standalone demo runs instead.
    """
    try:
        from langchain_openai import ChatOpenAI, OpenAIEmbeddings
        from langchain.globals import set_llm_cache
        from langchain_community.cache import RedisSemanticCache

        # Connect LangChain to Ferrite (drop-in Redis replacement)
        ferrite_url = f"redis://{FERRITE_HOST}:{FERRITE_PORT}"

        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

        # Use Ferrite as a semantic cache via the Redis protocol
        set_llm_cache(RedisSemanticCache(
            redis_url=ferrite_url,
            embedding=embeddings,
            score_threshold=SIMILARITY_THRESHOLD,
        ))

        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

        print("\n--- LangChain + Ferrite Semantic Cache Demo ---\n")

        # First call: cache miss, calls the LLM
        t0 = time.time()
        response1 = llm.invoke("What is the capital of France?")
        t1 = time.time()
        print(f"Query 1: 'What is the capital of France?'")
        print(f"  Response: {response1.content[:100]}")
        print(f"  Latency:  {(t1 - t0) * 1000:.1f} ms (cache MISS)\n")

        # Second call: semantically similar, should hit cache
        t0 = time.time()
        response2 = llm.invoke("France's capital city?")
        t1 = time.time()
        print(f"Query 2: 'France's capital city?'")
        print(f"  Response: {response2.content[:100]}")
        print(f"  Latency:  {(t1 - t0) * 1000:.1f} ms (cache HIT)\n")

        # Third call: different question, cache miss
        t0 = time.time()
        response3 = llm.invoke("Explain quantum entanglement in simple terms.")
        t1 = time.time()
        print(f"Query 3: 'Explain quantum entanglement...'")
        print(f"  Response: {response3.content[:100]}")
        print(f"  Latency:  {(t1 - t0) * 1000:.1f} ms (cache MISS)\n")

    except ImportError:
        print("[INFO] LangChain not installed. Running standalone demo instead.")
        print("       Install with: pip install langchain langchain-openai langchain-community")
        standalone_demo()


# ==============================================================================
# Standalone Demo (no LangChain required)
# ==============================================================================

def standalone_demo():
    """
    Demonstrates Ferrite semantic caching without LangChain.

    Uses simple random embeddings to simulate the cache behavior.
    """
    print("\n--- Ferrite Semantic Cache Standalone Demo ---\n")

    cache = FerriteSemanticCache(
        host=FERRITE_HOST,
        port=FERRITE_PORT,
        dimension=EMBEDDING_DIM,
        threshold=SIMILARITY_THRESHOLD,
    )

    # Simulate embeddings (in production, use a real embedding model)
    np.random.seed(42)

    queries = [
        ("What is the capital of France?", "Paris is the capital of France."),
        ("Explain machine learning", "Machine learning is a subset of AI..."),
        ("How does photosynthesis work?", "Photosynthesis converts CO2 and water..."),
    ]

    print("Populating cache with 3 entries...")
    embeddings = {}
    for query, response in queries:
        emb = np.random.randn(EMBEDDING_DIM).astype(np.float32)
        emb /= np.linalg.norm(emb)  # Normalize
        embeddings[query] = emb
        cache.set(query, response, emb)
        print(f"  Cached: '{query[:50]}'")

    print("\nQuerying cache...")

    # Exact match (should hit)
    emb = embeddings["What is the capital of France?"]
    result = cache.get(emb)
    if result:
        print(f"  HIT:  similarity={result['similarity']:.4f}")
        print(f"        response='{result['response'][:60]}'")
    else:
        print("  MISS: (no similar entry found)")

    # Random query (should miss)
    random_emb = np.random.randn(EMBEDDING_DIM).astype(np.float32)
    random_emb /= np.linalg.norm(random_emb)
    result = cache.get(random_emb)
    if result:
        print(f"  HIT:  similarity={result['similarity']:.4f}")
    else:
        print("  MISS: (random embedding, as expected)")

    print("\nDone.")


# ==============================================================================
# Main
# ==============================================================================

if __name__ == "__main__":
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        langchain_example()
    else:
        print("[INFO] OPENAI_API_KEY not set. Running standalone demo.")
        print("       Set OPENAI_API_KEY to use the full LangChain integration.\n")
        standalone_demo()
