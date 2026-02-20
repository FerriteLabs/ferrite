"""
Example: Using Ferrite as a vector store for LlamaIndex RAG pipelines.

Ferrite supports the Redis protocol with vector search extensions, making it
a high-performance drop-in replacement for Redis in LlamaIndex's vector store
and caching infrastructure.

This example shows how to:

1. Use Ferrite as a vector store for document embeddings (RAG)
2. Build a retrieval-augmented generation pipeline
3. Cache LLM responses with semantic similarity

Prerequisites:
    pip install llama-index llama-index-vector-stores-redis redis numpy

Usage:
    # Start Ferrite server
    cargo run --release

    # Set your OpenAI API key (for embeddings and LLM)
    export OPENAI_API_KEY="sk-..."

    # Run this example
    python examples/llamaindex_cache.py
"""

import os
import time
import json
import struct

import numpy as np
import redis


# ==============================================================================
# Configuration
# ==============================================================================

FERRITE_HOST = os.environ.get("FERRITE_HOST", "127.0.0.1")
FERRITE_PORT = int(os.environ.get("FERRITE_PORT", "6380"))
EMBEDDING_DIM = 384  # all-MiniLM-L6-v2
INDEX_NAME = "rag_documents"


# ==============================================================================
# Ferrite Vector Store (standalone, Redis-compatible)
# ==============================================================================

class FerriteVectorStore:
    """
    A vector store backed by Ferrite for RAG applications.

    Uses Ferrite's FT.CREATE / FT.SEARCH commands (RedisSearch-compatible)
    for storing and retrieving document embeddings.

    This class can be used standalone or as a foundation for LlamaIndex
    integration via the Redis vector store adapter.
    """

    def __init__(
        self,
        host: str = FERRITE_HOST,
        port: int = FERRITE_PORT,
        index_name: str = INDEX_NAME,
        dimension: int = EMBEDDING_DIM,
    ):
        self.client = redis.Redis(host=host, port=port, decode_responses=False)
        self.index_name = index_name
        self.dimension = dimension
        self._ensure_index()

    def _ensure_index(self):
        """Create the vector index if it does not exist."""
        try:
            self.client.execute_command(
                "FT.CREATE", self.index_name,
                "ON", "HASH",
                "PREFIX", "1", "doc:",
                "SCHEMA",
                "embedding", "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32",
                "DIM", str(self.dimension),
                "DISTANCE_METRIC", "COSINE",
                "content", "TEXT",
                "metadata", "TEXT",
            )
            print(f"[Ferrite] Created index '{self.index_name}'")
        except redis.exceptions.ResponseError as e:
            if "Index already exists" in str(e):
                print(f"[Ferrite] Index '{self.index_name}' already exists")
            else:
                raise

    def add_document(self, doc_id: str, content: str, embedding: np.ndarray,
                     metadata: dict | None = None):
        """
        Add a document with its embedding to the vector store.

        Args:
            doc_id: Unique document identifier.
            content: The document text content.
            embedding: The embedding vector (numpy array).
            metadata: Optional metadata dictionary (stored as JSON).
        """
        key = f"doc:{doc_id}"
        blob = embedding.astype(np.float32).tobytes()

        mapping = {
            "content": content,
            "embedding": blob,
        }
        if metadata:
            mapping["metadata"] = json.dumps(metadata)

        self.client.hset(key, mapping=mapping)

    def add_documents(self, documents: list[dict]):
        """
        Batch add documents to the vector store.

        Args:
            documents: List of dicts with keys 'id', 'content', 'embedding',
                       and optionally 'metadata'.
        """
        pipe = self.client.pipeline()
        for doc in documents:
            key = f"doc:{doc['id']}"
            blob = doc["embedding"].astype(np.float32).tobytes()
            mapping = {
                "content": doc["content"],
                "embedding": blob,
            }
            if doc.get("metadata"):
                mapping["metadata"] = json.dumps(doc["metadata"])
            pipe.hset(key, mapping=mapping)
        pipe.execute()
        print(f"[Ferrite] Added {len(documents)} documents")

    def search(self, query_embedding: np.ndarray, top_k: int = 5) -> list[dict]:
        """
        Search for similar documents.

        Args:
            query_embedding: The query embedding vector.
            top_k: Number of results to return.

        Returns:
            List of dicts with 'id', 'content', 'score', and 'metadata'.
        """
        blob = query_embedding.astype(np.float32).tobytes()

        try:
            results = self.client.execute_command(
                "FT.SEARCH", self.index_name,
                f"*=>[KNN {top_k} @embedding $query_vec AS score]",
                "PARAMS", "2", "query_vec", blob,
                "RETURN", "3", "content", "score", "metadata",
                "SORTBY", "score",
                "DIALECT", "2",
            )

            if not results or results[0] == 0:
                return []

            docs = []
            # Results format: [total, key1, [field, val, ...], key2, ...]
            i = 1
            while i < len(results):
                key = results[i].decode() if isinstance(results[i], bytes) else results[i]
                fields = results[i + 1]

                field_dict = {}
                for j in range(0, len(fields), 2):
                    k = fields[j].decode() if isinstance(fields[j], bytes) else fields[j]
                    v = fields[j + 1].decode() if isinstance(fields[j + 1], bytes) else fields[j + 1]
                    field_dict[k] = v

                doc = {
                    "id": key.replace("doc:", ""),
                    "content": field_dict.get("content", ""),
                    "score": 1.0 - float(field_dict.get("score", "1.0")),
                    "metadata": json.loads(field_dict["metadata"]) if "metadata" in field_dict else None,
                }
                docs.append(doc)
                i += 2

            return docs

        except Exception as e:
            print(f"[Ferrite] Search error: {e}")
            return []

    def delete(self, doc_id: str):
        """Delete a document by ID."""
        self.client.delete(f"doc:{doc_id}")

    def count(self) -> int:
        """Get the total number of documents."""
        try:
            info = self.client.execute_command("FT.INFO", self.index_name)
            # Parse info response to find num_docs
            for i, item in enumerate(info):
                if isinstance(item, bytes) and item == b"num_docs":
                    return int(info[i + 1])
            return 0
        except Exception:
            return 0


# ==============================================================================
# RAG Pipeline
# ==============================================================================

class SimpleRAGPipeline:
    """
    A simple RAG (Retrieval-Augmented Generation) pipeline using Ferrite.

    Stores document chunks as embeddings in Ferrite, retrieves relevant
    context for queries, and generates responses using an LLM.
    """

    def __init__(self, vector_store: FerriteVectorStore, embed_fn=None, llm_fn=None):
        self.store = vector_store
        self.embed_fn = embed_fn or self._default_embed
        self.llm_fn = llm_fn or self._default_llm

    def _default_embed(self, text: str) -> np.ndarray:
        """Simple hash-based embedding for demonstration purposes."""
        np.random.seed(hash(text) % (2**32))
        emb = np.random.randn(self.store.dimension).astype(np.float32)
        emb /= np.linalg.norm(emb)
        return emb

    def _default_llm(self, prompt: str) -> str:
        """Placeholder LLM that echoes the context."""
        return f"[Simulated LLM] Based on the context provided: {prompt[:200]}..."

    def ingest(self, documents: list[dict]):
        """
        Ingest documents into the vector store.

        Args:
            documents: List of dicts with 'id', 'content', and optionally 'metadata'.
        """
        docs_with_embeddings = []
        for doc in documents:
            emb = self.embed_fn(doc["content"])
            docs_with_embeddings.append({
                "id": doc["id"],
                "content": doc["content"],
                "embedding": emb,
                "metadata": doc.get("metadata"),
            })
        self.store.add_documents(docs_with_embeddings)

    def query(self, question: str, top_k: int = 3) -> dict:
        """
        Query the RAG pipeline.

        Args:
            question: The user's question.
            top_k: Number of context documents to retrieve.

        Returns:
            Dict with 'answer', 'sources', and 'latency_ms'.
        """
        t0 = time.time()

        # Step 1: Embed the query
        query_emb = self.embed_fn(question)

        # Step 2: Retrieve relevant documents
        results = self.store.search(query_emb, top_k=top_k)

        # Step 3: Build context
        context_parts = []
        sources = []
        for r in results:
            context_parts.append(r["content"])
            sources.append({
                "id": r["id"],
                "score": r["score"],
                "metadata": r.get("metadata"),
            })

        context = "\n\n".join(context_parts)

        # Step 4: Generate response
        prompt = f"Context:\n{context}\n\nQuestion: {question}\n\nAnswer:"
        answer = self.llm_fn(prompt)

        t1 = time.time()

        return {
            "answer": answer,
            "sources": sources,
            "latency_ms": (t1 - t0) * 1000,
        }


# ==============================================================================
# LlamaIndex Integration
# ==============================================================================

def llamaindex_example():
    """
    Demonstrates using Ferrite with LlamaIndex.

    This requires:
        pip install llama-index llama-index-vector-stores-redis

    If not installed, falls back to the standalone demo.
    """
    try:
        from llama_index.core import VectorStoreIndex, Document, Settings
        from llama_index.vector_stores.redis import RedisVectorStore

        # Configure LlamaIndex to use Ferrite (via Redis protocol)
        ferrite_url = f"redis://{FERRITE_HOST}:{FERRITE_PORT}"

        vector_store = RedisVectorStore(
            redis_url=ferrite_url,
            index_name="llamaindex_docs",
            overwrite=True,
        )

        # Create sample documents
        documents = [
            Document(
                text="Ferrite is a high-performance key-value store designed as a "
                     "drop-in Redis replacement. It uses epoch-based concurrency "
                     "and io_uring-first persistence for maximum throughput.",
                metadata={"source": "docs", "topic": "overview"},
            ),
            Document(
                text="Ferrite supports vector search with HNSW, IVF, and flat indexes. "
                     "Vectors can be stored alongside regular Redis data types, enabling "
                     "hybrid queries that combine keyword and semantic search.",
                metadata={"source": "docs", "topic": "vector-search"},
            ),
            Document(
                text="Semantic caching in Ferrite reduces LLM API costs by 40-60%. "
                     "Similar queries are matched using embedding similarity, returning "
                     "cached responses instead of making expensive API calls.",
                metadata={"source": "docs", "topic": "semantic-caching"},
            ),
        ]

        # Build index
        index = VectorStoreIndex.from_documents(
            documents,
            vector_store=vector_store,
        )

        # Query
        query_engine = index.as_query_engine()

        print("\n--- LlamaIndex + Ferrite RAG Demo ---\n")

        queries = [
            "What is Ferrite?",
            "How does vector search work in Ferrite?",
            "How much can semantic caching reduce LLM costs?",
        ]

        for q in queries:
            t0 = time.time()
            response = query_engine.query(q)
            t1 = time.time()

            print(f"Q: {q}")
            print(f"A: {str(response)[:200]}")
            print(f"   Latency: {(t1 - t0) * 1000:.1f} ms\n")

    except ImportError:
        print("[INFO] LlamaIndex not installed. Running standalone RAG demo.")
        print("       Install with: pip install llama-index llama-index-vector-stores-redis\n")
        standalone_rag_demo()


# ==============================================================================
# Standalone RAG Demo
# ==============================================================================

def standalone_rag_demo():
    """
    Demonstrates a RAG pipeline using Ferrite without LlamaIndex.
    """
    print("\n--- Ferrite RAG Pipeline Standalone Demo ---\n")

    store = FerriteVectorStore(
        host=FERRITE_HOST,
        port=FERRITE_PORT,
        dimension=EMBEDDING_DIM,
    )

    pipeline = SimpleRAGPipeline(store)

    # Sample documents about Ferrite
    documents = [
        {
            "id": "intro-1",
            "content": "Ferrite is a high-performance, tiered-storage key-value store "
                       "designed as a drop-in Redis replacement. Built in Rust with "
                       "epoch-based concurrency and io_uring-first persistence.",
            "metadata": {"category": "overview", "version": "0.1.0"},
        },
        {
            "id": "vector-1",
            "content": "Ferrite supports native vector similarity search with HNSW, "
                       "IVF, and flat index types. HNSW provides O(log n) search with "
                       "95%+ recall. IVF is memory-efficient for datasets over 1M vectors.",
            "metadata": {"category": "vector-search"},
        },
        {
            "id": "cache-1",
            "content": "Semantic caching uses vector embeddings to cache LLM responses "
                       "by meaning rather than exact key match. This typically reduces "
                       "LLM API costs by 40-60% while maintaining response quality.",
            "metadata": {"category": "semantic-caching"},
        },
        {
            "id": "perf-1",
            "content": "Ferrite achieves over 500K GET ops/sec per core and under 0.3ms "
                       "P50 latency. The three-tier HybridLog (memory, mmap, io_uring) "
                       "enables capacity beyond available RAM.",
            "metadata": {"category": "performance"},
        },
        {
            "id": "arch-1",
            "content": "The storage engine uses a HybridLog inspired by Microsoft FASTER. "
                       "Three tiers: Mutable Region (memory), Read-Only Region (mmap), "
                       "and Disk Region (io_uring). Thread-per-core architecture.",
            "metadata": {"category": "architecture"},
        },
    ]

    # Ingest documents
    print("Ingesting documents...")
    pipeline.ingest(documents)
    print(f"  {len(documents)} documents indexed\n")

    # Query the pipeline
    queries = [
        "What is Ferrite and what does it do?",
        "How fast is Ferrite?",
        "How does semantic caching work?",
        "Tell me about the storage architecture",
    ]

    for q in queries:
        result = pipeline.query(q, top_k=2)
        print(f"Q: {q}")
        print(f"A: {result['answer'][:150]}")
        print(f"   Sources: {[s['id'] for s in result['sources']]}")
        print(f"   Latency: {result['latency_ms']:.1f} ms\n")


# ==============================================================================
# Main
# ==============================================================================

if __name__ == "__main__":
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        llamaindex_example()
    else:
        print("[INFO] OPENAI_API_KEY not set. Running standalone RAG demo.")
        print("       Set OPENAI_API_KEY for full LlamaIndex integration.\n")
        standalone_rag_demo()
