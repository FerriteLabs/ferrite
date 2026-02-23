"""VectorStore helper for Ferrite vector search operations."""

from __future__ import annotations

from typing import Any

from ferrite.client import Ferrite


class VectorStore:
    """High-level helper for vector index operations on Ferrite.

    Wraps :class:`Ferrite` to provide a simple interface for creating
    vector indices and performing similarity searches.

    Example::

        store = VectorStore(Ferrite())
        store.create_index("embeddings", dimension=768, metric="COSINE")
        store.add("embeddings", "doc1", [0.1, 0.2, ...], {"title": "Hello"})
        results = store.search("embeddings", [0.1, 0.2, ...], k=5)
    """

    def __init__(self, client: Ferrite | None = None, **kwargs: Any) -> None:
        """Initialize VectorStore.

        Args:
            client: Existing :class:`Ferrite` client. If ``None``, a new
                client is created using ``**kwargs``.
            **kwargs: Connection arguments forwarded to :class:`Ferrite`
                when ``client`` is not provided.
        """
        self.client = client or Ferrite(**kwargs)

    def create_index(
        self,
        name: str,
        dimension: int,
        metric: str = "COSINE",
        prefix: str = "",
    ) -> Any:
        """Create a vector search index.

        Args:
            name: Index name.
            dimension: Vector dimensionality.
            metric: Distance metric (``COSINE``, ``L2``, or ``IP``).
            prefix: Optional key prefix filter.

        Returns:
            Server response.
        """
        return self.client.create_index(
            name,
            "HASH",
            prefix or f"{name}:",
            "__vector",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            str(dimension),
            "DISTANCE_METRIC",
            metric,
        )

    def add(
        self,
        index: str,
        key: str,
        vector: list[float],
        payload: dict[str, Any] | None = None,
    ) -> Any:
        """Add a vector to an index.

        Args:
            index: Index name.
            key: Document key.
            vector: Vector as a list of floats.
            payload: Optional metadata stored with the vector.

        Returns:
            Server response.
        """
        return self.client.add_vector(index, key, vector, payload)

    def search(self, index: str, vector: list[float], k: int = 10) -> Any:
        """Search for the k nearest neighbors.

        Args:
            index: Index name.
            vector: Query vector.
            k: Number of results to return.

        Returns:
            Search results from the server.
        """
        import struct

        blob = struct.pack(f"<{len(vector)}f", *vector)
        query = f"*=>[KNN {k} @__vector $vec AS score]"
        return self.client.search(
            index,
            query,
            PARAMS=["2", "vec", blob],
            SORTBY=["score"],
            DIALECT=["2"],
        )

    def delete(self, index: str, key: str) -> int:
        """Delete a vector entry by key.

        Args:
            index: Index name (used as key prefix if needed).
            key: Document key to delete.

        Returns:
            Number of keys deleted (0 or 1).
        """
        return self.client.delete(key)
