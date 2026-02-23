"""Ferrite client extending redis.Redis with Ferrite-specific commands."""

from __future__ import annotations

from typing import Any

import redis


class Ferrite(redis.Redis):
    """Client for Ferrite, extending the standard Redis client.

    Adds helper methods for Ferrite's search, vector, and plugin commands
    while remaining fully compatible with all standard Redis operations.

    Example::

        client = Ferrite(host="localhost", port=6379)
        client.set("key", "value")
        client.create_index("idx", "HASH", "doc:", "SCHEMA", "title", "TEXT")
    """

    def create_index(
        self,
        name: str,
        on: str = "HASH",
        prefix: str = "",
        *schema_args: str,
    ) -> Any:
        """Create a search index via FT.CREATE.

        Args:
            name: Index name.
            on: Data structure type (HASH or JSON).
            prefix: Key prefix filter.
            *schema_args: Field definitions, e.g. ``"title", "TEXT", "score", "NUMERIC"``.

        Returns:
            Server response (typically ``OK``).
        """
        cmd: list[str] = ["FT.CREATE", name, "ON", on]
        if prefix:
            cmd.extend(["PREFIX", "1", prefix])
        cmd.append("SCHEMA")
        cmd.extend(schema_args)
        return self.execute_command(*cmd)

    def search(self, index: str, query: str, **kwargs: Any) -> Any:
        """Execute a search query via FT.SEARCH.

        Args:
            index: Index name to search.
            query: Query string (e.g. ``"@title:hello"``).
            **kwargs: Additional parameters forwarded as command arguments
                (e.g. ``LIMIT=[0, 10]``, ``RETURN=[2, "title", "score"]``).

        Returns:
            Search results as returned by the server.
        """
        cmd: list[Any] = ["FT.SEARCH", index, query]
        for key, value in kwargs.items():
            cmd.append(key.upper())
            if isinstance(value, (list, tuple)):
                cmd.extend(value)
            else:
                cmd.append(value)
        return self.execute_command(*cmd)

    def add_vector(
        self,
        index: str,
        doc_id: str,
        vector: list[float],
        payload: dict[str, Any] | None = None,
    ) -> Any:
        """Add a document with a vector field via FT.ADD.

        Args:
            index: Target index name.
            doc_id: Document identifier.
            vector: Vector as a list of floats.
            payload: Optional additional fields stored alongside the vector.

        Returns:
            Server response.
        """
        fields: list[Any] = ["__vector", self._encode_vector(vector)]
        if payload:
            for k, v in payload.items():
                fields.extend([k, v])
        return self.execute_command("FT.ADD", index, doc_id, 1.0, "FIELDS", *fields)

    def plugin_list(self) -> Any:
        """List installed Ferrite plugins via PLUGIN LIST.

        Returns:
            List of installed plugins.
        """
        return self.execute_command("PLUGIN", "LIST")

    def plugin_install(self, name: str, **options: Any) -> Any:
        """Install a Ferrite plugin via PLUGIN INSTALL.

        Args:
            name: Plugin name or path.
            **options: Additional installation options.

        Returns:
            Server response.
        """
        cmd: list[Any] = ["PLUGIN", "INSTALL", name]
        for k, v in options.items():
            cmd.extend([k.upper(), v])
        return self.execute_command(*cmd)

    @staticmethod
    def _encode_vector(vector: list[float]) -> bytes:
        """Encode a float vector to bytes for storage."""
        import struct

        return struct.pack(f"<{len(vector)}f", *vector)
