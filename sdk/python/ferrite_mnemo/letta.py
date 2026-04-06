"""Letta (formerly MemGPT) memory adapter for Ferrite Mnemo.

Provides a storage shim that Letta can use as an external memory
backend, mapping its archival/recall storage to Ferrite MEM.* commands.
"""
import json
from typing import List, Dict, Any, Optional


class FerriteLettaMemory:
    """Letta-compatible memory shim backed by Ferrite MEM.* commands.

    Letta uses two memory tiers:

    * **Core memory** — small, always-in-context working memory.
    * **Archival memory** — large, searchable long-term storage.

    This adapter maps Letta's archival memory operations to Ferrite's
    ``MEM.PUT`` / ``MEM.RECALL`` / ``MEM.FORGET`` commands, giving
    Letta agents durable, server-side long-term memory.

    Usage::

        from ferrite_mnemo.letta import FerriteLettaMemory

        archival = FerriteLettaMemory(agent_id="letta-agent-1")
        archival.insert("The user prefers dark mode.")
        results = archival.search("dark mode")
    """

    def __init__(
        self,
        agent_id: str,
        session_id: str = "default",
        host: str = "localhost",
        port: int = 6379,
        **kwargs,
    ):
        from .client import FerriteMemoryClient

        self.client = FerriteMemoryClient(host=host, port=port, **kwargs)
        self.agent_id = agent_id
        self.session_id = session_id

    # ------------------------------------------------------------------
    # Archival memory interface
    # ------------------------------------------------------------------

    def insert(self, content: str, meta: Optional[Dict] = None) -> str:
        """Insert a passage into archival memory.

        Args:
            content: The text to store.
            meta: Optional metadata dict.

        Returns:
            The Ferrite record ID.
        """
        return self.client.put(
            self.agent_id,
            self.session_id,
            "archival",
            content,
            meta=meta,
        )

    def search(
        self,
        query: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Search archival memory.

        Args:
            query: Natural-language search query (currently unused by
                Ferrite but reserved for future semantic recall).
            limit: Maximum number of records to return.

        Returns:
            List of matching memory records.
        """
        return self.client.recall(
            self.agent_id, limit=limit, kind="archival"
        )

    def delete(self) -> int:
        """Delete all archival memories for this agent.

        Returns:
            Number of records deleted.
        """
        return self.client.forget(self.agent_id)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def list_all(self, limit: int = 100) -> List[Dict[str, Any]]:
        """List all archival passages (most recent first)."""
        return self.client.recall(
            self.agent_id, limit=limit, kind="archival"
        )

    @property
    def size(self) -> int:
        """Approximate number of archival entries (via MEM.STATS)."""
        stats = self.client.stats()
        return int(stats.get("total_records", 0))
