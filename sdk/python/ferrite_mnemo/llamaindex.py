"""LlamaIndex memory adapter for Ferrite Mnemo.

Provides a chat memory buffer backed by Ferrite's MEM.* commands,
compatible with LlamaIndex's ``BaseChatStoreMemory`` interface.
"""
import json
from typing import List, Dict, Any, Optional


class FerriteLlamaIndexMemory:
    """LlamaIndex-compatible memory backed by Ferrite MEM.* commands.

    Stores and retrieves ``ChatMessage``-style dicts so they can be
    fed directly into LlamaIndex chat engines and agents.

    Usage::

        from ferrite_mnemo.llamaindex import FerriteLlamaIndexMemory

        memory = FerriteLlamaIndexMemory(agent_id="rag-agent")
        memory.put(role="user", content="What is Ferrite?")
        memory.put(role="assistant", content="A fast KV store.")
        history = memory.get_all()
    """

    def __init__(
        self,
        agent_id: str,
        session_id: str = "default",
        host: str = "localhost",
        port: int = 6379,
        token_limit: int = 4096,
        **kwargs,
    ):
        from .client import FerriteMemoryClient

        self.client = FerriteMemoryClient(host=host, port=port, **kwargs)
        self.agent_id = agent_id
        self.session_id = session_id
        self.token_limit = token_limit

    def put(self, role: str, content: str, **extra) -> str:
        """Store a single chat message as a memory record.

        Args:
            role: Message role (``user``, ``assistant``, ``system``).
            content: Message text.
            **extra: Additional metadata persisted alongside the message.

        Returns:
            The Ferrite record ID.
        """
        payload = json.dumps({"role": role, "content": content})
        meta = {"role": role}
        meta.update(extra)
        return self.client.put(
            self.agent_id, self.session_id, "episodic", payload, meta=meta
        )

    def get_all(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return recent chat history as a list of message dicts.

        Each dict contains at least ``role`` and ``content`` keys,
        matching the shape expected by LlamaIndex chat models.
        """
        raw = self.client.recall(self.agent_id, limit=limit)
        messages: List[Dict[str, Any]] = []
        for record in raw:
            content = record.get("content", "")
            try:
                msg = json.loads(content)
            except (json.JSONDecodeError, TypeError):
                msg = {"role": "unknown", "content": str(content)}
            messages.append(msg)
        return messages

    def get_latest(self, k: int = 5) -> List[Dict[str, Any]]:
        """Return the *k* most recent messages."""
        return self.get_all(limit=k)

    def reset(self) -> None:
        """Delete all memories for this agent."""
        self.client.forget(self.agent_id)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise configuration (useful for LlamaIndex persistence)."""
        return {
            "type": "FerriteLlamaIndexMemory",
            "agent_id": self.agent_id,
            "session_id": self.session_id,
            "token_limit": self.token_limit,
        }
