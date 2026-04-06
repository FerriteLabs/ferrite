"""LangGraph BaseMemory adapter for Ferrite Mnemo."""
import json
from typing import List, Dict, Any, Optional


class FerriteLangGraphMemory:
    """LangGraph-compatible memory backed by Ferrite MEM.* commands.

    Implements the memory interface expected by LangGraph workflows.
    Each conversation turn is persisted as an episodic memory record
    and recalled when the graph needs context.

    Usage::

        from ferrite_mnemo.langgraph import FerriteLangGraphMemory

        memory = FerriteLangGraphMemory(agent_id="my-agent")
        memory.save_context(
            {"input": "Hello"},
            {"output": "Hi! How can I help?"},
        )
        ctx = memory.load_memory_variables({})
        print(ctx["history"])
    """

    def __init__(
        self,
        agent_id: str,
        session_id: str = "default",
        host: str = "localhost",
        port: int = 6379,
        recall_limit: int = 5,
        **kwargs,
    ):
        from .client import FerriteMemoryClient

        self.client = FerriteMemoryClient(host=host, port=port, **kwargs)
        self.agent_id = agent_id
        self.session_id = session_id
        self.recall_limit = recall_limit

    def save_context(
        self, inputs: Dict[str, Any], outputs: Dict[str, str]
    ) -> None:
        """Save a conversation turn to Ferrite memory."""
        content = json.dumps({"input": inputs, "output": outputs})
        self.client.put(
            self.agent_id, self.session_id, "episodic", content
        )

    def load_memory_variables(
        self, inputs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Load relevant memories for the current graph step."""
        memories = self.client.recall(
            self.agent_id, limit=self.recall_limit
        )
        return {"history": memories}

    def clear(self) -> None:
        """Clear all memories for this agent (GDPR-safe)."""
        self.client.forget(self.agent_id)

    @property
    def memory_variables(self) -> List[str]:
        """Keys injected into the graph state."""
        return ["history"]
