"""Core Ferrite memory client using redis-py."""
import json
from typing import Optional, Dict, List, Any


class FerriteMemoryClient:
    """Client for Ferrite's MEM.* commands."""

    def __init__(self, host="localhost", port=6379, **kwargs):
        """Connect to Ferrite using redis-py."""
        import redis

        self.redis = redis.Redis(
            host=host, port=port, decode_responses=True, **kwargs
        )

    def put(
        self,
        agent_id: str,
        session_id: str,
        kind: str,
        content: str,
        meta: Optional[Dict] = None,
    ) -> str:
        """Store a memory record. Returns the record ID."""
        args = ["MEM.PUT", agent_id, session_id, kind, content]
        if meta:
            args.extend(["META", json.dumps(meta)])
        return self.redis.execute_command(*args)

    def get(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a memory record by ID."""
        result = self.redis.execute_command("MEM.GET", record_id)
        if result is None:
            return None
        return self._parse_kv_array(result)

    def recall(
        self,
        agent_id: str,
        limit: int = 10,
        kind: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Recall memories for an agent."""
        args = ["MEM.RECALL", agent_id, "LIMIT", str(limit)]
        if kind:
            args.extend(["KIND", kind])
        result = self.redis.execute_command(*args)
        return self._parse_recall_result(result)

    def forget(self, agent_id: str) -> int:
        """Forget all memories for an agent (GDPR)."""
        return self.redis.execute_command("MEM.FORGET", agent_id)

    def stats(self) -> Dict[str, Any]:
        """Get memory store statistics."""
        result = self.redis.execute_command("MEM.STATS")
        return self._parse_kv_array(result)

    def _parse_kv_array(self, arr):
        if not arr or not isinstance(arr, (list, tuple)):
            return {}
        result = {}
        for i in range(0, len(arr) - 1, 2):
            result[arr[i]] = arr[i + 1]
        return result

    def _parse_recall_result(self, result):
        if not result:
            return []
        return [
            self._parse_kv_array(r) if isinstance(r, list) else r
            for r in result
        ]
