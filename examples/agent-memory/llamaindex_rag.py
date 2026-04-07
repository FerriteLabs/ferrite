#!/usr/bin/env python3
"""LlamaIndex RAG agent with persistent memory backed by Ferrite Mnemo.

Prerequisites:
    pip install ferrite-py[mnemo-llamaindex]
    # Ferrite server running on localhost:6379

This example shows a retrieval-augmented generation (RAG) workflow
where the agent stores and retrieves chat history from Ferrite, keeping
full conversation context across sessions.
"""
from ferrite_mnemo.llamaindex import FerriteLlamaIndexMemory

AGENT_ID = "llamaindex-rag-demo"


def main():
    # 1. Create a Ferrite-backed chat memory
    memory = FerriteLlamaIndexMemory(
        agent_id=AGENT_ID,
        session_id="rag-session-1",
        host="localhost",
        port=6379,
        token_limit=4096,
    )

    # 2. Check for prior history
    history = memory.get_all()
    if history:
        print(f"Resuming session with {len(history)} messages:")
        for msg in history:
            print(f"  [{msg.get('role', '?')}] {msg.get('content', '')[:60]}")
    else:
        print("Starting new RAG session.")

    # 3. Simulate user ↔ assistant turns
    conversation = [
        ("user", "What are the benefits of tiered storage in databases?"),
        ("assistant", "Tiered storage separates hot, warm, and cold data across "
                      "different media — memory, SSD, and HDD — to optimise both "
                      "cost and performance."),
        ("user", "How does Ferrite implement tiered storage?"),
        ("assistant", "Ferrite uses a HybridLog inspired by Microsoft FASTER: "
                      "mutable region (memory), read-only region (mmap), and "
                      "disk region (io_uring). Records migrate automatically "
                      "based on access patterns."),
        ("user", "Can I query data that has been flushed to disk?"),
        ("assistant", "Yes — Ferrite transparently fetches disk-resident records "
                      "via async io_uring reads. The query interface is the same "
                      "regardless of where the data physically lives."),
    ]

    for role, content in conversation:
        print(f"\n[{role}] {content[:80]}{'…' if len(content) > 80 else ''}")
        memory.put(role=role, content=content)

    # 4. Retrieve recent context (e.g. to feed into an LLM prompt)
    recent = memory.get_latest(k=4)
    print(f"\n--- Last {len(recent)} messages (for LLM context window) ---")
    for msg in recent:
        role = msg.get("role", "?")
        text = msg.get("content", "")
        print(f"  [{role}] {text[:70]}{'…' if len(text) > 70 else ''}")

    # 5. Serialise config for LlamaIndex persistence
    print(f"\nMemory config: {memory.to_dict()}")

    # 6. Reset if needed
    # memory.reset()


if __name__ == "__main__":
    main()
