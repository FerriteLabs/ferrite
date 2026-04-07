#!/usr/bin/env python3
"""LangGraph chatbot with persistent memory backed by Ferrite Mnemo.

Prerequisites:
    pip install ferrite-py[mnemo-langgraph]
    # Ferrite server running on localhost:6379

This example shows a simple chatbot that remembers conversation history
across restarts using Ferrite's MEM.* commands via the LangGraph adapter.
"""
from ferrite_mnemo.langgraph import FerriteLangGraphMemory

AGENT_ID = "langgraph-chatbot-demo"


def main():
    # 1. Create a Ferrite-backed memory instance
    memory = FerriteLangGraphMemory(
        agent_id=AGENT_ID,
        session_id="session-1",
        host="localhost",
        port=6379,
        recall_limit=5,
    )

    # 2. Load any existing conversation history
    context = memory.load_memory_variables({})
    if context["history"]:
        print(f"Restored {len(context['history'])} memories from Ferrite")
        for mem in context["history"]:
            print(f"  • {mem}")
    else:
        print("No previous memories — starting fresh.")

    # 3. Simulate a multi-turn conversation
    turns = [
        ("Hello! I'm planning a trip to Tokyo.", "Great choice! When are you going?"),
        ("Next March, for cherry blossom season.", "March is perfect for sakura!"),
        ("Any hotel recommendations?", "Shinjuku and Shibuya areas are popular."),
    ]

    for user_input, assistant_reply in turns:
        print(f"\nUser:      {user_input}")
        print(f"Assistant: {assistant_reply}")
        memory.save_context(
            {"input": user_input},
            {"output": assistant_reply},
        )

    # 4. Verify memories were persisted
    final_ctx = memory.load_memory_variables({})
    print(f"\n--- {len(final_ctx['history'])} memories stored in Ferrite ---")
    for i, mem in enumerate(final_ctx["history"], 1):
        print(f"  {i}. {mem}")

    # 5. GDPR: clear memories if needed
    # memory.clear()
    # print("All memories cleared.")


if __name__ == "__main__":
    main()
