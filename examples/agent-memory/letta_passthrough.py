#!/usr/bin/env python3
"""Letta (MemGPT) agent with archival memory backed by Ferrite Mnemo.

Prerequisites:
    pip install ferrite-py[mnemo-letta]
    # Ferrite server running on localhost:6379

This example shows how to use Ferrite as a drop-in archival memory
backend for Letta agents. The adapter maps Letta's insert/search/delete
operations to Ferrite MEM.* commands.
"""
from ferrite_mnemo.letta import FerriteLettaMemory

AGENT_ID = "letta-passthrough-demo"


def main():
    # 1. Create a Ferrite-backed archival memory
    archival = FerriteLettaMemory(
        agent_id=AGENT_ID,
        session_id="letta-session-1",
        host="localhost",
        port=6379,
    )

    # 2. Insert user preferences and knowledge into archival memory
    passages = [
        "The user prefers dark mode in all applications.",
        "The user's timezone is America/New_York (UTC-5).",
        "The user is allergic to peanuts — flag any recipe suggestions.",
        "The user works as a data engineer at Acme Corp.",
        "The user's favourite programming language is Rust.",
    ]

    print("Inserting archival passages…")
    for passage in passages:
        rid = archival.insert(passage, meta={"source": "onboarding"})
        print(f"  ✓ {rid}: {passage[:50]}…")

    # 3. Search archival memory (keyword-style, semantic in the future)
    print("\nSearching for 'dark mode'…")
    results = archival.search("dark mode", limit=3)
    for r in results:
        print(f"  → {r}")

    # 4. List all archival entries
    print(f"\nAll archival entries ({archival.size} total):")
    for entry in archival.list_all():
        print(f"  • {entry}")

    # 5. Delete everything (GDPR)
    # count = archival.delete()
    # print(f"\nDeleted {count} archival entries.")


if __name__ == "__main__":
    main()
