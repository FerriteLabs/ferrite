"""Basic semantic caching example.

Demonstrates storing and retrieving LLM responses using Ferrite's
semantic similarity matching.

Prerequisites:
    pip install ferrite-ai

    # Start Ferrite server
    cargo run --release
"""

from ferrite_ai import FerriteClient


def main():
    client = FerriteClient(host="127.0.0.1", port=6380)
    print("Connected to Ferrite\n")

    # Store some responses
    entries = [
        ("What is the capital of France?", "Paris is the capital of France."),
        ("Explain machine learning", "Machine learning is a subset of AI that enables systems to learn from data."),
        ("How does photosynthesis work?", "Photosynthesis converts sunlight, CO2, and water into glucose and oxygen."),
    ]

    print("Populating cache...")
    for query, response in entries:
        client.semantic_set(query, response, ttl=3600)
        print(f"  Cached: '{query}'")

    # Look up by semantic similarity
    print("\nQuerying cache...")

    test_queries = [
        "What is France's capital?",         # Similar to entry 1
        "Tell me about ML",                   # Similar to entry 2
        "What is quantum computing?",         # No match
    ]

    for query in test_queries:
        result = client.semantic_get(query, threshold=0.85)
        if result:
            print(f"  HIT:  '{query}'")
            print(f"        → {result.response}")
            print(f"        similarity: {result.similarity:.4f}")
        else:
            print(f"  MISS: '{query}'")

    # Stats
    stats = client.semantic_stats()
    print(f"\nCache stats:")
    print(f"  Hits:       {stats.hit_count}")
    print(f"  Misses:     {stats.miss_count}")
    print(f"  Hit rate:   {stats.hit_rate:.1%}")
    print(f"  Entries:    {stats.entry_count}")

    client.close()


if __name__ == "__main__":
    main()
