"""OpenAI caching example.

Demonstrates transparent semantic caching for OpenAI API calls,
reducing cost and latency for similar queries.

Prerequisites:
    pip install ferrite-ai[openai]
    export OPENAI_API_KEY="sk-..."

    # Start Ferrite server
    cargo run --release
"""

import time

from ferrite_ai.openai_wrapper import cached_completion


def main():
    print("OpenAI + Ferrite Semantic Cache\n")

    queries = [
        "What is the capital of France?",
        "France's capital city?",           # Should hit cache
        "Explain photosynthesis briefly",
        "How does photosynthesis work?",    # Should hit cache
    ]

    for query in queries:
        t0 = time.time()
        response = cached_completion(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": query}],
            threshold=0.85,
            ttl=3600,
        )
        latency = (time.time() - t0) * 1000

        content = response["choices"][0]["message"]["content"]
        hit = response["_cache_hit"]

        print(f"Query:    '{query}'")
        print(f"Response: {content[:100]}...")
        print(f"Latency:  {latency:.1f} ms — Cache {'HIT' if hit else 'MISS'}")
        print()


if __name__ == "__main__":
    main()
