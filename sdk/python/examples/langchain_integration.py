"""LangChain semantic caching example.

Demonstrates using Ferrite as a transparent semantic cache for LangChain
LLM calls, reducing API costs for semantically similar queries.

Prerequisites:
    pip install ferrite-ai[langchain] langchain-openai
    export OPENAI_API_KEY="sk-..."

    # Start Ferrite server
    cargo run --release
"""

import time

from langchain.globals import set_llm_cache
from langchain_openai import ChatOpenAI

from ferrite_ai.langchain import FerriteSemanticCache


def main():
    # Configure Ferrite as the LangChain cache
    set_llm_cache(FerriteSemanticCache(
        host="127.0.0.1",
        port=6380,
        similarity_threshold=0.85,
        ttl=3600,
        namespace="lc_demo",
    ))

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    print("LangChain + Ferrite Semantic Cache\n")

    queries = [
        ("What is the capital of France?", "Cache MISS (first call)"),
        ("France's capital city?", "Cache HIT (semantically similar)"),
        ("Explain quantum entanglement", "Cache MISS (different topic)"),
        ("What is quantum entanglement?", "Cache HIT (semantically similar)"),
    ]

    for query, expected in queries:
        t0 = time.time()
        response = llm.invoke(query)
        latency = (time.time() - t0) * 1000

        print(f"Query:    '{query}'")
        print(f"Response: {response.content[:100]}...")
        print(f"Latency:  {latency:.1f} ms — {expected}")
        print()


if __name__ == "__main__":
    main()
