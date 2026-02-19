/**
 * LangChain.js semantic caching example.
 *
 * Demonstrates using Ferrite as a transparent semantic cache for LangChain
 * LLM calls, reducing API costs for semantically similar queries.
 *
 * Prerequisites:
 *   npm install @ferrite/ai @langchain/core @langchain/openai
 *   export OPENAI_API_KEY="sk-..."
 *   # Start Ferrite server: cargo run --release
 */

import { FerriteSemanticCache } from "@ferrite/ai";

async function main() {
  const cache = new FerriteSemanticCache({
    host: "127.0.0.1",
    port: 6380,
    similarityThreshold: 0.85,
    ttl: 3600,
    namespace: "lc_demo",
  });

  console.log("LangChain.js + Ferrite Semantic Cache\n");

  // Simulate LangChain cache workflow
  const llmString = "gpt-4o-mini";
  const testCases = [
    {
      prompt: "What is the capital of France?",
      expected: "Cache MISS (first call)",
    },
    {
      prompt: "France's capital city?",
      expected: "Cache HIT (semantically similar)",
    },
    {
      prompt: "Explain quantum entanglement",
      expected: "Cache MISS (different topic)",
    },
    {
      prompt: "What is quantum entanglement?",
      expected: "Cache HIT (semantically similar)",
    },
  ];

  for (const { prompt, expected } of testCases) {
    const start = performance.now();

    // Lookup
    let result = await cache.lookup(prompt, llmString);
    if (!result) {
      // Simulate LLM call
      const fakeResponse = `This is a simulated response for: ${prompt}`;
      await cache.update(prompt, llmString, [{ text: fakeResponse }]);
      result = [{ text: fakeResponse }];
    }

    const latency = performance.now() - start;

    console.log(`Query:    '${prompt}'`);
    console.log(`Response: ${result[0].text.slice(0, 80)}...`);
    console.log(`Latency:  ${latency.toFixed(1)} ms — ${expected}`);
    console.log();
  }
}

main().catch(console.error);
