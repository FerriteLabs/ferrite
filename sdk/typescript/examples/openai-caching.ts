/**
 * OpenAI caching example.
 *
 * Demonstrates transparent semantic caching for OpenAI API calls,
 * reducing cost and latency for similar queries.
 *
 * Prerequisites:
 *   npm install @ferrite/ai openai
 *   export OPENAI_API_KEY="sk-..."
 *   # Start Ferrite server: cargo run --release
 */

import { cachedCompletion } from "@ferrite/ai";

async function main() {
  console.log("OpenAI + Ferrite Semantic Cache\n");

  const queries = [
    "What is the capital of France?",
    "France's capital city?", // Should hit cache
    "Explain photosynthesis briefly",
    "How does photosynthesis work?", // Should hit cache
  ];

  for (const query of queries) {
    const start = performance.now();

    const response = await cachedCompletion({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: query }],
      threshold: 0.85,
      ttl: 3600,
    });

    const latency = performance.now() - start;
    const content =
      (response as Record<string, unknown[]>).choices?.[0] ?? "N/A";

    console.log(`Query:    '${query}'`);
    console.log(`Response: ${JSON.stringify(content).slice(0, 100)}...`);
    console.log(
      `Latency:  ${latency.toFixed(1)} ms — Cache ${response._cacheHit ? "HIT" : "MISS"}`,
    );
    console.log();
  }
}

main().catch(console.error);
