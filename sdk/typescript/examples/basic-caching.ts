/**
 * Basic semantic caching example.
 *
 * Demonstrates storing and retrieving LLM responses using Ferrite's
 * semantic similarity matching.
 *
 * Prerequisites:
 *   npm install @ferrite/ai
 *   # Start Ferrite server: cargo run --release
 */

import { FerriteClient } from "@ferrite/ai";

async function main() {
  const client = new FerriteClient({ host: "127.0.0.1", port: 6380 });
  console.log("Connected to Ferrite\n");

  // Store some responses
  const entries = [
    {
      query: "What is the capital of France?",
      response: "Paris is the capital of France.",
    },
    {
      query: "Explain machine learning",
      response:
        "Machine learning is a subset of AI that enables systems to learn from data.",
    },
    {
      query: "How does photosynthesis work?",
      response:
        "Photosynthesis converts sunlight, CO2, and water into glucose and oxygen.",
    },
  ];

  console.log("Populating cache...");
  for (const entry of entries) {
    await client.semanticSet(entry.query, entry.response, undefined, 3600);
    console.log(`  Cached: '${entry.query}'`);
  }

  // Look up by semantic similarity
  console.log("\nQuerying cache...");

  const testQueries = [
    "What is France's capital?", // Similar to entry 1
    "Tell me about ML", // Similar to entry 2
    "What is quantum computing?", // No match
  ];

  for (const query of testQueries) {
    const result = await client.semanticGet(query, 0.85);
    if (result) {
      console.log(`  HIT:  '${query}'`);
      console.log(`        → ${result.response}`);
      console.log(`        similarity: ${result.similarity.toFixed(4)}`);
    } else {
      console.log(`  MISS: '${query}'`);
    }
  }

  // Stats
  const stats = await client.semanticStats();
  console.log("\nCache stats:");
  console.log(`  Hits:       ${stats.hitCount}`);
  console.log(`  Misses:     ${stats.missCount}`);
  console.log(`  Hit rate:   ${(stats.hitRate * 100).toFixed(1)}%`);
  console.log(`  Entries:    ${stats.entryCount}`);

  await client.disconnect();
}

main().catch(console.error);
