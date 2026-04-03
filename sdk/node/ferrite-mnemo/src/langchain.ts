/**
 * LangChain.js BaseMemory adapter for Ferrite Mnemo.
 *
 * Provides a drop-in memory class for LangChain.js agents and chains
 * that persists conversation history in Ferrite via MEM.* commands.
 *
 * @example
 * ```ts
 * import { FerriteLangChainMemory } from "ferrite-mnemo";
 *
 * const memory = new FerriteLangChainMemory({ agentId: "my-agent" });
 * await memory.saveContext(
 *   { input: "Hello" },
 *   { output: "Hi there!" },
 * );
 * const vars = await memory.loadMemoryVariables({});
 * console.log(vars.history);
 * ```
 */
import { FerriteMemoryClient, type FerriteMemoryClientOptions, type MemoryRecord } from "./index";

export interface FerriteLangChainMemoryOptions extends FerriteMemoryClientOptions {
  agentId: string;
  sessionId?: string;
  memoryKey?: string;
  recallLimit?: number;
}

/**
 * LangChain.js–compatible memory backed by Ferrite MEM.* commands.
 *
 * Implements the same surface as LangChain's `BaseMemory`:
 *   - `loadMemoryVariables()`
 *   - `saveContext()`
 *   - `clear()`
 */
export class FerriteLangChainMemory {
  private client: FerriteMemoryClient;
  readonly agentId: string;
  readonly sessionId: string;
  readonly memoryKey: string;
  readonly recallLimit: number;

  constructor(opts: FerriteLangChainMemoryOptions) {
    this.client = new FerriteMemoryClient(opts);
    this.agentId = opts.agentId;
    this.sessionId = opts.sessionId ?? "default";
    this.memoryKey = opts.memoryKey ?? "history";
    this.recallLimit = opts.recallLimit ?? 5;
  }

  /** Keys injected into the chain / agent state. */
  get memoryVariables(): string[] {
    return [this.memoryKey];
  }

  /**
   * Load relevant memories for the current chain invocation.
   */
  async loadMemoryVariables(
    _inputs: Record<string, unknown>
  ): Promise<Record<string, MemoryRecord[]>> {
    const memories = await this.client.recall(
      this.agentId,
      this.recallLimit
    );
    return { [this.memoryKey]: memories };
  }

  /**
   * Persist a conversation turn to Ferrite memory.
   */
  async saveContext(
    inputs: Record<string, unknown>,
    outputs: Record<string, unknown>
  ): Promise<void> {
    const content = JSON.stringify({ input: inputs, output: outputs });
    await this.client.put(
      this.agentId,
      this.sessionId,
      "episodic",
      content
    );
  }

  /**
   * Clear all memories for this agent (GDPR-safe).
   */
  async clear(): Promise<void> {
    await this.client.forget(this.agentId);
  }

  /**
   * Disconnect the underlying Redis connection.
   */
  async disconnect(): Promise<void> {
    await this.client.disconnect();
  }
}
