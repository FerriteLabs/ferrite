# Take-Home Exercise — Senior Rust Engineer

> **Time box: 4 hours.** We mean it. A focused 4-hour solution beats a polished weekend
> project. If you run out of time, leave TODOs explaining what you'd do next.

---

## Task: WIT-Bound Function Host

Implement a minimal WebAssembly function host that:

1. **Loads a `.wasm` module** compiled from a WIT (WebAssembly Interface Type) definition.
2. **Exposes a host function** that the guest can call — for example, a key-value `get(key: string) -> option<string>` backed by an in-memory `HashMap`.
3. **Invokes a guest-exported function** — for example, `transform(key: string) -> string` — that reads a value via the host function, transforms it, and returns the result.
4. **Handles errors gracefully** — missing keys, malformed modules, guest traps.

You may use [Wasmtime](https://github.com/bytecodealliance/wasmtime) and the
[`wasmtime-wasi`](https://docs.rs/wasmtime-wasi) crate, or any Wasm runtime you prefer.

### Deliverables

| Artifact | Required |
|---|---|
| Rust library crate with the host implementation | ✅ |
| At least one guest `.wasm` module (source + compiled) | ✅ |
| Unit tests covering: happy path, missing key, invalid module | ✅ |
| `README.md` with build instructions and design notes | ✅ |
| Benchmarks (e.g., `criterion`) | Optional |

### Evaluation Criteria

We review submissions on four axes, equally weighted:

1. **Correctness.** Does it work? Are edge cases handled? Does it pass its own tests?
2. **Safety.** Is `unsafe` used only when necessary and with `// SAFETY:` comments? Are resources cleaned up properly? Is error handling exhaustive (no `.unwrap()` in library code)?
3. **Testing.** Are tests meaningful (not just "it compiles")? Do they cover failure modes? Is test output clear on failure?
4. **Documentation.** Can a reviewer understand the design from the README and doc comments alone? Are tradeoffs explained?

### What We Don't Care About

- Polished CI/CD pipelines.
- Production-grade performance tuning (but awareness of perf implications is great).
- Supporting every edge case — scope ruthlessly and document what you cut.

### Starter WIT Definition (Optional)

You're free to define your own WIT, but here's a starting point:

```wit
package ferrite:exercise;

interface kv-store {
    get: func(key: string) -> option<string>;
    set: func(key: string, value: string);
}

world function-host {
    import kv-store;
    export transform: func(key: string) -> string;
}
```

---

## Submission

- **GitHub repository** (preferred): public or private (add `ferrite-hiring` as a collaborator).
- **Alternative submission**: if you cannot use GitHub, open a [GitHub Discussion](https://github.com/ferritelabs/ferrite/discussions) before starting to arrange another submission method; do not post submission materials publicly.

Include a `TIME_LOG.md` noting roughly how you spent your 4 hours (e.g., "1h design,
2h implementation, 0.5h tests, 0.5h docs"). This helps us calibrate, not judge.

---

## Questions?

If anything is unclear, open a [GitHub Discussion](https://github.com/ferritelabs/ferrite/discussions). We'd rather answer a question than have you spend time guessing our intent. Report suspected vulnerabilities only through [GitHub private vulnerability reporting](https://github.com/ferritelabs/ferrite/security/advisories/new).
