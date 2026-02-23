# ADR-007: Argon2id for Password Hashing

## Status

Accepted

## Context

Ferrite provides authentication via ACLs (core engine) and session-based auth (Studio web UI). Password storage requires a secure, modern hashing algorithm.

Candidates considered:

- **Argon2id** — Winner of the Password Hashing Competition (2015), memory-hard
- **bcrypt** — Widely adopted, CPU-hard, 72-byte password limit
- **scrypt** — Memory-hard, less standardized parameter guidance
- **PBKDF2** — NIST-approved, CPU-hard only, no memory hardness

Requirements:

1. Resistance to GPU/ASIC brute-force attacks (memory hardness)
2. Configurable cost parameters for future hardware scaling
3. Available as a pure-Rust crate (no OpenSSL dependency)
4. OWASP recommendation compliance

## Decision

We use **Argon2id** via the `argon2` crate (v0.5) with default parameters.

Argon2id is the hybrid variant that provides both data-dependent (Argon2d) and data-independent (Argon2i) memory access patterns, offering resistance to both side-channel and GPU attacks.

Both `ferrite-core/src/auth/` and `ferrite-studio/src/studio/auth.rs` use the same algorithm for consistency. Legacy hash migration is supported in the core auth module.

## Consequences

### Positive

- OWASP-recommended algorithm for password storage
- Memory-hard: resistant to GPU/ASIC attacks
- Pure-Rust implementation: no C dependencies or OpenSSL linking
- PHC string format stores algorithm parameters alongside the hash

### Negative

- Higher memory usage per hash operation (~19MB with defaults) vs bcrypt (~4KB)
- Slightly slower than bcrypt for low-security configurations
- Adds ~200KB to binary size when the `crypto` feature is enabled

### Mitigations

- Password hashing is infrequent (login only), so memory spike is brief
- The `crypto` feature flag makes Argon2 opt-in for embedded/lite builds
- Default parameters follow the `argon2` crate's OWASP-aligned defaults
