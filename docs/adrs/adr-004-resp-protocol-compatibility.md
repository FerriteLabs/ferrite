# ADR-004: RESP2/RESP3 Protocol Compatibility

- **Date:** 2026-02-20
- **Status:** Accepted

## Context

Ferrite aims to be a drop-in Redis replacement. The biggest barrier to adopting an alternative
database is migration cost — rewriting client code, changing libraries, and retraining teams.
Redis has a massive ecosystem of client libraries in every major language, all speaking the
RESP (REdis Serialization Protocol) wire format.

## Decision

We implement full RESP2 and RESP3 protocol support, targeting 90%+ Redis command compatibility:

- **RESP2** — The established protocol supported by all existing Redis clients.
- **RESP3** — The newer protocol adding typed responses (maps, sets, booleans, doubles)
  and client-side caching support via server-push notifications.
- Commands are implemented in the top-level crate's `commands/` module, organized by
  category (strings, lists, sets, hashes, sorted sets, streams, etc.).
- Protocol parsing lives in `ferrite-core` for reuse across the codebase.

## Consequences

### Positive

- Existing Redis clients (redis-py, ioredis, Lettuce, go-redis, etc.) work unmodified
- Zero migration cost for applications switching from Redis to Ferrite
- IDE extensions and monitoring tools built for Redis also work with Ferrite
- RESP3 enables advanced features like client-side caching and typed responses

### Negative

- Constrains protocol evolution — we cannot break RESP compatibility without forking
  the client ecosystem
- API design must follow Redis conventions even where better alternatives exist
- Must track Redis command additions and deprecations to maintain compatibility
- Some Redis commands have complex semantics that add implementation burden
