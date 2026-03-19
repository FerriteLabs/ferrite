# ferrite-mnemo

Mnemo — Agent Memory OS facade.  See [ADR-018](../../docs/adrs/adr-018-mnemo-agent-memory-os.md)
and the [phase roadmap](../../docs/phases/m1-mnemo-roadmap.md).

This crate is the **public-facing data model and key-layout contract** for Mnemo.
Storage continues to live in `ferrite-ai::agent_memory`; this crate adds the
multi-tenant, versioned schema ADR-018 specifies and defines the Store-key
conventions every command handler will use.

Status: P0 spike — schema + key layout only.  Command handlers, retrieval, and
hybrid scoring land in P1+.
