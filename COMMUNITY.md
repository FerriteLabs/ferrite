# Ferrite Community

Welcome to the Ferrite community! 🦀

Ferrite is a high-performance, tiered-storage key-value store built in Rust — designed as a drop-in Redis replacement that keeps your data hot in memory and warm on disk, so you get Redis-level speed without Redis-level memory bills.

Whether you're a seasoned Rust developer or just getting started with databases, there's a place for you here.

## Get Involved

### 🧪 External Tester Program

Join the non-production [Tester Program](./TESTER_PROGRAM.md) for one canonical
60–90 minute journey, safe diagnostics, and structured feedback. A specific
candidate/hardening campaign is scheduled and announced separately by the
project; the current Ferrite version is tracked in [CHANGELOG.md](CHANGELOG.md).

### 🐛 Report Bugs

Found something broken? [Open an issue](https://github.com/ferritelabs/ferrite/issues/new?template=bug_report.md) with steps to reproduce, and we'll triage it quickly.

### 💡 Suggest Features

Have an idea? Open a [feature request](https://github.com/ferritelabs/ferrite/issues/new?template=feature_request.md). We love hearing how people want to use Ferrite.

### 🔧 Contribute Code

1. Check out our [Contributing Guide](./CONTRIBUTING.md) for setup instructions and PR workflow.
2. Browse [`good first issue`](https://github.com/ferritelabs/ferrite/labels/good%20first%20issue) labels for beginner-friendly tasks.
3. See the crate architecture in `crates/` — each is self-contained and a great place to start.

### 📖 Improve Documentation

Docs live in [ferrite-docs](https://github.com/ferritelabs/ferrite-docs). Typo fixes, new guides, and better examples are always welcome. Run the docs site locally with:

```bash
cd ferrite-docs/website && npm install && npm start
```

### 🧪 Write Tests & Benchmarks

More test coverage and real-world benchmarks help everyone. See `cargo test` and `benches/` in the repo root.

## Communication Channels

| Channel | Link |
|---------|------|
| **GitHub Issues** | [ferritelabs/ferrite/issues](https://github.com/ferritelabs/ferrite/issues) |
| **Tester interest / questions** | [Tester Interest form](https://github.com/ferritelabs/ferrite/issues/new?template=tester_interest.yml) |

For security vulnerabilities, do not open a public issue — report them privately using [GitHub private vulnerability reporting](https://github.com/ferritelabs/ferrite/security/advisories/new).

## Key Resources

- [CONTRIBUTING.md](./CONTRIBUTING.md) — How to set up your dev environment and submit PRs
- [Good First Issues](https://github.com/ferritelabs/ferrite/labels/good%20first%20issue) — Curated tasks for new contributors
- [Architecture Overview](./docs/ARCHITECTURE.md) — Understand how Ferrite works under the hood
- [Observability Guide](./docs/OPERATIONS.md) — Monitoring, metrics, and tracing for storage operations
- [Code of Conduct](./CODE_OF_CONDUCT.md) — Our standards for a welcoming community

## Contributor Recognition

We believe every contribution matters. Here's how we say thanks:

- **Contributors list** — All contributors are recognized in our [README](./README.md) and release notes.
- **Commit credit** — We use `Co-authored-by` trailers to ensure your Git history reflects your work.
- **Shout-outs** — Significant contributions are highlighted in release announcements and on social media.
- **Maintainer path** — Consistent, high-quality contributors are invited to become project maintainers with write access.

No contribution is too small — documentation fixes, test improvements, and bug reports all count.

## License

Ferrite is open source under the [MIT License](./LICENSE). By contributing, you agree that your contributions will be licensed under the same terms.

---

_Thank you for being part of the Ferrite community. Let's build something great together._ 🚀
