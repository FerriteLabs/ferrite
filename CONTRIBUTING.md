# Contributing to Ferrite

Thank you for your interest in contributing to Ferrite! We're building a high-performance, tiered-storage key-value store designed as a drop-in Redis replacement, and we welcome contributions from the community.

**Tagline**: *The speed of memory, the capacity of disk, the economics of cloud.*

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Submitting Changes](#submitting-changes)
- [Development Setup](#development-setup)
- [Code Style Guidelines](#code-style-guidelines)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Commit Message Format](#commit-message-format)
- [Review Process](#review-process)
- [Getting Help](#getting-help)

## Code of Conduct

This project adheres to a code of conduct that we expect all contributors to follow. Please be respectful, inclusive, and constructive in all interactions. We are committed to providing a welcoming and harassment-free experience for everyone, regardless of background or identity.

**Our Standards:**
- Be respectful and professional in all communications
- Welcome diverse perspectives and constructive feedback
- Focus on what is best for the project and community
- Show empathy towards other community members
- Gracefully accept constructive criticism

**Unacceptable Behavior:**
- Harassment, trolling, or discriminatory language
- Personal attacks or inflammatory comments
- Publishing others' private information without consent
- Any conduct that could reasonably be considered inappropriate

## Getting Started

Before you begin:
- **Rust 1.88+** required (check with `rustc --version`)
- Familiarize yourself with the [project documentation](CLAUDE.md) to understand Ferrite's architecture
- Read the [Redis protocol specification](https://redis.io/docs/reference/protocol-spec/) for protocol work
- For storage engine contributions, review the [FASTER paper](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
- **Linux users**: For full io_uring support, kernel 5.11+ recommended

## How to Contribute

There are many ways to contribute to Ferrite:

- **Report bugs** to help us improve stability
- **Suggest features** that enhance functionality
- **Submit code changes** for fixes or new features
- **Improve documentation** to help users and developers
- **Review pull requests** to share your expertise
- **Answer questions** in issues and discussions

### Reporting Bugs

Found a bug? Help us fix it by submitting a detailed bug report.

**Before submitting:**
1. Check if the issue already exists in the [issue tracker](../../issues)
2. Verify the bug exists in the latest version from `main`
3. Collect relevant information (logs, configurations, environment details)

**When submitting a bug report, include:**

```markdown
## Description
Clear and concise description of the bug

## Steps to Reproduce
1. Start Ferrite with configuration X
2. Execute command Y
3. Observe behavior Z

## Expected Behavior
What you expected to happen

## Actual Behavior
What actually happened (include error messages)

## Environment
- **OS**: Linux/macOS/Windows (version)
- **Rust**: 1.88.0 (output of `rustc --version`)
- **Ferrite**: v0.1.0 or commit hash
- **Kernel** (Linux only): 5.15.0 (for io_uring issues)

## Logs
```
RUST_LOG=ferrite=debug output here
```

## Configuration
```toml
# Your ferrite.toml (redact sensitive data)
```

## Additional Context
Any other context, screenshots, or related issues
```

**Labels to use:**
- `bug` - Confirmed bugs
- `needs-investigation` - Potential bugs requiring more info
- `performance` - Performance-related issues
- `io_uring` - Linux io_uring specific issues

### Suggesting Features

We welcome feature suggestions that align with Ferrite's goals of high performance and Redis compatibility.

**When suggesting a feature:**

```markdown
## Use Case
Describe the problem you're trying to solve. What workflow would this enable?

## Proposed Solution
Outline your suggested approach. How would users interact with this feature?

## Redis Compatibility
- Does Redis have this feature? If so, link to documentation
- If not, explain why Ferrite should have it

## Performance Impact
- Expected impact on throughput/latency
- Memory/disk usage considerations
- Impact on existing features

## Alternatives Considered
What other approaches did you consider? Why is this approach preferred?

## Example Usage
```rust
// Code example showing how this would be used
```

## Implementation Notes
Any technical details about how this might be implemented (optional)
```

**Feature Guidelines:**
- Features should generally maintain Redis protocol compatibility
- Performance characteristics should align with Ferrite's goals (see [CLAUDE.md](CLAUDE.md))
- Consider whether the feature belongs in core or as an extension
- Breaking changes require strong justification

### Submitting Changes

Ready to contribute code? Here's the workflow:

1. **Fork** the repository on GitHub
2. **Clone** your fork locally
3. **Create a branch** from `main` for your changes
4. **Make your changes** following our code style guidelines
5. **Add tests** for your changes
6. **Run all checks** (`cargo fmt`, `cargo clippy`, `cargo test`)
7. **Commit** with descriptive messages (see format below)
8. **Push** to your fork
9. **Open a Pull Request** against the `main` branch

## Development Setup

### Initial Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/ferrite.git
cd ferrite

# Add upstream remote
git remote add upstream https://github.com/ORIGINAL_OWNER/ferrite.git

# Install dependencies and build
cargo build

# Run tests to verify setup
cargo test
```

### Development Workflow

```bash
# Create a feature branch
git checkout -b feature/my-feature-name

# Make changes and run tests frequently
cargo test

# Run specific tests
cargo test test_name

# Run tests with logging
RUST_LOG=ferrite=debug cargo test -- --nocapture

# Run tests with optimizations (important for performance-critical code)
cargo test --release

# Check formatting
cargo fmt --check

# Run linter
cargo clippy --all-targets --all-features -- -D warnings

# Run all checks at once
make check  # if Makefile exists

# Run benchmarks (when working on performance)
cargo bench

# Generate and view documentation
cargo doc --open
```

### Branch Naming Conventions

Use descriptive branch names that indicate the type and scope of changes:

- `feature/add-hgetall` - New features
- `fix/resp-parser-panic` - Bug fixes
- `perf/optimize-hybridlog-reads` - Performance improvements
- `docs/update-contributing` - Documentation updates
- `refactor/cleanup-storage-module` - Code refactoring
- `test/add-persistence-tests` - Test additions

### Platform-Specific Notes

**Linux (Full io_uring support):**
- Requires kernel 5.11+ for optimal io_uring features
- Install development packages: `sudo apt-get install liburing-dev` (Debian/Ubuntu)
- Some tests may be Linux-specific (marked with `#[cfg(target_os = "linux")]`)

**macOS:**
- io_uring features will gracefully fall back to `tokio::fs`
- All networking and protocol features work normally
- Use the latest Xcode command line tools

**Windows:**
- Similar fallback behavior to macOS for io_uring
- Ensure you're using the latest Rust toolchain
- Some benchmarks may show different characteristics

## Code Style Guidelines

Ferrite follows standard Rust conventions with specific project requirements. See [CLAUDE.md](CLAUDE.md) for comprehensive coding guidelines.

### General Rust Style

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `rustfmt` with default settings (enforced in CI)
- All public items **must** have documentation comments (`///`)
- Prefer `Result<T, E>` over panics for recoverable errors
- Use `thiserror` for library errors, `anyhow` for application errors
- Avoid `unwrap()` and `expect()` in library code; use `?` operator

### Naming Conventions

```rust
// Types: PascalCase
struct HybridLog { }
enum CommandType { }
trait StorageEngine { }

// Functions and methods: snake_case
fn parse_command() { }
fn get_value(&self, key: &str) -> Result<Value> { }

// Constants: SCREAMING_SNAKE_CASE
const MAX_KEY_SIZE: usize = 512;
const DEFAULT_PORT: u16 = 6379;

// Modules: snake_case
mod hybrid_log;
mod command_parser;
mod resp_protocol;

// Lifetimes: single lowercase letter or descriptive
fn process<'a>(data: &'a [u8]) { }
fn parse<'input>(text: &'input str) { }
```

### Error Handling

Define clear, actionable errors using `thiserror`:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FerriteError {
    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("invalid command: {0}")]
    InvalidCommand(String),

    #[error("value exceeds maximum size of {max} bytes (got {actual})")]
    ValueTooLarge { max: usize, actual: usize },

    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
}

// Use type alias for convenience
pub type Result<T> = std::result::Result<T, FerriteError>;
```

**Error handling best practices:**
- Provide context in error messages
- Use error variants that can be programmatically handled
- Preserve the error chain with `#[from]`
- Add `#[error("...")]` messages that are user-friendly

### Unsafe Code Guidelines

**Minimize use of `unsafe` code.** When absolutely necessary for performance:

```rust
// Every unsafe block MUST have a SAFETY comment explaining:
// 1. What invariants must be upheld
// 2. Why those invariants are satisfied in this context
// 3. What could go wrong if invariants are violated

// SAFETY: We have exclusive access to this memory region because:
// 1. The epoch system guarantees no other thread holds a reference
// 2. The address was returned from our allocator in this epoch
// 3. The buffer is properly aligned for type T
// 4. The memory is initialized with valid data before use
unsafe {
    std::ptr::write(addr as *mut T, value);
}
```

**Requirements for all unsafe code:**
- Wrap in safe abstractions with documented guarantees
- Add comprehensive tests including edge cases
- Document all invariants that must be maintained
- Consider safe alternatives first (even if slightly slower)
- Get review from multiple maintainers

### Documentation

All public items require documentation:

```rust
/// Retrieves a value from storage or returns a default.
///
/// This method performs a lock-free read from the appropriate tier
/// (mutable, read-only, or disk) based on the key's current location.
///
/// # Arguments
///
/// * `key` - The key to look up (max 512MB)
/// * `default` - Default value to return if key doesn't exist
///
/// # Returns
///
/// Returns the value associated with the key, or the default if the
/// key is not present.
///
/// # Errors
///
/// Returns [`FerriteError::InvalidKey`] if the key is malformed or
/// exceeds the maximum size.
///
/// Returns [`FerriteError::Storage`] if an I/O error occurs reading
/// from disk tier.
///
/// # Examples
///
/// ```
/// use ferrite::storage::HybridLog;
///
/// let storage = HybridLog::new()?;
/// let value = storage.get_or_default("mykey", "default")?;
/// assert_eq!(value, "default");
/// ```
///
/// # Performance
///
/// - Mutable tier reads: O(1), ~100ns
/// - Read-only tier: O(1), ~200ns (mmap)
/// - Disk tier: O(1), ~10μs (io_uring)
pub fn get_or_default(&self, key: &str, default: &str) -> Result<String> {
    // implementation
}
```

### Performance-Critical Code

When writing performance-critical code:

```rust
// Use #[inline] for small, hot-path functions
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    // fast hash implementation
}

// Use #[cold] for error paths to optimize hot path
#[cold]
fn handle_error(err: Error) -> Result<()> {
    // error handling
}

// Document performance characteristics
/// Fast path for mutable tier lookups.
///
/// # Performance
/// - Expected: <100ns
/// - No allocations in hot path
/// - Lock-free reads using epoch-based reclamation
#[inline]
fn lookup_mutable(&self, key: &[u8]) -> Option<&Value> {
    // implementation
}
```

**Performance guidelines:**
- Profile before optimizing (`cargo flamegraph`, `perf`)
- Prefer stack allocation for small buffers
- Use `bytes::Bytes` for zero-copy buffer sharing
- Avoid allocations in hot paths
- Document performance characteristics and expectations
- Add benchmarks for critical paths (see Testing section)

## Testing Requirements

All contributions must include appropriate tests. Ferrite uses multiple testing strategies:

### Test Types

**Unit Tests** (within source files)
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let result = parse("+OK\r\n");
        assert_eq!(result, Ok(Response::SimpleString("OK".to_string())));
    }

    #[test]
    fn test_parse_error() {
        let result = parse("-ERR unknown command\r\n");
        assert!(matches!(result, Ok(Response::Error(_))));
    }

    #[test]
    #[should_panic(expected = "invalid input")]
    fn test_parse_invalid_input() {
        parse("invalid\r\n").unwrap();
    }
}
```

**Async Tests**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_handling() {
        let listener = start_test_server().await;
        let client = connect_test_client().await;

        let response = client.ping().await.unwrap();
        assert_eq!(response, "PONG");
    }
}
```

**Integration Tests** (in `tests/` directory)
```rust
// tests/redis_compat.rs

#[tokio::test]
async fn test_get_set_flow() {
    let server = spawn_test_server().await;
    let client = redis::Client::open(server.url()).unwrap();
    let mut con = client.get_connection().unwrap();

    redis::cmd("SET")
        .arg("key")
        .arg("value")
        .query::<()>(&mut con)
        .unwrap();

    let result: String = redis::cmd("GET")
        .arg("key")
        .query(&mut con)
        .unwrap();

    assert_eq!(result, "value");
}
```

**Property-Based Tests**
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_roundtrip_encoding(s in "\\PC*") {
        let encoded = encode_bulk_string(&s);
        let decoded = parse_bulk_string(&encoded)?;
        prop_assert_eq!(s, decoded);
    }
}
```

### Dependency Policy

Dependency checks run in CI (`.github/workflows/security.yml`). Run locally as
needed:

```bash
cargo install cargo-deny cargo-audit cargo-outdated cargo-license
cargo deny check --all-features
cargo audit
cargo outdated --exit-code 0
cargo license
```

**Benchmarks** (in `benches/` directory)
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_get_hot_key(c: &mut Criterion) {
    let storage = setup_test_storage();
    storage.set("hot_key", "value").unwrap();

    c.bench_function("get_hot_key", |b| {
        b.iter(|| {
            storage.get(black_box("hot_key")).unwrap()
        })
    });
}

criterion_group!(benches, bench_get_hot_key);
criterion_main!(benches);
```

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run with logging
RUST_LOG=ferrite=debug cargo test -- --nocapture

# Run integration tests only
cargo test --test redis_compat

# Run tests in release mode (important for performance tests)
cargo test --release

# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench throughput
```

### Test Requirements for PRs

Before submitting a PR, ensure:

- [ ] All new code has unit tests
- [ ] All tests pass on your platform (`cargo test`)
- [ ] Integration tests pass (if applicable)
- [ ] Tests cover both success and error cases
- [ ] Edge cases and boundary conditions are tested
- [ ] For Redis commands, compatibility is verified
- [ ] Performance-critical code has benchmarks
- [ ] No test warnings or ignored tests without justification

**Test Coverage Guidelines:**
- Aim for >80% coverage for new code
- 100% coverage for error handling paths
- All public APIs must have tests
- Document any untested edge cases with `// TODO: test ...`

## Pull Request Process

### Before Submitting

Ensure your PR meets these requirements:

- [ ] Code follows style guidelines (`cargo fmt --check` passes)
- [ ] No clippy warnings (`cargo clippy -- -D warnings` passes)
- [ ] All tests pass (`cargo test` passes)
- [ ] New tests added for changes
- [ ] Documentation updated (code comments, README if needed)
- [ ] Benchmarks run if performance-related
- [ ] Commit messages follow format guidelines
- [ ] Branch is up-to-date with `main`

### PR Description Template

Use this template for your PR description:

```markdown
## Summary
Brief description of what this PR does and why it's needed.
Closes #123

## Changes
- Detailed list of changes
- Each bullet should describe one logical change
- Highlight any breaking changes or migrations needed

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Performance improvement
- [ ] Documentation update
- [ ] Refactoring (no functional changes)

## Testing
Describe how you tested these changes:
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed (describe steps)
- [ ] Benchmarks run (attach results if performance-related)

## Performance Impact
(If applicable) Describe performance impact:
- Benchmark results (before/after)
- Memory usage changes
- Latency impact

## Redis Compatibility
(If applicable) How does this affect Redis compatibility?
- [ ] Matches Redis behavior exactly
- [ ] Documented deviation from Redis
- [ ] Not applicable

## Documentation
- [ ] Code comments added/updated
- [ ] Public API documented
- [ ] README updated (if needed)
- [ ] CLAUDE.md updated (if architecture changed)

## Related Issues
- Fixes #123
- Related to #456
- Blocks #789

## Checklist
- [ ] Self-review completed
- [ ] Tests pass locally
- [ ] No new warnings
- [ ] Commits follow convention
- [ ] Ready for review
```

### PR Size Guidelines

Keep PRs focused and reviewable:

- **Small**: <100 lines changed - ideal for quick review
- **Medium**: 100-500 lines - normal PR size
- **Large**: 500-1000 lines - needs clear organization
- **Extra Large**: >1000 lines - should be split if possible

**Tips for large changes:**
- Break into multiple PRs with logical boundaries
- Create tracking issue for multi-PR features
- Submit infrastructure/refactoring PRs before feature PRs
- Provide detailed documentation for complex changes

### What Happens Next

1. **Automated Checks**: CI will run tests, linting, and formatting checks
2. **Initial Review**: Maintainers will review within 3-5 business days
3. **Feedback**: You'll receive comments, questions, or change requests
4. **Iteration**: Make requested changes and push updates
5. **Approval**: PRs need approval from at least one maintainer
6. **Merge**: Maintainers will merge once all requirements are met

## Commit Message Format

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for clear, structured commit history:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Commit Types

- `feat`: New feature for users
- `fix`: Bug fix for users
- `docs`: Documentation changes
- `style`: Code style changes (formatting, missing semicolons, etc.)
- `refactor`: Code refactoring without behavior changes
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `build`: Build system or dependency changes
- `ci`: CI/CD configuration changes
- `chore`: Maintenance tasks (releases, etc.)

### Commit Scope (optional)

Indicates what part of the codebase is affected:

- `commands`: Redis command implementations
- `protocol`: RESP protocol parsing/encoding
- `storage`: Storage engine (HybridLog, etc.)
- `server`: Network server and connection handling
- `persistence`: AOF, checkpoints, recovery
- `metrics`: Prometheus metrics
- `config`: Configuration handling
- `bench`: Benchmarks

### Commit Examples

**Simple fix:**
```
fix(protocol): handle empty bulk strings correctly

The RESP parser would panic when encountering empty bulk strings
($0\r\n\r\n). Added validation to return an empty string instead.

Fixes #234
```

**New feature:**
```
feat(commands): implement HGETALL command

Add HGETALL for hash operations to improve Redis compatibility.
Implementation uses optimized batch retrieval from hash storage.

- Add HGETALL parser in protocol module
- Implement command handler in commands/hashes.rs
- Add comprehensive tests for edge cases
- Update command coverage documentation

Closes #456
```

**Performance improvement:**
```
perf(storage): optimize HybridLog read hot path

Reduce allocations in the read path by reusing thread-local buffers.
Improves P99 latency by ~15% in throughput benchmarks.

Benchmark results (ops/sec, P99 latency):
- Before: 450K ops/sec, 1.2ms P99
- After:  480K ops/sec, 1.0ms P99

Workload: 80% GET, 20% SET, 1KB values
```

**Breaking change:**
```
feat(config)!: change default bind address to localhost

BREAKING CHANGE: The default bind address has changed from 0.0.0.0
to 127.0.0.1 for security. Production deployments must explicitly
set bind = "0.0.0.0" in ferrite.toml if remote access is needed.

This improves security for development and local deployments.

Migration: Add `bind = "0.0.0.0"` to [server] section if needed.
```

**Commit Message Guidelines:**
- Use imperative mood ("Add feature" not "Added feature")
- First line (subject) should be ≤50 characters
- Separate subject from body with a blank line
- Wrap body at 72 characters
- Explain **what** and **why**, not **how** (code shows how)
- Reference issues and PRs where relevant

## Review Process

### What to Expect

1. **Automated Checks** (~5 minutes)
   - CI runs tests, clippy, formatting
   - Build verification on Linux, macOS, Windows
   - Code coverage analysis

2. **Initial Review** (within 3-5 business days)
   - Maintainer reviews code and design
   - Provides feedback or asks questions
   - May request changes or clarifications

3. **Iteration** (as needed)
   - Address feedback by pushing new commits
   - Respond to comments and questions
   - Mark conversations as resolved when addressed

4. **Approval** (after feedback addressed)
   - At least one maintainer approval required
   - Complex changes may need multiple approvals
   - Final CI checks must pass

5. **Merge** (by maintainers)
   - Squash or rebase merge based on commit history
   - Included in next release
   - Acknowledged in release notes

### Review Criteria

Reviewers will evaluate:

**Code Quality:**
- Follows Rust API guidelines and project conventions
- Clear, readable code with appropriate comments
- Proper error handling without panics
- No unsafe code without justification and safety comments

**Testing:**
- Adequate test coverage
- Tests cover edge cases and error paths
- Integration tests for user-facing changes
- Benchmarks for performance-critical code

**Documentation:**
- Public APIs have rustdoc comments
- Complex logic has explanatory comments
- README or other docs updated if needed
- Migration guide for breaking changes

**Performance:**
- No obvious performance regressions
- Benchmarks show improvements (if applicable)
- Memory usage is reasonable
- Async code uses proper patterns

**Redis Compatibility:**
- Commands match Redis behavior (or document deviations)
- Protocol handling is correct
- Error messages are appropriate

**Security:**
- No obvious security vulnerabilities
- Input validation is present
- Authentication/authorization respected
- Safe handling of untrusted data

### Responding to Feedback

**Best practices:**
- Be open to suggestions and constructive criticism
- Ask questions if feedback is unclear
- Explain your reasoning when disagreeing
- Make requested changes or propose alternatives
- Mark conversations as resolved when addressed
- Push new commits rather than force-pushing during review
- Be patient and respectful

**Example responses:**

Good:
```
Thanks for catching that! I've added validation for empty keys in commit abc123.
```

Good:
```
Interesting point about using DashMap here. I chose HashMap + RwLock because
the access pattern is 95% reads and the lock contention is minimal. Would you
like me to add a benchmark comparing both approaches?
```

Avoid:
```
It works fine on my machine.
```

Avoid:
```
That's how Redis does it.  [without verification]
```

### After Your PR is Merged

- Update your fork with the latest changes
- Close related issues if not auto-closed
- Consider contributing to related areas
- Help review others' PRs in areas you know well

## Getting Help

Need assistance? We're here to help:

**For Questions:**
- Open a [GitHub Discussion](../../discussions)
- Create an issue with the `question` label
- Check existing issues and discussions first

**For Architecture Questions:**
- Refer to [CLAUDE.md](CLAUDE.md) for technical details
- Ask in discussions for design guidance
- Reference the FASTER paper for storage engine questions

**For Redis Compatibility:**
- Check [Redis documentation](https://redis.io/docs/)
- Look at existing command implementations
- Ask in discussions for clarification

**For Contributing Process:**
- Re-read this guide
- Look at merged PRs for examples
- Ask maintainers for guidance

## Recognition and Acknowledgments

We value and recognize our contributors:

- **Release Notes**: Significant contributions acknowledged in release notes
- **Contributors File**: All contributors listed in repository
- **Technical Docs**: Complex contributions mentioned in architecture docs
- **GitHub Profile**: Contributions visible on your GitHub profile

## Additional Resources

**Project Documentation:**
- [CLAUDE.md](CLAUDE.md) - Architecture and development guide
- [README.md](README.md) - Project overview and quick start
- [SECURITY.md](SECURITY.md) - Security policy and best practices

**External Resources:**
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [FASTER Paper](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
- [Redis Protocol Spec](https://redis.io/docs/reference/protocol-spec/)
- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [Epoch-Based Reclamation](https://aturon.github.io/blog/2015/08/27/epoch/)

## License

By contributing to Ferrite, you agree that your contributions will be licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.

---

Thank you for contributing to Ferrite! Your efforts help build a better, faster key-value store for everyone. We look forward to working with you!
