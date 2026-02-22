## Summary

<!-- Brief description of the changes -->

## Type of Change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Refactoring (no functional changes)
- [ ] CI/CD or infrastructure change

## Crate(s) Affected

- [ ] `ferrite` (top-level / server)
- [ ] `ferrite-core`
- [ ] Extension crate(s): ___

## Checklist

- [ ] My code follows the project's coding conventions
- [ ] I have run `cargo fmt --all` and `cargo clippy --all-targets`
- [ ] I have added tests that prove my fix is effective or my feature works
- [ ] New and existing unit tests pass locally with `cargo test`
- [ ] I have updated relevant documentation
- [ ] My changes generate no new warnings
- [ ] Any `unsafe` blocks have `// SAFETY:` comments

## Redis Compatibility

<!-- If applicable, does this change affect Redis protocol compatibility? -->
- [ ] N/A
- [ ] Tested with redis-cli
- [ ] Tested with Redis compatibility suite

## Performance Impact

<!-- If applicable, describe the performance implications -->
- [ ] N/A
- [ ] Benchmarked with `cargo bench`
- [ ] No significant performance regression expected
