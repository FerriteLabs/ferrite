# Troubleshooting

## Common Issues

### Server Won't Start

**Port already in use:**
```bash
# Check what's using port 6379
lsof -i :6379

# Use a different port
ferrite --port 6380
```

**Permission denied on data directory:**
```bash
# Fix permissions
sudo chown -R $(whoami) /var/lib/ferrite
```

**Config file not found:**
```bash
# Generate a default config
ferrite init --output ferrite.toml

# Or run without config (uses defaults)
ferrite
```

### Connection Issues

**Cannot connect with redis-cli:**
```bash
# Check server is running
ferrite-cli PING

# Check bind address (default: 127.0.0.1)
# For remote access, set bind = "0.0.0.0" in ferrite.toml

# Check TLS
redis-cli --tls -p 6379
```

**Authentication failures:**
```bash
# Verify password
redis-cli -a your_password PING

# Check ACL configuration
redis-cli ACL LIST
```

### Performance Issues

**High latency:**
1. Check if persistence is causing sync stalls: try `aof_sync = "everysec"`
2. Disable Transparent Huge Pages on Linux
3. Check memory pressure: `ferrite-cli INFO memory`
4. Look at slow log: `ferrite-cli SLOWLOG GET 10`

**High memory usage:**
1. Set `max_memory` in config
2. Enable eviction: `eviction_policy = "allkeys-lru"`
3. Check for large keys: `ferrite-cli --bigkeys`

### Docker Issues

**Container exits immediately:**
```bash
# Check logs
docker logs ferrite

# Verify config mount
docker run -it ferrite:latest --test-config
```

## Ferrite Doctor

The `ferrite doctor` command runs preflight diagnostics to verify your environment and configuration before starting the server.

### Running Doctor

```bash
# Run with default configuration
ferrite doctor

# Run with a specific config file
ferrite doctor --config ferrite.toml
```

### What Doctor Checks

| Check | Description | Pass Criteria |
|-------|-------------|---------------|
| Configuration | Loads and validates config file | Valid TOML, all values in range |
| Data directory | Verifies path exists and is writable | Directory exists, write test passes |
| Server port | Bind address and port availability | Port not already in use |
| Metrics port | Metrics endpoint can bind (if enabled) | Metrics port not in use |
| Kernel version | Linux kernel supports io_uring (Linux only) | Kernel ≥ 5.11 |
| Memory | Available system memory | ≥ 256 MB available |
| Disk space | Free disk on data directory volume | ≥ 1 GB available |
| TLS certificates | Cert, key, and CA files exist (if TLS enabled) | All files found on disk |
| Build info | Ferrite and Rust compiler versions | Informational |

### Example Output

```
Ferrite doctor
Config source: ferrite.toml
Data directory OK: ./data
Port OK: 127.0.0.1:6379
Metrics OK: 127.0.0.1:9090
Memory OK: 15832MB available
Disk space OK: 142GB available on ./data
Ferrite version: 0.1.0
Rust compiler: rustc 1.88.0 (e7e1dc158 2025-06-04)
Doctor checks passed
```

### When to Use Doctor

- **First installation** — verify environment before first startup
- **After config changes** — validate edits before restarting
- **Before production** — confirm resources and TLS setup
- **Debugging startup failures** — when `ferrite run` fails, doctor provides targeted diagnostics

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All checks passed |
| `1` | One or more checks failed |

## Other Diagnostics

Enable debug logging:

```bash
RUST_LOG=ferrite=debug ferrite --config ferrite.toml
```

## Getting Help

- **GitHub Issues**: [ferritelabs/ferrite/issues](https://github.com/ferritelabs/ferrite/issues)
- **Discussions**: [ferritelabs/ferrite/discussions](https://github.com/ferritelabs/ferrite/discussions)
- **Documentation**: [ferritelabs.github.io/ferrite-docs](https://ferritelabs.github.io/ferrite-docs)
