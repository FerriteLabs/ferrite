# Security Policy

## Table of Contents

- [Supported Versions](#supported-versions)
- [Reporting a Vulnerability](#reporting-a-vulnerability)
- [Security Best Practices](#security-best-practices)
- [Known Security Considerations](#known-security-considerations)
- [Security Updates](#security-updates)

## Supported Versions

As Ferrite is currently in active development (pre-1.0), security updates are provided for the latest release version only.

| Version | Supported          | Status |
| ------- | ------------------ | ------ |
| 0.x.x   | :white_check_mark: | Active development |
| < 0.1.0 | :x:                | Not supported |

**Version Support Policy:**
- **Latest Release**: Receives all security updates and patches
- **Pre-release/Development**: Use at your own risk; no security guarantees
- **Post-1.0**: We will maintain the latest major version and the previous major version for critical security issues

## Reporting a Vulnerability

We take security vulnerabilities seriously and appreciate responsible disclosure. If you discover a security issue in Ferrite, please help us address it quickly and responsibly.

### How to Report

**DO NOT** open a public GitHub issue for security vulnerabilities.

Instead, please use one of these secure channels:

1. **GitHub Security Advisories** (Preferred):
   - Navigate to the [Security tab](../../security/advisories/new)
   - Click "Report a vulnerability"
   - Fill out the private disclosure form

2. **Email** (Alternative):
   - **Email**: Open a private security advisory via GitHub (preferred method)
   - For urgent matters, contact the maintainer through GitHub profile
   - Include "SECURITY" in the subject line

### What to Include

When reporting a vulnerability, please provide as much information as possible:

```markdown
## Vulnerability Description
Clear description of the security issue and its potential impact

## Affected Components
- Which modules/features are affected
- Affected versions (if known)

## Severity Assessment
Your assessment of the severity (Critical/High/Medium/Low)

## Attack Vector
How could an attacker exploit this vulnerability?
- Network accessible?
- Authentication required?
- User interaction required?

## Impact
What could an attacker accomplish?
- Data confidentiality breach
- Data integrity violation
- Service availability impact
- Privilege escalation
- Remote code execution

## Steps to Reproduce
Detailed steps to reproduce the vulnerability:
1. Start Ferrite with configuration X
2. Send command Y
3. Observe behavior Z

## Proof of Concept
- Code snippets, scripts, or commands demonstrating the issue
- Example payloads (if applicable)
- Screenshots or logs (if helpful)

## Suggested Fix (Optional)
If you have ideas for remediation, please share them

## Your Environment
- Ferrite version
- Operating system and version
- Deployment configuration (Docker, bare metal, etc.)

## Disclosure Timeline
Your expectations for disclosure timing
```

### What NOT to Include

- Do not include actual credentials or secrets
- Do not share exploit code publicly before resolution
- Do not test vulnerabilities on production systems you don't own

### Response Timeline

We are committed to addressing security issues promptly:

| Stage | Timeline |
|-------|----------|
| **Initial Response** | Within 48 hours |
| **Acknowledgment & Triage** | Within 7 days |
| **Status Updates** | Every 7 days |
| **Resolution Target (Critical)** | Within 30 days |
| **Resolution Target (High)** | Within 60 days |
| **Resolution Target (Medium/Low)** | Within 90 days |

**Note**: Timelines may vary based on complexity and severity. We will keep you informed throughout the process.

### Disclosure Policy

We follow a coordinated disclosure process:

1. **Receipt**: We acknowledge receipt of your report within 48 hours
2. **Triage**: We assess the vulnerability and determine severity (within 7 days)
3. **Development**: We develop and test a fix
4. **Notification**: We notify you when a fix is ready
5. **Release**: We release the security update
6. **Disclosure**: We publicly disclose the vulnerability (typically 90 days after fix release or by mutual agreement)
7. **Credit**: We acknowledge your responsible disclosure (unless you prefer anonymity)

**Public Disclosure:**
- We will coordinate disclosure timing with you
- Disclosure typically occurs 90 days after the patch is available
- Earlier disclosure may occur if the vulnerability is being actively exploited
- We will credit you in the security advisory (unless you request anonymity)

## Security Best Practices

### Deployment Recommendations

#### Network Security

**1. Bind to Localhost by Default**

For development and local deployments:

```toml
[server]
bind = "127.0.0.1"
port = 6379
```

**2. Use TLS for Remote Connections**

Always use TLS when exposing Ferrite to untrusted networks:

```toml
[tls]
enabled = true
cert_file = "/etc/ferrite/tls/server.crt"
key_file = "/etc/ferrite/tls/server.key"
ca_cert_file = "/etc/ferrite/tls/ca.crt"  # For mutual TLS
min_protocol_version = "1.2"  # TLS 1.2 or higher
cipher_suites = [
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
]
```

**TLS Best Practices:**
- Use certificates from a trusted CA
- Keep certificates and keys secure (600 permissions, owned by ferrite user)
- Rotate certificates before expiration
- Use strong cipher suites only
- Consider mutual TLS (mTLS) for client authentication

**3. Firewall Configuration**

Restrict network access using firewall rules:

```bash
# Allow only from specific IP ranges
sudo iptables -A INPUT -p tcp --dport 6379 -s 10.0.0.0/8 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 6379 -j DROP

# For Prometheus metrics endpoint (if exposed)
sudo iptables -A INPUT -p tcp --dport 9090 -s MONITORING_IP -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9090 -j DROP
```

**4. Network Segmentation**

- Deploy Ferrite in a private network or VPC
- Use bastion hosts or VPN for administrative access
- Isolate database traffic from public internet
- Use security groups or network policies in cloud/Kubernetes deployments

#### Authentication and Authorization

**1. Always Enable Authentication in Production**

Never run Ferrite without authentication in production:

```toml
[auth]
enabled = true
# Use a strong password (16+ characters)
password = "$FERRITE_PASSWORD"  # Use environment variable
require_pass = true
```

**Password Security:**
- Minimum 16 characters recommended
- Use a password manager or secrets management system (HashiCorp Vault, AWS Secrets Manager, etc.)
- Rotate passwords periodically (every 90 days recommended)
- Use environment variables or secret files, never hardcode passwords
- Different passwords for different environments (dev/staging/prod)

**2. Use ACLs for Fine-Grained Access Control**

Implement least privilege principle with ACLs:

```toml
[acl]
enabled = true
users_file = "/etc/ferrite/users.acl"
default_user_enabled = false  # Disable default user in production
```

**3. ACL Configuration Examples**

`/etc/ferrite/users.acl`:

```
# Read-only user for analytics and monitoring
user analytics on >STRONG_PASSWORD_HERE ~reports:* ~metrics:* +get +mget +scan +exists +ttl +info

# Application user with limited write access
user webapp on >STRONG_PASSWORD_HERE ~app:* ~session:* +@read +@write +@string +@hash +@list -flushdb -flushall -config

# Admin user (use sparingly, preferably for emergency access only)
user admin on >VERY_STRONG_PASSWORD_HERE ~* +@all

# CI/CD user for health checks
user healthcheck on >HEALTHCHECK_PASSWORD ~* +ping +info

# Disable the default user
user default off nopass ~* -@all
```

**ACL Best Practices:**
- Grant only necessary permissions
- Use separate users for different applications
- Restrict dangerous commands (FLUSHALL, CONFIG, DEBUG, etc.)
- Use key patterns (`~prefix:*`) to isolate namespaces
- Regularly audit ACL configurations
- Monitor failed authentication attempts

#### Data Protection

**1. Enable Persistence with Durability Guarantees**

Configure persistence to prevent data loss:

```toml
[persistence]
enabled = true
dir = "/var/lib/ferrite"

# Append-Only File (AOF) for durability
aof_enabled = true
aof_filename = "appendonly.aof"
aof_fsync = "everysec"  # Balance between performance and durability
# Options: "always" (safest, slowest), "everysec" (balanced), "no" (fastest, least safe)

# Snapshots (checkpoints)
checkpoint_enabled = true
checkpoint_interval_seconds = 3600  # Every hour
checkpoint_keep_count = 3  # Keep last 3 checkpoints
```

**Fsync Options:**
- `always`: Fsync after every write (maximum durability, significant performance impact)
- `everysec`: Fsync every second (Redis-compatible, balanced approach)
- `no`: Let OS decide when to fsync (best performance, risk of data loss)

**2. Encrypt Data at Rest**

Use filesystem-level encryption for data at rest:

**Linux (LUKS):**
```bash
# Create encrypted volume
cryptsetup luksFormat /dev/sdb1
cryptsetup luksOpen /dev/sdb1 ferrite-data

# Create filesystem and mount
mkfs.ext4 /dev/mapper/ferrite-data
mount /dev/mapper/ferrite-data /var/lib/ferrite
```

**Cloud Deployments:**
- **AWS**: Enable EBS encryption
- **GCP**: Enable disk encryption
- **Azure**: Enable Azure Disk Encryption
- **Kubernetes**: Use encrypted PersistentVolumes

**3. Secure Backups**

Protect backups with encryption and access controls:

```bash
# Encrypt backup with GPG
tar czf - /var/lib/ferrite | gpg --encrypt --recipient admin@example.com > ferrite-backup.tar.gz.gpg

# Or use age (modern encryption tool)
tar czf - /var/lib/ferrite | age -r <public-key> > ferrite-backup.tar.gz.age

# Store backups in secure location
aws s3 cp ferrite-backup.tar.gz.gpg s3://secure-backups/ferrite/ --sse AES256
```

**Backup Best Practices:**
- Encrypt all backups
- Store backups in separate location/region
- Use immutable storage (S3 Object Lock, etc.)
- Test restore procedures regularly
- Secure backup credentials
- Set appropriate retention policies

#### Container Security

When running Ferrite in containers (Docker, Kubernetes):

**1. Run as Non-Root User**

```dockerfile
# Dockerfile
FROM rust:1.88 AS builder
# ... build steps ...

FROM debian:bookworm-slim
RUN groupadd -r ferrite && useradd -r -g ferrite ferrite
USER ferrite
COPY --from=builder --chown=ferrite:ferrite /app/target/release/ferrite /usr/local/bin/
CMD ["ferrite", "--config", "/etc/ferrite/ferrite.toml"]
```

**2. Use Read-Only Root Filesystem**

```yaml
# Kubernetes
apiVersion: v1
kind: Pod
metadata:
  name: ferrite
spec:
  containers:
  - name: ferrite
    image: ferrite:latest
    securityContext:
      readOnlyRootFilesystem: true
      runAsNonRoot: true
      runAsUser: 1000
      allowPrivilegeEscalation: false
    volumeMounts:
    - name: data
      mountPath: /var/lib/ferrite
    - name: tmp
      mountPath: /tmp
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: ferrite-data
  - name: tmp
    emptyDir: {}
```

**3. Drop Unnecessary Capabilities**

```yaml
securityContext:
  capabilities:
    drop:
    - ALL
    add:
    - NET_BIND_SERVICE  # Only if binding to privileged ports (<1024)
```

**4. Resource Limits**

Prevent resource exhaustion attacks:

```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "1"
  limits:
    memory: "4Gi"
    cpu: "2"
```

**5. Network Policies**

Restrict network access in Kubernetes:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ferrite-netpol
spec:
  podSelector:
    matchLabels:
      app: ferrite
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: backend
    ports:
    - protocol: TCP
      port: 6379
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: monitoring
    ports:
    - protocol: TCP
      port: 9090
```

**6. Image Security**

- Use minimal base images (distroless, alpine)
- Scan images for vulnerabilities (`trivy`, `grype`)
- Sign images and verify signatures
- Use private registries with access control
- Keep base images updated

### Monitoring and Auditing

**1. Enable Command Logging**

Track all commands for security auditing:

```toml
[logging]
level = "info"
audit_enabled = true
audit_log = "/var/log/ferrite/audit.log"
audit_commands = ["SET", "DEL", "FLUSHDB", "FLUSHALL", "CONFIG"]  # Log sensitive commands
```

**2. Monitor for Suspicious Activity**

Watch for potential security incidents:

- **Authentication Failures**: Repeated failed AUTH attempts (brute force)
- **Unusual Commands**: Unexpected use of administrative commands
- **Data Exfiltration**: Large number of GET/DUMP commands in short time
- **Configuration Changes**: Unexpected CONFIG SET commands
- **Command Patterns**: Unusual command sequences or frequencies

**3. Prometheus Metrics for Security**

Monitor security-relevant metrics:

```toml
[metrics]
enabled = true
bind = "127.0.0.1:9090"  # Never expose to public internet
```

**Key metrics to monitor:**
- `ferrite_auth_failures_total` - Failed authentication attempts
- `ferrite_commands_total{command="FLUSHALL"}` - Destructive commands
- `ferrite_connections_total` - Connection rate (detect scanning)
- `ferrite_command_errors_total` - Error rate (detect attacks)
- `ferrite_commands_duration_seconds` - Latency anomalies

**4. Alerting Rules**

Set up alerts for security events:

```yaml
# Prometheus alerting rules
groups:
- name: ferrite-security
  rules:
  - alert: HighAuthFailureRate
    expr: rate(ferrite_auth_failures_total[5m]) > 10
    for: 5m
    annotations:
      summary: "High authentication failure rate detected"
      description: "More than 10 auth failures/sec for 5 minutes"

  - alert: UnexpectedFlushCommand
    expr: increase(ferrite_commands_total{command="FLUSHALL"}[1h]) > 0
    annotations:
      summary: "FLUSHALL command executed"
      description: "Destructive FLUSHALL command was executed"
```

**5. Log Management**

- Centralize logs (ELK, Loki, Splunk)
- Set up log retention policies
- Secure log storage (encryption, access control)
- Regular log review and analysis
- SIEM integration for threat detection

### Configuration Hardening

**1. Disable Unnecessary Features**

```toml
[server]
bind = "127.0.0.1"
port = 6379
timeout = 300  # Close idle connections
tcp_keepalive = 300  # Keepalive for dead connection detection
max_clients = 10000  # Limit concurrent connections

[features]
lua_scripting = false  # Disable if not needed
debug_commands = false  # Disable DEBUG commands in production
```

**2. Set Resource Limits**

Prevent resource exhaustion:

```toml
[limits]
max_key_size = 536870912  # 512MB
max_value_size = 536870912  # 512MB
max_clients = 10000
max_memory = 4294967296  # 4GB
eviction_policy = "allkeys-lru"  # When max_memory is reached

[performance]
max_connections_per_second = 1000  # Rate limiting
slowlog_log_slower_than = 10000  # microseconds
slowlog_max_len = 128
```

**3. Secure Default Configuration**

```toml
# Example secure production configuration

[server]
bind = "127.0.0.1"  # Localhost only, override for remote access
port = 6379
timeout = 300
tcp_keepalive = 300
max_clients = 10000

[tls]
enabled = true
cert_file = "/etc/ferrite/tls/server.crt"
key_file = "/etc/ferrite/tls/server.key"
ca_cert_file = "/etc/ferrite/tls/ca.crt"
min_protocol_version = "1.2"

[auth]
enabled = true
require_pass = true
password_hash_iterations = 100000

[acl]
enabled = true
users_file = "/etc/ferrite/users.acl"
default_user_enabled = false

[limits]
max_key_size = 536870912
max_value_size = 536870912  # 512MB
max_clients = 10000
max_memory = 4294967296  # 4GB
max_connections_per_second = 1000

[persistence]
enabled = true
dir = "/var/lib/ferrite"
aof_enabled = true
aof_fsync = "everysec"
checkpoint_enabled = true
checkpoint_interval_seconds = 3600

[logging]
level = "info"
audit_enabled = true
audit_log = "/var/log/ferrite/audit.log"

[metrics]
enabled = true
bind = "127.0.0.1:9090"  # Metrics on localhost only

[features]
lua_scripting = false
debug_commands = false
```

## Known Security Considerations

### Commands to Restrict in Production

Consider disabling or restricting these commands via ACL:

| Command | Risk | Recommendation | Mitigation |
|---------|------|----------------|------------|
| `DEBUG` | Server manipulation, information disclosure | Disable in production | ACL: `-debug` |
| `FLUSHALL` | Complete data destruction | Restrict to admin users only | ACL: `user app -flushall` |
| `FLUSHDB` | Database data destruction | Restrict to admin users only | ACL: `user app -flushdb` |
| `CONFIG` | Configuration changes, information disclosure | Restrict to admin users only | ACL: `user app -config` |
| `SHUTDOWN` | Service disruption | Restrict to admin users only | ACL: `user app -shutdown` |
| `KEYS` | Performance impact (DoS via CPU exhaustion) | Use `SCAN` instead | ACL: `user app -keys` |
| `SAVE` | Blocking operation (DoS) | Use `BGSAVE` instead | ACL: `user app -save` |
| `EVAL/EVALSHA` | Script execution, potential sandbox escape | Review ACL policies, disable if not needed | ACL: `user app -eval -evalsha` |
| `SCRIPT` | Script management | Restrict to admin users | ACL: `user app -script` |
| `MONITOR` | Exposes all commands (privacy violation) | Restrict to admin users | ACL: `user app -monitor` |
| `CLIENT` | Connection manipulation | Restrict to admin users | ACL: `user app -client` |
| `MIGRATE` | Data exfiltration risk | Restrict carefully | ACL: `user app -migrate` |

**ACL Example for Restricted User:**
```
user webapp on >PASSWORD ~app:* +@read +@write +@string +@hash +@list +@set +@sortedset -flushall -flushdb -config -shutdown -keys -save -eval -evalsha -script -monitor -client -debug -migrate
```

### Lua Scripting Security (If Enabled)

If Lua scripting is enabled:

**Sandbox Limitations:**
- Scripts run in a sandboxed environment
- No filesystem access from scripts
- No network access from scripts
- Limited to Ferrite commands and safe Lua functions

**Best Practices:**
- Be cautious with user-provided scripts
- Review all scripts before deployment
- Use ACLs to control who can execute scripts
- Consider disabling scripting entirely if not needed
- Monitor script execution time (potential DoS)

**Known Risks:**
- CPU exhaustion via infinite loops
- Memory exhaustion via large data structures
- Potential sandbox escapes (stay updated on CVEs)

### io_uring Security (Linux)

Ferrite uses io_uring on Linux for high-performance I/O:

**Security Considerations:**
- Requires kernel 5.11+ for security-hardened io_uring
- Some environments disable io_uring (SELinux, seccomp)
- Graceful fallback to standard I/O if io_uring unavailable

**Recommendations:**
- Use kernel 5.11 or later
- Keep kernel updated for io_uring security patches
- Test io_uring functionality in your environment
- Monitor for kernel CVEs related to io_uring

### Denial of Service (DoS) Risks

**Potential DoS Vectors:**

1. **Connection Exhaustion**
   - Mitigation: `max_clients` limit
   - Mitigation: Connection rate limiting
   - Mitigation: Firewall rules

2. **Memory Exhaustion**
   - Mitigation: `max_memory` limit
   - Mitigation: Eviction policies
   - Mitigation: `max_value_size` limit

3. **CPU Exhaustion**
   - Mitigation: Disable `KEYS` command (use `SCAN`)
   - Mitigation: Disable slow commands or use ACLs
   - Mitigation: `slowlog` monitoring

4. **Disk Exhaustion**
   - Mitigation: Disk quotas
   - Mitigation: Monitoring disk usage
   - Mitigation: Log rotation

### Data Confidentiality

**Considerations:**

1. **Data in Memory**
   - Ferrite stores data in plaintext in memory
   - Memory dumps could expose sensitive data
   - Mitigation: Encrypted swap, secure system administration

2. **Data on Disk**
   - AOF and checkpoints are plaintext by default
   - Mitigation: Filesystem encryption (LUKS, etc.)

3. **Network Transmission**
   - RESP protocol is plaintext by default
   - Mitigation: Use TLS for all connections

4. **Log Files**
   - Audit logs may contain sensitive data
   - Mitigation: Secure log files, encrypt logs, careful logging configuration

## Security Updates

### How We Communicate Security Updates

Security updates and advisories are published through:

1. **GitHub Security Advisories**: Primary channel for CVE notifications
2. **Release Notes**: Security fixes highlighted in release notes
3. **Security Mailing List**: Opt-in mailing list for security announcements
4. **RSS Feed**: GitHub releases feed

### Subscribing to Security Notifications

To stay informed about security updates:

1. **Watch this repository** with "Releases only" or "All activity"
2. **Subscribe to GitHub Security Advisories** for this repository
3. **Join the security mailing list**
4. **Follow release notes** for each version

### Security Update Process

When a security issue is identified:

1. **Assessment**: We assess severity and impact
2. **Fix Development**: We develop and test a fix in private
3. **Advisory Draft**: We prepare a security advisory
4. **Coordinated Release**: We release the fix and advisory
5. **Disclosure**: We publicly disclose the issue (typically after 90 days)

**Severity Levels:**
- **Critical**: Remote code execution, authentication bypass
- **High**: Privilege escalation, data breach potential
- **Medium**: Denial of service, information disclosure
- **Low**: Minor information leaks, low-impact issues

## Bug Bounty

We do not currently have a formal bug bounty program. However:

- We greatly appreciate security researchers who responsibly disclose vulnerabilities
- We will publicly acknowledge your contribution (with your permission)
- We may provide recognition in release notes and documentation

As the project matures, we may establish a formal bug bounty program.

## Security Contacts

**Primary Contact:**
- **Email**: `josedab@gmail.com`
- **PGP Key**: Available in repository root (`SECURITY-PGP-KEY.asc`)

**GitHub Security Advisories:**
- Preferred method for private vulnerability disclosure
- [Create a security advisory](../../security/advisories/new)

## Additional Resources

**Documentation:**
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- [CLAUDE.md](CLAUDE.md) - Architecture and development guide
- [README.md](README.md) - Project overview

**Security References:**
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE Top 25](https://cwe.mitre.org/top25/)
- [Redis Security Documentation](https://redis.io/docs/management/security/)

---

Thank you for helping keep Ferrite and its users secure! If you have questions about this security policy, please open a GitHub Discussion or contact the maintainers.
