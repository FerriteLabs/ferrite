# GitHub Actions Examples for Ferrite

This directory contains example GitHub Actions workflow files for testing applications with Ferrite as a service container.

## Available Examples

| File | Language | Description |
|------|----------|-------------|
| `nodejs.yml` | Node.js | Test with npm, supports Node 18/20/22 |
| `python.yml` | Python | Test with pytest, supports Python 3.10/3.11/3.12 |
| `go.yml` | Go | Test with go test, includes linting |
| `rust.yml` | Rust | Test with cargo, includes clippy and coverage |
| `java.yml` | Java | Test with Maven and Gradle, JDK 17/21 |

## Quick Start

1. Copy the relevant workflow file to `.github/workflows/` in your repository
2. Customize the workflow for your project structure
3. Commit and push

## Using Ferrite as a Service

All examples use Ferrite as a GitHub Actions service container:

```yaml
services:
  ferrite:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6379:6379
    options: >-
      --health-cmd "ferrite-cli PING"
      --health-interval 10s
      --health-timeout 5s
      --health-retries 5
```

## Environment Variables

The workflows set these environment variables for your tests:

| Variable | Value | Description |
|----------|-------|-------------|
| `FERRITE_HOST` | `localhost` | Ferrite server hostname |
| `FERRITE_PORT` | `6379` | Ferrite server port |
| `REDIS_URL` | `redis://localhost:6379` | Full connection URL |

## Docker Hub Alternative

If you prefer Docker Hub over GitHub Container Registry:

```yaml
services:
  ferrite:
    image: ferritecache/ferrite:latest
    # ... rest of config
```

## Configuration Options

### Custom Port

```yaml
services:
  ferrite:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6380:6379  # Maps container 6379 to host 6380
    env:
      FERRITE_PORT: 6379
```

### With Password

```yaml
services:
  ferrite:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6379:6379
    env:
      FERRITE_PASSWORD: ${{ secrets.FERRITE_PASSWORD }}
```

### Multiple Databases

```yaml
services:
  ferrite:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6379:6379
    env:
      FERRITE_DATABASES: 16
```

### With Persistence

```yaml
services:
  ferrite:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6379:6379
    volumes:
      - ferrite-data:/var/lib/ferrite/data
```

## Waiting for Ferrite

The health check ensures Ferrite is ready before tests run. If you need additional waiting:

```yaml
- name: Wait for Ferrite
  run: |
    for i in {1..30}; do
      if nc -z localhost 6379; then
        echo "Ferrite is ready!"
        break
      fi
      echo "Waiting for Ferrite..."
      sleep 1
    done
```

## Matrix Testing

Test against multiple versions:

```yaml
strategy:
  matrix:
    ferrite-version: ['0.1', '0.2', 'latest']

services:
  ferrite:
    image: ghcr.io/josedab/ferrite:${{ matrix.ferrite-version }}
```

## Caching

For faster CI, cache your dependencies:

### Node.js
```yaml
- uses: actions/setup-node@v4
  with:
    cache: 'npm'
```

### Python
```yaml
- uses: actions/setup-python@v5
  with:
    cache: 'pip'
```

### Go
```yaml
- uses: actions/setup-go@v5
  with:
    cache: true
```

### Rust
```yaml
- uses: Swatinem/rust-cache@v2
```

### Java (Maven)
```yaml
- uses: actions/setup-java@v4
  with:
    cache: 'maven'
```

## Parallel Testing

Run tests in parallel with matrix strategies:

```yaml
jobs:
  test:
    strategy:
      matrix:
        shard: [1, 2, 3, 4]
    steps:
      - run: npm test -- --shard=${{ matrix.shard }}/4
```

## Integration with Other Services

### Ferrite + PostgreSQL

```yaml
services:
  ferrite:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6379:6379

  postgres:
    image: postgres:15
    ports:
      - 5432:5432
    env:
      POSTGRES_PASSWORD: postgres
```

### Ferrite Cluster (3 nodes)

```yaml
services:
  ferrite-1:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6379:6379
    env:
      FERRITE_CLUSTER_ENABLED: "true"
      FERRITE_CLUSTER_NODE_ID: node1

  ferrite-2:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6380:6379
    env:
      FERRITE_CLUSTER_ENABLED: "true"
      FERRITE_CLUSTER_NODE_ID: node2

  ferrite-3:
    image: ghcr.io/josedab/ferrite:latest
    ports:
      - 6381:6379
    env:
      FERRITE_CLUSTER_ENABLED: "true"
      FERRITE_CLUSTER_NODE_ID: node3
```

## Troubleshooting

### Service not starting

Check the service logs:

```yaml
- name: Debug Ferrite
  if: failure()
  run: docker logs "${{ job.services.ferrite.id }}"
```

### Connection refused

Ensure you're using `localhost` not `ferrite` as the hostname (services run on the host network in GitHub Actions).

### Timeout issues

Increase health check timeout:

```yaml
options: >-
  --health-cmd "ferrite-cli PING"
  --health-interval 10s
  --health-timeout 10s
  --health-retries 10
  --health-start-period 30s
```

## More Information

- [Ferrite Documentation](https://ferrite.dev/docs)
- [GitHub Actions Service Containers](https://docs.github.com/en/actions/using-containerized-services)
- [Ferrite Docker Hub](https://hub.docker.com/r/ferritecache/ferrite)
