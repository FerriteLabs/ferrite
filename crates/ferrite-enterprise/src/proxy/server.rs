//! Async TCP proxy server for zero-downtime Redis-to-Ferrite migration.
//!
//! Accepts client connections and forwards RESP frames to the appropriate
//! backend (Redis, Ferrite, or both) based on the current [`ProxyMode`].

use super::{ProxyConfig, ProxyError, RedisProxy, RoutingDecision};
use bytes::BytesMut;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// Runs the proxy TCP listener, accepting connections and routing them.
///
/// This is the main entry point for the live migration proxy server.
/// It spawns a task per client connection and delegates routing to the
/// [`RedisProxy`] instance.
pub async fn run_proxy(proxy: Arc<RedisProxy>) -> Result<(), ProxyError> {
    let config = proxy.config_snapshot();
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .map_err(|e| ProxyError::BindFailed(e.to_string()))?;

    info!(
        addr = %config.listen_addr,
        mode = ?config.mode,
        "proxy listening"
    );

    loop {
        let (client_stream, client_addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                error!("accept error: {e}");
                continue;
            }
        };

        let active = proxy.metrics().active_connections.load(Ordering::Relaxed);
        if active >= config.max_connections as u64 {
            warn!(
                addr = %client_addr,
                "rejecting connection: max connections reached"
            );
            drop(client_stream);
            continue;
        }

        proxy
            .metrics()
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        let proxy = Arc::clone(&proxy);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(client_stream, &proxy).await {
                debug!(addr = %client_addr, error = %e, "connection ended");
            }
            proxy
                .metrics()
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
        });
    }
}

/// Handle a single proxied client connection.
///
/// Reads RESP frames from the client, determines the routing target,
/// forwards the raw bytes to the chosen backend, and relays the
/// response back to the client.
async fn handle_connection(mut client: TcpStream, proxy: &RedisProxy) -> Result<(), ProxyError> {
    let config = proxy.config_snapshot();
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        buf.clear();
        let n = client
            .read_buf(&mut buf)
            .await
            .map_err(|e| ProxyError::UpstreamConnectionFailed(e.to_string()))?;

        if n == 0 {
            return Ok(()); // client disconnected
        }

        proxy
            .metrics()
            .total_commands
            .fetch_add(1, Ordering::Relaxed);
        proxy
            .metrics()
            .bytes_proxied
            .fetch_add(n as u64, Ordering::Relaxed);

        // Extract the key from the RESP command for routing decisions.
        let key = extract_key_from_resp(&buf).unwrap_or_default();
        let decision = proxy.route_command(&key);

        // Auto-rollback check.
        if proxy.should_rollback() {
            warn!("auto-rollback triggered: switching to Passthrough");
            proxy.set_mode(
                super::ProxyMode::Passthrough,
                "auto-rollback: error rate exceeded threshold",
            );
        }

        let response = match decision {
            RoutingDecision::Redis => {
                proxy
                    .metrics()
                    .commands_to_redis
                    .fetch_add(1, Ordering::Relaxed);
                forward_to_backend(&config.redis_upstream, &buf, &config).await
            }
            RoutingDecision::Ferrite => {
                proxy
                    .metrics()
                    .commands_to_ferrite
                    .fetch_add(1, Ordering::Relaxed);
                // In a real deployment the Ferrite address would be configurable;
                // for now we respond with a placeholder indicating Ferrite would
                // handle it locally.
                Ok(b"+OK\r\n".to_vec())
            }
            RoutingDecision::Both => {
                proxy
                    .metrics()
                    .commands_to_redis
                    .fetch_add(1, Ordering::Relaxed);
                proxy
                    .metrics()
                    .commands_to_ferrite
                    .fetch_add(1, Ordering::Relaxed);

                let redis_resp = forward_to_backend(&config.redis_upstream, &buf, &config).await;
                let _ferrite_resp: std::result::Result<Vec<u8>, std::io::Error> =
                    Ok(b"+OK\r\n".to_vec());

                proxy
                    .metrics()
                    .shadow_comparisons
                    .fetch_add(1, Ordering::Relaxed);

                // Track divergences when both succeed.
                if let (Ok(ref r), Ok(ref f)) = (&redis_resp, &_ferrite_resp) {
                    if r != f {
                        proxy
                            .metrics()
                            .shadow_divergences
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Primary response comes from Redis in Shadow mode.
                redis_resp
            }
        };

        match response {
            Ok(data) => {
                if let Err(e) = client.write_all(&data).await {
                    return Err(ProxyError::UpstreamConnectionFailed(e.to_string()));
                }
            }
            Err(e) => {
                let err_msg = format!("-ERR proxy: {e}\r\n");
                let _ = client.write_all(err_msg.as_bytes()).await;
            }
        }
    }
}

/// Forward raw RESP bytes to a backend and read the response.
async fn forward_to_backend(
    addr: &str,
    data: &[u8],
    config: &ProxyConfig,
) -> Result<Vec<u8>, ProxyError> {
    let stream = tokio::time::timeout(config.upstream_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| ProxyError::UpstreamTimeout(config.upstream_timeout))?
        .map_err(|e| ProxyError::UpstreamConnectionFailed(e.to_string()))?;

    let (mut reader, mut writer) = stream.into_split();

    writer
        .write_all(data)
        .await
        .map_err(|e| ProxyError::UpstreamConnectionFailed(e.to_string()))?;

    let mut response = vec![0u8; 4096];
    let n = tokio::time::timeout(config.upstream_timeout, reader.read(&mut response))
        .await
        .map_err(|_| ProxyError::UpstreamTimeout(config.upstream_timeout))?
        .map_err(|e| ProxyError::UpstreamConnectionFailed(e.to_string()))?;

    response.truncate(n);
    Ok(response)
}

/// Best-effort extraction of the Redis key from an inline or RESP command.
///
/// For RESP arrays, the key is typically the second element (index 1).
/// Returns an empty string if parsing fails.
fn extract_key_from_resp(data: &[u8]) -> Option<String> {
    if data.is_empty() {
        return None;
    }

    if data[0] == b'*' {
        // RESP array: *N\r\n$Len\r\nCMD\r\n$Len\r\nKEY\r\n...
        let mut pos = 0;
        // Skip array header (*N\r\n)
        pos = skip_line(data, pos)?;
        // Skip first bulk string (command)
        pos = skip_bulk_string(data, pos)?;
        // Read second bulk string (key)
        return read_bulk_string(data, pos);
    }

    // Inline command: CMD KEY ...
    let s = std::str::from_utf8(data).ok()?;
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() >= 2 {
        Some(parts[1].to_string())
    } else {
        None
    }
}

fn skip_line(data: &[u8], start: usize) -> Option<usize> {
    for i in start..data.len().saturating_sub(1) {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            return Some(i + 2);
        }
    }
    None
}

fn skip_bulk_string(data: &[u8], start: usize) -> Option<usize> {
    if start >= data.len() || data[start] != b'$' {
        return None;
    }
    let after_dollar = skip_line(data, start)?;
    // Parse the length
    let len_str = std::str::from_utf8(&data[start + 1..after_dollar - 2]).ok()?;
    let len: usize = len_str.parse().ok()?;
    let end = after_dollar + len + 2; // data + \r\n
    if end <= data.len() {
        Some(end)
    } else {
        None
    }
}

fn read_bulk_string(data: &[u8], start: usize) -> Option<String> {
    if start >= data.len() || data[start] != b'$' {
        return None;
    }
    let after_dollar = skip_line(data, start)?;
    let len_str = std::str::from_utf8(&data[start + 1..after_dollar - 2]).ok()?;
    let len: usize = len_str.parse().ok()?;
    let end = after_dollar + len;
    if end <= data.len() {
        std::str::from_utf8(&data[after_dollar..end])
            .ok()
            .map(|s| s.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_key_from_resp_array() {
        // *2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n
        let data = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
        assert_eq!(extract_key_from_resp(data), Some("mykey".to_string()));
    }

    #[test]
    fn extract_key_from_inline() {
        let data = b"SET mykey myvalue\r\n";
        assert_eq!(extract_key_from_resp(data), Some("mykey".to_string()));
    }

    #[test]
    fn extract_key_empty_data() {
        assert_eq!(extract_key_from_resp(b""), None);
    }

    #[test]
    fn extract_key_command_only() {
        let data = b"PING\r\n";
        assert_eq!(extract_key_from_resp(data), None);
    }
}
