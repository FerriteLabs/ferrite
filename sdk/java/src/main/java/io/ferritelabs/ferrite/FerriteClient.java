package io.ferritelabs.ferrite;

import io.ferritelabs.ferrite.pool.ConnectionPool;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Main Ferrite client with connection pooling and support for all standard
 * Redis data structures plus Ferrite-specific extensions.
 *
 * <pre>{@code
 * FerriteConfig config = FerriteConfig.builder()
 *     .host("localhost")
 *     .port(6379)
 *     .build();
 * try (FerriteClient client = FerriteClient.connect(config)) {
 *     client.set("key", "value");
 *     String val = client.get("key");
 * }
 * }</pre>
 */
public class FerriteClient implements Closeable {

    private final ConnectionPool pool;
    private final FerriteConfig config;

    private FerriteClient(FerriteConfig config) {
        this.config = config;
        this.pool = new ConnectionPool(config);
    }

    /**
     * Connects to a Ferrite server with the given configuration.
     */
    public static FerriteClient connect(FerriteConfig config) throws IOException {
        FerriteClient client = new FerriteClient(config);
        // Validate connectivity
        client.ping();
        // Authenticate if needed
        if (config.getPassword() != null && !config.getPassword().isEmpty()) {
            client.execute("AUTH", config.getPassword());
        }
        // Select database
        if (config.getDatabase() != 0) {
            client.execute("SELECT", String.valueOf(config.getDatabase()));
        }
        return client;
    }

    /**
     * Connects to a Ferrite server at the given host and port with defaults.
     */
    public static FerriteClient connect(String host, int port) throws IOException {
        return connect(FerriteConfig.builder().host(host).port(port).build());
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }

    // --- Raw command execution ---

    /**
     * Executes a raw RESP command. Used internally and by extension clients
     * (VectorClient, QueryClient).
     */
    Object executeRaw(String... args) throws IOException {
        return execute(args);
    }

    private Object execute(String... args) throws IOException {
        IOException lastErr = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            if (attempt > 0) {
                try {
                    Thread.sleep(config.getRetryBackoff().toMillis() * attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", e);
                }
            }
            ConnectionPool.Connection conn = pool.get();
            try {
                conn.getEncoder().writeCommand(args);
                Object result = conn.getDecoder().readValue();
                pool.release(conn);
                return result;
            } catch (IOException e) {
                lastErr = e;
                try { conn.close(); } catch (IOException ignored) {}
            }
        }
        throw lastErr;
    }

    // --- String operations ---

    /** Gets the value of a key. Returns null if the key does not exist. */
    public String get(String key) throws IOException {
        Object val = execute("GET", key);
        return val != null ? val.toString() : null;
    }

    /** Sets a key to a string value. */
    public void set(String key, String value) throws IOException {
        execute("SET", key, value);
    }

    /** Sets a key to a string value with a TTL. */
    public void set(String key, String value, Duration ttl) throws IOException {
        execute("SET", key, value, "PX", String.valueOf(ttl.toMillis()));
    }

    /** Deletes one or more keys. Returns the number of keys removed. */
    public long del(String... keys) throws IOException {
        String[] args = new String[keys.length + 1];
        args[0] = "DEL";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return (Long) execute(args);
    }

    /** Returns the number of specified keys that exist. */
    public long exists(String... keys) throws IOException {
        String[] args = new String[keys.length + 1];
        args[0] = "EXISTS";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return (Long) execute(args);
    }

    /** Sets a timeout on a key in seconds. Returns true if the timeout was set. */
    public boolean expire(String key, Duration ttl) throws IOException {
        return (Long) execute("EXPIRE", key, String.valueOf(ttl.getSeconds())) == 1;
    }

    /** Returns the remaining TTL of a key in seconds. */
    public long ttl(String key) throws IOException {
        return (Long) execute("TTL", key);
    }

    // --- List operations ---

    /** Prepends values to a list. Returns the new list length. */
    public long lpush(String key, String... values) throws IOException {
        String[] args = new String[values.length + 2];
        args[0] = "LPUSH";
        args[1] = key;
        System.arraycopy(values, 0, args, 2, values.length);
        return (Long) execute(args);
    }

    /** Appends values to a list. Returns the new list length. */
    public long rpush(String key, String... values) throws IOException {
        String[] args = new String[values.length + 2];
        args[0] = "RPUSH";
        args[1] = key;
        System.arraycopy(values, 0, args, 2, values.length);
        return (Long) execute(args);
    }

    /** Removes and returns the first element. Returns null if list is empty. */
    public String lpop(String key) throws IOException {
        Object val = execute("LPOP", key);
        return val != null ? val.toString() : null;
    }

    /** Removes and returns the last element. Returns null if list is empty. */
    public String rpop(String key) throws IOException {
        Object val = execute("RPOP", key);
        return val != null ? val.toString() : null;
    }

    /** Returns elements in the given range. */
    @SuppressWarnings("unchecked")
    public List<String> lrange(String key, long start, long stop) throws IOException {
        Object val = execute("LRANGE", key, String.valueOf(start), String.valueOf(stop));
        return toStringList(val);
    }

    /** Returns the length of a list. */
    public long llen(String key) throws IOException {
        return (Long) execute("LLEN", key);
    }

    // --- Hash operations ---

    /** Sets field-value pairs in a hash. Returns the number of new fields added. */
    public long hset(String key, String... fieldValues) throws IOException {
        String[] args = new String[fieldValues.length + 2];
        args[0] = "HSET";
        args[1] = key;
        System.arraycopy(fieldValues, 0, args, 2, fieldValues.length);
        return (Long) execute(args);
    }

    /** Gets the value of a hash field. Returns null if field does not exist. */
    public String hget(String key, String field) throws IOException {
        Object val = execute("HGET", key, field);
        return val != null ? val.toString() : null;
    }

    /** Returns all fields and values of a hash. */
    @SuppressWarnings("unchecked")
    public Map<String, String> hgetAll(String key) throws IOException {
        Object val = execute("HGETALL", key);
        return toStringMap(val);
    }

    /** Deletes fields from a hash. Returns the number removed. */
    public long hdel(String key, String... fields) throws IOException {
        String[] args = new String[fields.length + 2];
        args[0] = "HDEL";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return (Long) execute(args);
    }

    // --- Set operations ---

    /** Adds members to a set. Returns the number of new members added. */
    public long sadd(String key, String... members) throws IOException {
        String[] args = new String[members.length + 2];
        args[0] = "SADD";
        args[1] = key;
        System.arraycopy(members, 0, args, 2, members.length);
        return (Long) execute(args);
    }

    /** Returns all members of a set. */
    @SuppressWarnings("unchecked")
    public Set<String> smembers(String key) throws IOException {
        Object val = execute("SMEMBERS", key);
        List<String> list = toStringList(val);
        return new HashSet<>(list);
    }

    /** Removes members from a set. Returns the number removed. */
    public long srem(String key, String... members) throws IOException {
        String[] args = new String[members.length + 2];
        args[0] = "SREM";
        args[1] = key;
        System.arraycopy(members, 0, args, 2, members.length);
        return (Long) execute(args);
    }

    /** Returns the number of members in a set. */
    public long scard(String key) throws IOException {
        return (Long) execute("SCARD", key);
    }

    // --- Sorted set operations ---

    /** Adds a member with a score. Returns the number of new members added. */
    public long zadd(String key, double score, String member) throws IOException {
        return (Long) execute("ZADD", key, String.valueOf(score), member);
    }

    /** Returns members in the given rank range. */
    @SuppressWarnings("unchecked")
    public List<String> zrange(String key, long start, long stop) throws IOException {
        Object val = execute("ZRANGE", key, String.valueOf(start), String.valueOf(stop));
        return toStringList(val);
    }

    /** Returns the score of a member. Returns null if not found. */
    public Double zscore(String key, String member) throws IOException {
        Object val = execute("ZSCORE", key, member);
        if (val == null) return null;
        return Double.parseDouble(val.toString());
    }

    /** Removes members from a sorted set. Returns the number removed. */
    public long zrem(String key, String... members) throws IOException {
        String[] args = new String[members.length + 2];
        args[0] = "ZREM";
        args[1] = key;
        System.arraycopy(members, 0, args, 2, members.length);
        return (Long) execute(args);
    }

    // --- Server operations ---

    /** Sends PING to verify connectivity. */
    public void ping() throws IOException {
        execute("PING");
    }

    /** Returns server information. */
    public String info() throws IOException {
        Object val = execute("INFO");
        return val != null ? val.toString() : "";
    }

    // --- Helpers ---

    @SuppressWarnings("unchecked")
    private static List<String> toStringList(Object val) {
        if (val == null) return Collections.emptyList();
        List<Object> list = (List<Object>) val;
        return list.stream()
                .map(o -> o != null ? o.toString() : null)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> toStringMap(Object val) {
        if (val == null) return Collections.emptyMap();
        List<Object> list = (List<Object>) val;
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < list.size() - 1; i += 2) {
            String k = list.get(i) != null ? list.get(i).toString() : null;
            String v = list.get(i + 1) != null ? list.get(i + 1).toString() : null;
            map.put(k, v);
        }
        return map;
    }
}
