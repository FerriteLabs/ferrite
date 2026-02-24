//! Query analysis for routing decisions

use bytes::Bytes;
use std::collections::HashSet;

/// Query analyzer for determining query patterns
pub struct QueryAnalyzer {
    /// Known read-only commands
    read_commands: HashSet<&'static str>,
    /// Known write commands
    write_commands: HashSet<&'static str>,
    /// Vector search commands
    vector_commands: HashSet<&'static str>,
    /// Range query commands
    range_commands: HashSet<&'static str>,
}

impl QueryAnalyzer {
    /// Create a new query analyzer
    pub fn new() -> Self {
        Self {
            read_commands: [
                "GET",
                "MGET",
                "HGET",
                "HMGET",
                "HGETALL",
                "LRANGE",
                "LINDEX",
                "LLEN",
                "SMEMBERS",
                "SISMEMBER",
                "SCARD",
                "ZRANGE",
                "ZRANGEBYSCORE",
                "ZRANK",
                "ZSCORE",
                "ZCARD",
                "EXISTS",
                "TYPE",
                "TTL",
                "PTTL",
                "KEYS",
                "SCAN",
                "HSCAN",
                "SSCAN",
                "ZSCAN",
                "STRLEN",
                "GETRANGE",
                "BITCOUNT",
                "BITPOS",
                "PFCOUNT",
                "XLEN",
                "XRANGE",
                "XREAD",
                "XINFO",
                "OBJECT",
            ]
            .into_iter()
            .collect(),
            write_commands: [
                "SET",
                "MSET",
                "HSET",
                "HMSET",
                "HDEL",
                "LPUSH",
                "RPUSH",
                "LPOP",
                "RPOP",
                "SADD",
                "SREM",
                "ZADD",
                "ZREM",
                "ZINCRBY",
                "DEL",
                "UNLINK",
                "EXPIRE",
                "EXPIREAT",
                "PEXPIRE",
                "PEXPIREAT",
                "PERSIST",
                "RENAME",
                "RENAMENX",
                "COPY",
                "INCR",
                "INCRBY",
                "INCRBYFLOAT",
                "DECR",
                "DECRBY",
                "APPEND",
                "SETRANGE",
                "SETEX",
                "PSETEX",
                "SETNX",
                "SETXX",
                "GETSET",
                "GETDEL",
                "PFADD",
                "PFMERGE",
                "XADD",
                "XTRIM",
                "XDEL",
                "XGROUP",
            ]
            .into_iter()
            .collect(),
            vector_commands: [
                "FT.SEARCH",
                "VECTOR.SEARCH",
                "VECTOR.KNN",
                "VECTOR.ADD",
                "VECTOR.GET",
                "VECTOR.DEL",
                "VECTOR.INFO",
                "SEMANTIC.SEARCH",
                "SEMANTIC.SIMILAR",
            ]
            .into_iter()
            .collect(),
            range_commands: [
                "ZRANGE",
                "ZRANGEBYSCORE",
                "ZRANGEBYLEX",
                "ZREVRANGE",
                "ZREVRANGEBYSCORE",
                "LRANGE",
                "XRANGE",
                "XREVRANGE",
                "SCAN",
                "HSCAN",
                "SSCAN",
                "ZSCAN",
                "KEYS",
                "TS.RANGE",
                "TS.MRANGE",
            ]
            .into_iter()
            .collect(),
        }
    }

    /// Analyze a query and return its pattern
    pub fn analyze(&self, command: &str, args: &[Bytes]) -> QueryPattern {
        let cmd_upper = command.to_uppercase();

        // Determine query type
        let query_type = self.classify_query(&cmd_upper, args);

        // Determine if read-only
        let is_read_only = self.is_read_only(&cmd_upper);

        // Estimate cost
        let estimated_cost = self.estimate_cost(&cmd_upper, args);

        // Count keys
        let key_count = self.count_keys(&cmd_upper, args);

        // Detect patterns
        let has_pattern = self.has_pattern(args);
        let key_prefix = self.extract_key_prefix(args);

        QueryPattern {
            command: cmd_upper,
            query_type,
            is_read_only,
            estimated_cost,
            key_count,
            has_pattern,
            key_prefix,
        }
    }

    fn classify_query(&self, command: &str, args: &[Bytes]) -> QueryType {
        // Vector search
        if self.vector_commands.contains(command) {
            return QueryType::VectorSearch;
        }

        // Full-text search
        if command.starts_with("FT.") && command != "FT.CREATE" && command != "FT.DROP" {
            return QueryType::FullTextSearch;
        }

        // Geo queries
        if command.starts_with("GEO") {
            return QueryType::GeoQuery;
        }

        // Time-series queries
        if command.starts_with("TS.") {
            return QueryType::TimeSeriesQuery;
        }

        // Range queries
        if self.range_commands.contains(command) {
            return QueryType::RangeScan;
        }

        // Aggregation
        if command.contains("COUNT") || command.contains("SUM") || command.contains("AVG") {
            return QueryType::Aggregation;
        }

        // Write operations
        if self.write_commands.contains(command) {
            return QueryType::Write;
        }

        // Point lookups (most common)
        if matches!(command, "GET" | "HGET" | "EXISTS" | "TYPE" | "TTL") {
            return QueryType::PointLookup;
        }

        // Multi-key operations
        if matches!(command, "MGET" | "MSET" | "DEL") && args.len() > 1 {
            return if self.is_read_only(command) {
                QueryType::PointLookup // Still considered point lookups, just multiple
            } else {
                QueryType::Write
            };
        }

        QueryType::Unknown
    }

    fn is_read_only(&self, command: &str) -> bool {
        self.read_commands.contains(command)
            || command.starts_with("FT.SEARCH")
            || command.starts_with("FT.AGGREGATE")
    }

    fn estimate_cost(&self, command: &str, args: &[Bytes]) -> u64 {
        let base_cost = match command {
            // Very cheap operations
            "GET" | "SET" | "EXISTS" | "TTL" | "TYPE" => 1,

            // Cheap operations
            "HGET" | "HSET" | "LPUSH" | "RPUSH" | "SADD" | "ZADD" => 2,

            // Medium operations
            "MGET" | "MSET" | "HMGET" | "HMSET" => 5 + args.len() as u64,

            // Expensive operations
            "HGETALL" | "SMEMBERS" | "LRANGE" | "ZRANGE" => 10,

            // Very expensive operations
            "KEYS" | "SCAN" => 100,

            // Vector operations
            cmd if cmd.starts_with("VECTOR") || cmd.starts_with("FT.SEARCH") => 50,

            // Unknown
            _ => 5,
        };

        // Adjust for argument count (more keys = more work)
        base_cost + (args.len() as u64 / 10)
    }

    fn count_keys(&self, command: &str, args: &[Bytes]) -> usize {
        match command {
            "MGET" | "DEL" | "UNLINK" | "EXISTS" => args.len(),
            "MSET" => args.len() / 2,
            _ if !args.is_empty() => 1,
            _ => 0,
        }
    }

    fn has_pattern(&self, args: &[Bytes]) -> bool {
        args.first()
            .map(|arg| {
                let s = String::from_utf8_lossy(arg);
                s.contains('*') || s.contains('?') || s.contains('[')
            })
            .unwrap_or(false)
    }

    fn extract_key_prefix(&self, args: &[Bytes]) -> Option<String> {
        args.first().and_then(|arg| {
            let s = String::from_utf8_lossy(arg);
            // Extract prefix up to first wildcard or special char
            let prefix: String = s
                .chars()
                .take_while(|c| !matches!(c, '*' | '?' | '['))
                .collect();
            if prefix.is_empty() {
                None
            } else {
                Some(prefix)
            }
        })
    }
}

impl Default for QueryAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Analyzed query pattern
#[derive(Clone, Debug)]
pub struct QueryPattern {
    /// Original command
    pub command: String,
    /// Classified query type
    pub query_type: QueryType,
    /// Whether this is a read-only query
    pub is_read_only: bool,
    /// Estimated execution cost (1-100 scale)
    pub estimated_cost: u64,
    /// Number of keys involved
    pub key_count: usize,
    /// Whether the query uses patterns (wildcards)
    pub has_pattern: bool,
    /// Extracted key prefix (if any)
    pub key_prefix: Option<String>,
}

/// Query type classification
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryType {
    /// Single key lookup (GET, HGET, etc.)
    PointLookup,
    /// Range scan (ZRANGE, SCAN, etc.)
    RangeScan,
    /// Vector similarity search
    VectorSearch,
    /// Full-text search (FT.SEARCH)
    FullTextSearch,
    /// Geo queries (GEORADIUS, etc.)
    GeoQuery,
    /// Time-series queries (TS.RANGE, etc.)
    TimeSeriesQuery,
    /// Aggregation queries
    Aggregation,
    /// Write operation
    Write,
    /// Unknown/unclassified
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_get() {
        let analyzer = QueryAnalyzer::new();
        let pattern = analyzer.analyze("GET", &[Bytes::from("key1")]);
        assert_eq!(pattern.query_type, QueryType::PointLookup);
        assert!(pattern.is_read_only);
    }

    #[test]
    fn test_classify_set() {
        let analyzer = QueryAnalyzer::new();
        let pattern = analyzer.analyze("SET", &[Bytes::from("key1"), Bytes::from("value1")]);
        assert_eq!(pattern.query_type, QueryType::Write);
        assert!(!pattern.is_read_only);
    }

    #[test]
    fn test_classify_zrange() {
        let analyzer = QueryAnalyzer::new();
        let pattern = analyzer.analyze(
            "ZRANGE",
            &[Bytes::from("zset"), Bytes::from("0"), Bytes::from("-1")],
        );
        assert_eq!(pattern.query_type, QueryType::RangeScan);
    }

    #[test]
    fn test_pattern_detection() {
        let analyzer = QueryAnalyzer::new();
        let pattern = analyzer.analyze("KEYS", &[Bytes::from("user:*")]);
        assert!(pattern.has_pattern);
        assert_eq!(pattern.key_prefix, Some("user:".to_string()));
    }

    #[test]
    fn test_cost_estimation() {
        let analyzer = QueryAnalyzer::new();

        let cheap = analyzer.analyze("GET", &[Bytes::from("key")]);
        let expensive = analyzer.analyze("KEYS", &[Bytes::from("*")]);

        assert!(cheap.estimated_cost < expensive.estimated_cost);
    }
}
