#![forbid(unsafe_code)]
//! Interactive query builder — command suggestions and help.

use serde::{Deserialize, Serialize};

/// Provides query suggestions and command help.
#[derive(Debug, Clone, Default)]
pub struct QueryBuilder;

/// A suggested query with context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySuggestion {
    pub query: String,
    pub description: String,
    pub category: String,
}

/// Help information for a single command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandHelp {
    pub name: String,
    pub syntax: String,
    pub description: String,
    pub examples: Vec<String>,
    pub complexity: String,
    pub since_version: String,
}

/// Built-in command help database.
fn command_database() -> Vec<CommandHelp> {
    vec![
        CommandHelp {
            name: "GET".to_string(),
            syntax: "GET key".to_string(),
            description: "Get the value of a key. Returns nil if the key does not exist.".to_string(),
            examples: vec!["GET mykey".to_string()],
            complexity: "O(1)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "SET".to_string(),
            syntax: "SET key value [EX seconds] [PX milliseconds] [NX|XX] [GET]".to_string(),
            description: "Set the string value of a key with optional expiration and conditions.".to_string(),
            examples: vec![
                "SET mykey myvalue".to_string(),
                "SET mykey myvalue EX 60".to_string(),
                "SET mykey myvalue NX".to_string(),
            ],
            complexity: "O(1)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "DEL".to_string(),
            syntax: "DEL key [key ...]".to_string(),
            description: "Delete one or more keys.".to_string(),
            examples: vec!["DEL key1 key2 key3".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "HSET".to_string(),
            syntax: "HSET key field value [field value ...]".to_string(),
            description: "Set one or more field-value pairs in a hash.".to_string(),
            examples: vec![
                "HSET user:1 name Alice age 30".to_string(),
                "HSET config timeout 30".to_string(),
            ],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "HGET".to_string(),
            syntax: "HGET key field".to_string(),
            description: "Get the value of a hash field.".to_string(),
            examples: vec!["HGET user:1 name".to_string()],
            complexity: "O(1)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "HGETALL".to_string(),
            syntax: "HGETALL key".to_string(),
            description: "Get all fields and values of a hash.".to_string(),
            examples: vec!["HGETALL user:1".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "LPUSH".to_string(),
            syntax: "LPUSH key element [element ...]".to_string(),
            description: "Push one or more elements to the head of a list.".to_string(),
            examples: vec!["LPUSH queue task1 task2".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "RPUSH".to_string(),
            syntax: "RPUSH key element [element ...]".to_string(),
            description: "Push one or more elements to the tail of a list.".to_string(),
            examples: vec!["RPUSH queue task1 task2".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "SADD".to_string(),
            syntax: "SADD key member [member ...]".to_string(),
            description: "Add one or more members to a set.".to_string(),
            examples: vec!["SADD tags rust database nosql".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "ZADD".to_string(),
            syntax: "ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]".to_string(),
            description: "Add members to a sorted set with scores, or update scores of existing members.".to_string(),
            examples: vec![
                "ZADD leaderboard 100 alice 200 bob".to_string(),
                "ZADD leaderboard GT 150 alice".to_string(),
            ],
            complexity: "O(log(N))".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "EXPIRE".to_string(),
            syntax: "EXPIRE key seconds".to_string(),
            description: "Set a timeout on a key in seconds.".to_string(),
            examples: vec!["EXPIRE session:abc 3600".to_string()],
            complexity: "O(1)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "TTL".to_string(),
            syntax: "TTL key".to_string(),
            description: "Get the remaining time to live of a key in seconds.".to_string(),
            examples: vec!["TTL session:abc".to_string()],
            complexity: "O(1)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "SCAN".to_string(),
            syntax: "SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]".to_string(),
            description: "Incrementally iterate over the key space.".to_string(),
            examples: vec![
                "SCAN 0 MATCH user:* COUNT 100".to_string(),
                "SCAN 0 TYPE string".to_string(),
            ],
            complexity: "O(1) per call, O(N) total".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "VECTOR.SEARCH".to_string(),
            syntax: "VECTOR.SEARCH index query_vector KNN k [FILTER expr]".to_string(),
            description: "Perform vector similarity search on an index.".to_string(),
            examples: vec![
                "VECTOR.SEARCH idx <vector> KNN 10".to_string(),
            ],
            complexity: "O(N·log(k))".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "FT.CREATE".to_string(),
            syntax: "FT.CREATE index [ON HASH|JSON] [PREFIX count prefix ...] SCHEMA field type ...".to_string(),
            description: "Create a search index for full-text and vector search.".to_string(),
            examples: vec![
                "FT.CREATE idx ON HASH SCHEMA title TEXT content TEXT".to_string(),
            ],
            complexity: "O(1)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "FT.SEARCH".to_string(),
            syntax: "FT.SEARCH index query [LIMIT offset num]".to_string(),
            description: "Search the index with a query expression.".to_string(),
            examples: vec![
                "FT.SEARCH idx \"hello world\"".to_string(),
                "FT.SEARCH idx \"@title:ferrite\" LIMIT 0 10".to_string(),
            ],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "TS.ADD".to_string(),
            syntax: "TS.ADD key timestamp value [RETENTION ms] [LABELS label value ...]".to_string(),
            description: "Append a sample to a time series.".to_string(),
            examples: vec![
                "TS.ADD sensor:temp * 22.5".to_string(),
                "TS.ADD sensor:temp 1700000000 22.5 LABELS location office".to_string(),
            ],
            complexity: "O(log(N))".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "TS.RANGE".to_string(),
            syntax: "TS.RANGE key fromTimestamp toTimestamp [AGGREGATION type timeBucket]".to_string(),
            description: "Query a range of samples from a time series.".to_string(),
            examples: vec![
                "TS.RANGE sensor:temp - + AGGREGATION avg 3600000".to_string(),
            ],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "SUBSCRIBE".to_string(),
            syntax: "SUBSCRIBE channel [channel ...]".to_string(),
            description: "Subscribe to one or more channels.".to_string(),
            examples: vec!["SUBSCRIBE events notifications".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "PUBLISH".to_string(),
            syntax: "PUBLISH channel message".to_string(),
            description: "Post a message to a channel.".to_string(),
            examples: vec!["PUBLISH events \"user:42:login\"".to_string()],
            complexity: "O(N+M)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "MGET".to_string(),
            syntax: "MGET key [key ...]".to_string(),
            description: "Get the values of all given keys.".to_string(),
            examples: vec!["MGET key1 key2 key3".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "MSET".to_string(),
            syntax: "MSET key value [key value ...]".to_string(),
            description: "Set multiple key-value pairs atomically.".to_string(),
            examples: vec!["MSET key1 val1 key2 val2".to_string()],
            complexity: "O(N)".to_string(),
            since_version: "1.0.0".to_string(),
        },
        CommandHelp {
            name: "INCR".to_string(),
            syntax: "INCR key".to_string(),
            description: "Increment the integer value of a key by one.".to_string(),
            examples: vec!["INCR counter".to_string()],
            complexity: "O(1)".to_string(),
            since_version: "1.0.0".to_string(),
        },
    ]
}

impl QueryBuilder {
    /// Suggest queries based on a context string (key patterns, data types, etc.).
    #[allow(dead_code)]
    pub fn suggest_queries(context: &str) -> Vec<QuerySuggestion> {
        let ctx = context.to_lowercase();
        let mut suggestions = Vec::new();

        if ctx.contains("user") || ctx.contains("session") || ctx.contains("hash") {
            suggestions.push(QuerySuggestion {
                query: "HGETALL user:1".to_string(),
                description: "Retrieve all fields of a user hash".to_string(),
                category: "hash".to_string(),
            });
            suggestions.push(QuerySuggestion {
                query: "SCAN 0 MATCH user:* COUNT 100".to_string(),
                description: "Scan for all user keys".to_string(),
                category: "scan".to_string(),
            });
        }

        if ctx.contains("score") || ctx.contains("rank") || ctx.contains("leaderboard") || ctx.contains("sorted") {
            suggestions.push(QuerySuggestion {
                query: "ZREVRANGE leaderboard 0 9 WITHSCORES".to_string(),
                description: "Get top 10 from leaderboard".to_string(),
                category: "sorted_set".to_string(),
            });
            suggestions.push(QuerySuggestion {
                query: "ZRANGEBYSCORE leaderboard 100 +inf WITHSCORES".to_string(),
                description: "Get entries with score >= 100".to_string(),
                category: "sorted_set".to_string(),
            });
        }

        if ctx.contains("vector") || ctx.contains("search") || ctx.contains("semantic") {
            suggestions.push(QuerySuggestion {
                query: "VECTOR.SEARCH idx <query_vector> KNN 10".to_string(),
                description: "Find 10 nearest vectors".to_string(),
                category: "vector".to_string(),
            });
            suggestions.push(QuerySuggestion {
                query: "FT.SEARCH idx \"@content:query\"".to_string(),
                description: "Full-text search on content field".to_string(),
                category: "search".to_string(),
            });
        }

        if ctx.contains("time") || ctx.contains("series") || ctx.contains("sensor") || ctx.contains("iot") {
            suggestions.push(QuerySuggestion {
                query: "TS.RANGE sensor:temp - + AGGREGATION avg 3600000".to_string(),
                description: "Hourly average of sensor readings".to_string(),
                category: "timeseries".to_string(),
            });
        }

        if ctx.contains("cache") || ctx.contains("ttl") || ctx.contains("expire") {
            suggestions.push(QuerySuggestion {
                query: "SET cache:key value EX 300".to_string(),
                description: "Set a cache key with 5-minute TTL".to_string(),
                category: "caching".to_string(),
            });
            suggestions.push(QuerySuggestion {
                query: "TTL cache:key".to_string(),
                description: "Check remaining cache TTL".to_string(),
                category: "caching".to_string(),
            });
        }

        if ctx.contains("queue") || ctx.contains("list") || ctx.contains("job") {
            suggestions.push(QuerySuggestion {
                query: "LPUSH queue:jobs '{\"task\":\"process\"}'".to_string(),
                description: "Push a job to the queue".to_string(),
                category: "list".to_string(),
            });
            suggestions.push(QuerySuggestion {
                query: "RPOP queue:jobs".to_string(),
                description: "Pop a job from the queue".to_string(),
                category: "list".to_string(),
            });
        }

        // Fallback: general suggestions
        if suggestions.is_empty() {
            suggestions.push(QuerySuggestion {
                query: "SCAN 0 COUNT 100".to_string(),
                description: "Scan keys in the database".to_string(),
                category: "general".to_string(),
            });
            suggestions.push(QuerySuggestion {
                query: "INFO".to_string(),
                description: "Get server information".to_string(),
                category: "general".to_string(),
            });
            suggestions.push(QuerySuggestion {
                query: "DBSIZE".to_string(),
                description: "Get the number of keys".to_string(),
                category: "general".to_string(),
            });
        }

        suggestions
    }

    /// Return help text for a command.
    #[allow(dead_code)]
    pub fn explain_command(command: &str) -> Option<CommandHelp> {
        let upper = command.to_uppercase();
        command_database().into_iter().find(|h| h.name == upper)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_queries_user_context() {
        let suggestions = QueryBuilder::suggest_queries("user session");
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.query.contains("user")));
    }

    #[test]
    fn test_suggest_queries_leaderboard_context() {
        let suggestions = QueryBuilder::suggest_queries("leaderboard score");
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.category == "sorted_set"));
    }

    #[test]
    fn test_suggest_queries_vector_context() {
        let suggestions = QueryBuilder::suggest_queries("vector search");
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.category == "vector"));
    }

    #[test]
    fn test_suggest_queries_empty_context() {
        let suggestions = QueryBuilder::suggest_queries("");
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.category == "general"));
    }

    #[test]
    fn test_explain_command_get() {
        let help = QueryBuilder::explain_command("GET").unwrap();
        assert_eq!(help.name, "GET");
        assert_eq!(help.complexity, "O(1)");
        assert!(!help.examples.is_empty());
    }

    #[test]
    fn test_explain_command_case_insensitive() {
        let help = QueryBuilder::explain_command("set").unwrap();
        assert_eq!(help.name, "SET");
    }

    #[test]
    fn test_explain_command_vector_search() {
        let help = QueryBuilder::explain_command("VECTOR.SEARCH").unwrap();
        assert_eq!(help.name, "VECTOR.SEARCH");
    }

    #[test]
    fn test_explain_command_unknown() {
        assert!(QueryBuilder::explain_command("NONEXISTENT").is_none());
    }

    #[test]
    fn test_command_database_size() {
        let db = command_database();
        assert!(db.len() >= 20, "Expected at least 20 commands, got {}", db.len());
    }

    #[test]
    fn test_suggest_queries_timeseries() {
        let suggestions = QueryBuilder::suggest_queries("time series sensor");
        assert!(suggestions.iter().any(|s| s.category == "timeseries"));
    }
}
