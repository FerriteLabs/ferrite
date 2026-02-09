//! FerriteQL Built-in Functions
//!
//! Implements aggregate functions, scalar functions, and window functions.

use crate::query::{QueryError, Value};

/// Function registry
pub struct FunctionRegistry {
    aggregates: Vec<AggregateInfo>,
    scalars: Vec<ScalarInfo>,
}

impl FunctionRegistry {
    /// Create a new function registry with built-in functions
    pub fn new() -> Self {
        let mut registry = Self {
            aggregates: Vec::new(),
            scalars: Vec::new(),
        };

        // Register aggregate functions
        registry.register_aggregate(AggregateInfo {
            name: "COUNT".to_string(),
            factory: Box::new(|| Box::new(CountAggregate::new())),
        });
        registry.register_aggregate(AggregateInfo {
            name: "SUM".to_string(),
            factory: Box::new(|| Box::new(SumAggregate::new())),
        });
        registry.register_aggregate(AggregateInfo {
            name: "AVG".to_string(),
            factory: Box::new(|| Box::new(AvgAggregate::new())),
        });
        registry.register_aggregate(AggregateInfo {
            name: "MIN".to_string(),
            factory: Box::new(|| Box::new(MinAggregate::new())),
        });
        registry.register_aggregate(AggregateInfo {
            name: "MAX".to_string(),
            factory: Box::new(|| Box::new(MaxAggregate::new())),
        });

        // Register scalar functions
        registry.register_scalar(ScalarInfo {
            name: "UPPER".to_string(),
            func: Box::new(|args| {
                if args.is_empty() {
                    return Err(QueryError::InvalidParams(
                        "UPPER requires 1 argument".to_string(),
                    ));
                }
                match &args[0] {
                    Value::String(s) => Ok(Value::String(s.to_uppercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(QueryError::TypeError(
                        "UPPER requires a string argument".to_string(),
                    )),
                }
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "LOWER".to_string(),
            func: Box::new(|args| {
                if args.is_empty() {
                    return Err(QueryError::InvalidParams(
                        "LOWER requires 1 argument".to_string(),
                    ));
                }
                match &args[0] {
                    Value::String(s) => Ok(Value::String(s.to_lowercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(QueryError::TypeError(
                        "LOWER requires a string argument".to_string(),
                    )),
                }
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "LENGTH".to_string(),
            func: Box::new(|args| {
                if args.is_empty() {
                    return Err(QueryError::InvalidParams(
                        "LENGTH requires 1 argument".to_string(),
                    ));
                }
                match &args[0] {
                    Value::String(s) => Ok(Value::Int(s.len() as i64)),
                    Value::Bytes(b) => Ok(Value::Int(b.len() as i64)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(QueryError::TypeError(
                        "LENGTH requires a string or bytes argument".to_string(),
                    )),
                }
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "CONCAT".to_string(),
            func: Box::new(|args| {
                let mut result = String::new();
                for arg in args {
                    match arg {
                        Value::String(s) => result.push_str(s),
                        Value::Int(n) => result.push_str(&n.to_string()),
                        Value::Float(f) => result.push_str(&f.to_string()),
                        Value::Bool(b) => result.push_str(&b.to_string()),
                        Value::Null => {}
                        _ => {
                            return Err(QueryError::TypeError(
                                "CONCAT requires scalar arguments".to_string(),
                            ))
                        }
                    }
                }
                Ok(Value::String(result))
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "COALESCE".to_string(),
            func: Box::new(|args| {
                for arg in args {
                    if !matches!(arg, Value::Null) {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::Null)
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "NULLIF".to_string(),
            func: Box::new(|args| {
                if args.len() != 2 {
                    return Err(QueryError::InvalidParams(
                        "NULLIF requires 2 arguments".to_string(),
                    ));
                }
                if args[0] == args[1] {
                    Ok(Value::Null)
                } else {
                    Ok(args[0].clone())
                }
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "ABS".to_string(),
            func: Box::new(|args| {
                if args.is_empty() {
                    return Err(QueryError::InvalidParams(
                        "ABS requires 1 argument".to_string(),
                    ));
                }
                match &args[0] {
                    Value::Int(n) => Ok(Value::Int(n.abs())),
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(QueryError::TypeError(
                        "ABS requires a numeric argument".to_string(),
                    )),
                }
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "ROUND".to_string(),
            func: Box::new(|args| {
                if args.is_empty() {
                    return Err(QueryError::InvalidParams(
                        "ROUND requires 1-2 arguments".to_string(),
                    ));
                }
                let precision = if args.len() > 1 {
                    match &args[1] {
                        Value::Int(n) => *n as i32,
                        _ => 0,
                    }
                } else {
                    0
                };
                match &args[0] {
                    Value::Float(f) => {
                        let multiplier = 10f64.powi(precision);
                        Ok(Value::Float((f * multiplier).round() / multiplier))
                    }
                    Value::Int(n) => Ok(Value::Int(*n)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(QueryError::TypeError(
                        "ROUND requires a numeric argument".to_string(),
                    )),
                }
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "SUBSTRING".to_string(),
            func: Box::new(|args| {
                if args.len() < 2 {
                    return Err(QueryError::InvalidParams(
                        "SUBSTRING requires 2-3 arguments".to_string(),
                    ));
                }
                let s = match &args[0] {
                    Value::String(s) => s,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(QueryError::TypeError(
                            "SUBSTRING requires a string".to_string(),
                        ))
                    }
                };
                let start = match &args[1] {
                    Value::Int(n) => (*n as usize).saturating_sub(1), // 1-indexed
                    _ => {
                        return Err(QueryError::TypeError(
                            "SUBSTRING start must be an integer".to_string(),
                        ))
                    }
                };
                let len = if args.len() > 2 {
                    match &args[2] {
                        Value::Int(n) => Some(*n as usize),
                        _ => {
                            return Err(QueryError::TypeError(
                                "SUBSTRING length must be an integer".to_string(),
                            ))
                        }
                    }
                } else {
                    None
                };
                let result: String = if let Some(len) = len {
                    s.chars().skip(start).take(len).collect()
                } else {
                    s.chars().skip(start).collect()
                };
                Ok(Value::String(result))
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "TRIM".to_string(),
            func: Box::new(|args| {
                if args.is_empty() {
                    return Err(QueryError::InvalidParams(
                        "TRIM requires 1 argument".to_string(),
                    ));
                }
                match &args[0] {
                    Value::String(s) => Ok(Value::String(s.trim().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(QueryError::TypeError("TRIM requires a string".to_string())),
                }
            }),
        });
        registry.register_scalar(ScalarInfo {
            name: "NOW".to_string(),
            func: Box::new(|_| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                Ok(Value::Int(now))
            }),
        });

        registry
    }

    /// Register an aggregate function
    pub fn register_aggregate(&mut self, info: AggregateInfo) {
        self.aggregates.push(info);
    }

    /// Register a scalar function
    pub fn register_scalar(&mut self, info: ScalarInfo) {
        self.scalars.push(info);
    }

    /// Get an aggregate function by name
    pub fn get_aggregate(&self, name: &str) -> Option<Box<dyn Aggregate>> {
        let name_upper = name.to_uppercase();
        self.aggregates
            .iter()
            .find(|a| a.name == name_upper)
            .map(|a| (a.factory)())
    }

    /// Get a scalar function by name
    pub fn get_scalar(&self, name: &str) -> Option<&ScalarInfo> {
        let name_upper = name.to_uppercase();
        self.scalars.iter().find(|s| s.name == name_upper)
    }

    /// Check if a function is an aggregate
    pub fn is_aggregate(&self, name: &str) -> bool {
        let name_upper = name.to_uppercase();
        self.aggregates.iter().any(|a| a.name == name_upper)
    }

    /// Call a scalar function
    pub fn call_scalar(&self, name: &str, args: &[Value]) -> Result<Value, QueryError> {
        let info = self
            .get_scalar(name)
            .ok_or_else(|| QueryError::UnknownColumn(format!("Unknown function: {}", name)))?;
        (info.func)(args)
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate function info
pub struct AggregateInfo {
    /// Function name
    pub name: String,
    /// Factory to create new aggregate state
    pub factory: Box<dyn Fn() -> Box<dyn Aggregate> + Send + Sync>,
}

/// Scalar function info
pub struct ScalarInfo {
    /// Function name
    pub name: String,
    /// Function implementation
    #[allow(clippy::type_complexity)]
    pub func: Box<dyn Fn(&[Value]) -> Result<Value, QueryError> + Send + Sync>,
}

/// Aggregate function trait
pub trait Aggregate: Send {
    /// Update the aggregate with a new value
    fn update(&mut self, value: &Value);

    /// Get the final result
    fn finalize(&self) -> Value;

    /// Reset the aggregate state
    fn reset(&mut self);
}

/// COUNT aggregate
pub struct CountAggregate {
    count: i64,
}

impl CountAggregate {
    /// Creates a new COUNT aggregate initialized to zero.
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for CountAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregate for CountAggregate {
    fn update(&mut self, value: &Value) {
        if !matches!(value, Value::Null) {
            self.count += 1;
        }
    }

    fn finalize(&self) -> Value {
        Value::Int(self.count)
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

/// SUM aggregate
pub struct SumAggregate {
    sum: f64,
    has_value: bool,
}

impl SumAggregate {
    /// Creates a new SUM aggregate initialized to zero.
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            has_value: false,
        }
    }
}

impl Default for SumAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregate for SumAggregate {
    fn update(&mut self, value: &Value) {
        if let Some(n) = value.as_float() {
            self.sum += n;
            self.has_value = true;
        }
    }

    fn finalize(&self) -> Value {
        if self.has_value {
            Value::Float(self.sum)
        } else {
            Value::Null
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.has_value = false;
    }
}

/// AVG aggregate
pub struct AvgAggregate {
    sum: f64,
    count: i64,
}

impl AvgAggregate {
    /// Creates a new AVG aggregate initialized to zero.
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Default for AvgAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregate for AvgAggregate {
    fn update(&mut self, value: &Value) {
        if let Some(n) = value.as_float() {
            self.sum += n;
            self.count += 1;
        }
    }

    fn finalize(&self) -> Value {
        if self.count > 0 {
            Value::Float(self.sum / self.count as f64)
        } else {
            Value::Null
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
}

/// MIN aggregate
pub struct MinAggregate {
    min: Option<Value>,
}

impl MinAggregate {
    /// Creates a new MIN aggregate with no initial value.
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Default for MinAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregate for MinAggregate {
    fn update(&mut self, value: &Value) {
        if matches!(value, Value::Null) {
            return;
        }
        match &self.min {
            None => self.min = Some(value.clone()),
            Some(current) => {
                if value < current {
                    self.min = Some(value.clone());
                }
            }
        }
    }

    fn finalize(&self) -> Value {
        self.min.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.min = None;
    }
}

/// MAX aggregate
pub struct MaxAggregate {
    max: Option<Value>,
}

impl MaxAggregate {
    /// Creates a new MAX aggregate with no initial value.
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Default for MaxAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregate for MaxAggregate {
    fn update(&mut self, value: &Value) {
        if matches!(value, Value::Null) {
            return;
        }
        match &self.max {
            None => self.max = Some(value.clone()),
            Some(current) => {
                if value > current {
                    self.max = Some(value.clone());
                }
            }
        }
    }

    fn finalize(&self) -> Value {
        self.max.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.max = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_aggregate() {
        let mut count = CountAggregate::new();
        count.update(&Value::Int(1));
        count.update(&Value::Int(2));
        count.update(&Value::Null);
        count.update(&Value::Int(3));

        assert_eq!(count.finalize(), Value::Int(3));
    }

    #[test]
    fn test_sum_aggregate() {
        let mut sum = SumAggregate::new();
        sum.update(&Value::Int(10));
        sum.update(&Value::Int(20));
        sum.update(&Value::Float(5.5));

        assert_eq!(sum.finalize(), Value::Float(35.5));
    }

    #[test]
    fn test_avg_aggregate() {
        let mut avg = AvgAggregate::new();
        avg.update(&Value::Int(10));
        avg.update(&Value::Int(20));
        avg.update(&Value::Int(30));

        assert_eq!(avg.finalize(), Value::Float(20.0));
    }

    #[test]
    fn test_min_max_aggregate() {
        let mut min = MinAggregate::new();
        let mut max = MaxAggregate::new();

        for v in [5, 2, 8, 1, 9] {
            min.update(&Value::Int(v));
            max.update(&Value::Int(v));
        }

        assert_eq!(min.finalize(), Value::Int(1));
        assert_eq!(max.finalize(), Value::Int(9));
    }

    #[test]
    fn test_scalar_functions() {
        let registry = FunctionRegistry::new();

        assert_eq!(
            registry
                .call_scalar("UPPER", &[Value::String("hello".to_string())])
                .unwrap(),
            Value::String("HELLO".to_string())
        );

        assert_eq!(
            registry
                .call_scalar("LOWER", &[Value::String("HELLO".to_string())])
                .unwrap(),
            Value::String("hello".to_string())
        );

        assert_eq!(
            registry
                .call_scalar("LENGTH", &[Value::String("hello".to_string())])
                .unwrap(),
            Value::Int(5)
        );

        assert_eq!(
            registry.call_scalar("ABS", &[Value::Int(-5)]).unwrap(),
            Value::Int(5)
        );

        assert_eq!(
            registry
                .call_scalar("COALESCE", &[Value::Null, Value::Int(5)])
                .unwrap(),
            Value::Int(5)
        );
    }

    #[test]
    fn test_concat() {
        let registry = FunctionRegistry::new();

        assert_eq!(
            registry
                .call_scalar(
                    "CONCAT",
                    &[
                        Value::String("Hello".to_string()),
                        Value::String(" ".to_string()),
                        Value::String("World".to_string()),
                    ]
                )
                .unwrap(),
            Value::String("Hello World".to_string())
        );
    }

    #[test]
    fn test_substring() {
        let registry = FunctionRegistry::new();

        assert_eq!(
            registry
                .call_scalar(
                    "SUBSTRING",
                    &[
                        Value::String("Hello World".to_string()),
                        Value::Int(1),
                        Value::Int(5),
                    ]
                )
                .unwrap(),
            Value::String("Hello".to_string())
        );
    }
}
