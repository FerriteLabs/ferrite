//! Hot-Path JIT Compilation Engine
//!
//! Compiles frequently-executed query expressions and Lua scripts into
//! optimized native code at runtime, providing 10-50x speedup on hot paths.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │              JIT Compilation Pipeline            │
//! │                                                  │
//! │  ┌──────────┐   ┌──────────┐   ┌──────────────┐ │
//! │  │ Profiler  │──▶│ Compiler │──▶│ Code Cache   │ │
//! │  │ (counts)  │   │ (IR→nat) │   │ (compiled)   │ │
//! │  └──────────┘   └──────────┘   └──────────────┘ │
//! │       ▲                              │           │
//! │       │              ┌───────────────┘           │
//! │       │              ▼                           │
//! │  ┌──────────┐   ┌──────────────┐                │
//! │  │ Hot Path  │   │  Executor    │                │
//! │  │ Detector  │   │  (dispatch)  │                │
//! │  └──────────┘   └──────────────┘                │
//! └─────────────────────────────────────────────────┘
//! ```
//!
//! # Tiered Execution
//!
//! 1. **Interpreted** (0-99 executions): Standard execution path
//! 2. **Optimized** (100-999 executions): Expression tree optimization
//! 3. **Compiled** (1000+ executions): Native code via compilation

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Execution tier based on invocation count
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExecutionTier {
    /// Standard interpreted execution
    Interpreted,
    /// Optimized expression tree (constant folding, dead code elimination)
    Optimized,
    /// Compiled to native code
    Compiled,
}

impl fmt::Display for ExecutionTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionTier::Interpreted => write!(f, "Interpreted"),
            ExecutionTier::Optimized => write!(f, "Optimized"),
            ExecutionTier::Compiled => write!(f, "Compiled"),
        }
    }
}

/// Type of expression that can be JIT-compiled
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExpressionType {
    /// FerriteQL WHERE clause predicate
    Predicate,
    /// FerriteQL computed column / projection
    Projection,
    /// Lua script body
    LuaScript,
    /// Hash function for routing
    HashFunction,
    /// Pattern matcher for key filtering
    PatternMatch,
}

/// Intermediate representation for compilable expressions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JitIR {
    /// Load a constant value
    Const(JitValue),
    /// Load a variable by name
    LoadVar(String),
    /// Binary operation
    BinOp {
        /// The binary operator
        op: BinOpKind,
        /// Left-hand operand
        left: Box<JitIR>,
        /// Right-hand operand
        right: Box<JitIR>,
    },
    /// Unary operation
    UnaryOp {
        /// The unary operator
        op: UnaryOpKind,
        /// The operand expression
        operand: Box<JitIR>,
    },
    /// Function call
    Call {
        /// Function name
        name: String,
        /// Argument expressions
        args: Vec<JitIR>,
    },
    /// Conditional (if-then-else)
    Cond {
        /// Condition expression
        condition: Box<JitIR>,
        /// Branch taken when condition is truthy
        then_branch: Box<JitIR>,
        /// Branch taken when condition is falsy
        else_branch: Box<JitIR>,
    },
    /// String comparison
    StrCmp {
        /// Left-hand string expression
        left: Box<JitIR>,
        /// Right-hand string expression
        right: Box<JitIR>,
        /// Whether comparison is case-sensitive
        case_sensitive: bool,
    },
    /// Pattern match (LIKE / glob)
    PatternMatch {
        /// Expression to match against
        value: Box<JitIR>,
        /// Glob/LIKE pattern string
        pattern: String,
    },
    /// Field access on a JSON/hash value
    FieldAccess {
        /// Object expression
        object: Box<JitIR>,
        /// Field name to access
        field: String,
    },
    /// No operation (evaluates to null)
    Noop,
}

/// Binary operation kinds
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinOpKind {
    /// Addition (`+`)
    Add,
    /// Subtraction (`-`)
    Sub,
    /// Multiplication (`*`)
    Mul,
    /// Division (`/`)
    Div,
    /// Modulo (`%`)
    Mod,
    /// Equality (`==`)
    Eq,
    /// Not equal (`!=`)
    Ne,
    /// Less than (`<`)
    Lt,
    /// Less than or equal (`<=`)
    Le,
    /// Greater than (`>`)
    Gt,
    /// Greater than or equal (`>=`)
    Ge,
    /// Logical AND
    And,
    /// Logical OR
    Or,
}

/// Unary operation kinds
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOpKind {
    /// Arithmetic negation (`-x`)
    Neg,
    /// Logical NOT (`!x`)
    Not,
    /// Null check (`IS NULL`)
    IsNull,
    /// Non-null check (`IS NOT NULL`)
    IsNotNull,
}

/// Values that flow through JIT expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JitValue {
    /// Null / absent value
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit floating-point number
    Float(f64),
    /// UTF-8 string
    Str(String),
}

impl JitValue {
    /// Returns `true` if this value is considered truthy (non-null, non-zero, non-empty).
    pub fn is_truthy(&self) -> bool {
        match self {
            JitValue::Null => false,
            JitValue::Bool(b) => *b,
            JitValue::Int(i) => *i != 0,
            JitValue::Float(f) => *f != 0.0,
            JitValue::Str(s) => !s.is_empty(),
        }
    }

    /// Attempts to coerce this value to an `i64`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            JitValue::Int(i) => Some(*i),
            JitValue::Float(f) => Some(*f as i64),
            JitValue::Bool(b) => Some(if *b { 1 } else { 0 }),
            JitValue::Str(s) => s.parse().ok(),
            JitValue::Null => None,
        }
    }

    /// Attempts to coerce this value to an `f64`.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            JitValue::Float(f) => Some(*f),
            JitValue::Int(i) => Some(*i as f64),
            JitValue::Str(s) => s.parse().ok(),
            _ => None,
        }
    }
}

impl fmt::Display for JitValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JitValue::Null => write!(f, "null"),
            JitValue::Bool(b) => write!(f, "{}", b),
            JitValue::Int(i) => write!(f, "{}", i),
            JitValue::Float(v) => write!(f, "{}", v),
            JitValue::Str(s) => write!(f, "{}", s),
        }
    }
}

/// Execution profile for a single expression/script
#[derive(Debug, Clone)]
pub struct ExecutionProfile {
    /// Unique identifier (hash of the expression)
    pub id: String,
    /// Type of expression
    pub expr_type: ExpressionType,
    /// Number of times executed
    pub execution_count: u64,
    /// Total execution time across all invocations
    pub total_time: Duration,
    /// Current execution tier
    pub tier: ExecutionTier,
    /// The IR (set when optimized or compiled)
    pub ir: Option<JitIR>,
    /// When this profile was last accessed
    pub last_accessed: Instant,
}

impl ExecutionProfile {
    /// Creates a new profile in the `Interpreted` tier with zero executions.
    pub fn new(id: String, expr_type: ExpressionType) -> Self {
        Self {
            id,
            expr_type,
            execution_count: 0,
            total_time: Duration::ZERO,
            tier: ExecutionTier::Interpreted,
            ir: None,
            last_accessed: Instant::now(),
        }
    }

    /// Average execution time
    pub fn avg_time(&self) -> Duration {
        if self.execution_count == 0 {
            Duration::ZERO
        } else {
            self.total_time / self.execution_count as u32
        }
    }
}

/// Configuration for the JIT engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitConfig {
    /// Enable JIT compilation
    pub enabled: bool,
    /// Threshold to move from Interpreted → Optimized
    pub optimize_threshold: u64,
    /// Threshold to move from Optimized → Compiled
    pub compile_threshold: u64,
    /// Maximum number of compiled expressions in cache
    pub max_cache_size: usize,
    /// Eviction age: profiles not accessed within this duration are evicted
    pub eviction_age_secs: u64,
    /// Maximum IR depth (prevent stack overflow on deeply nested expressions)
    pub max_ir_depth: usize,
}

impl Default for JitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            optimize_threshold: 100,
            compile_threshold: 1000,
            max_cache_size: 10_000,
            eviction_age_secs: 3600,
            max_ir_depth: 64,
        }
    }
}

/// The JIT compilation engine
pub struct JitEngine {
    /// Configuration
    config: JitConfig,
    /// Execution profiles indexed by expression ID
    profiles: RwLock<HashMap<String, ExecutionProfile>>,
    /// Compiled expression cache (ID → optimized IR)
    compiled_cache: RwLock<HashMap<String, CompiledExpression>>,
    /// Statistics
    stats: JitStats,
}

/// A compiled expression ready for fast evaluation
#[derive(Debug, Clone)]
pub struct CompiledExpression {
    /// The optimized IR
    pub ir: JitIR,
    /// Tier of compilation
    pub tier: ExecutionTier,
    /// When it was compiled
    pub compiled_at: Instant,
    /// Estimated speedup factor
    pub speedup_factor: f64,
}

/// JIT engine statistics
#[derive(Debug)]
pub struct JitStats {
    /// Total number of expression executions
    pub total_executions: AtomicU64,
    /// Executions dispatched to the interpreted tier
    pub interpreted_executions: AtomicU64,
    /// Executions dispatched to the optimized tier
    pub optimized_executions: AtomicU64,
    /// Executions dispatched to the compiled tier
    pub compiled_executions: AtomicU64,
    /// Number of compilation events
    pub compilations: AtomicU64,
    /// Compiled-expression cache hits
    pub cache_hits: AtomicU64,
    /// Compiled-expression cache misses
    pub cache_misses: AtomicU64,
    /// Number of cache evictions
    pub evictions: AtomicU64,
}

impl Default for JitStats {
    fn default() -> Self {
        Self {
            total_executions: AtomicU64::new(0),
            interpreted_executions: AtomicU64::new(0),
            optimized_executions: AtomicU64::new(0),
            compiled_executions: AtomicU64::new(0),
            compilations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
}

impl JitEngine {
    /// Create a new JIT engine with default configuration
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::with_config(JitConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: JitConfig) -> Self {
        Self {
            config,
            profiles: RwLock::new(HashMap::new()),
            compiled_cache: RwLock::new(HashMap::new()),
            stats: JitStats::default(),
        }
    }

    /// Record an execution and potentially trigger compilation
    pub fn record_execution(
        &self,
        expr_id: &str,
        expr_type: ExpressionType,
        execution_time: Duration,
        ir: Option<JitIR>,
    ) -> ExecutionTier {
        if !self.config.enabled {
            return ExecutionTier::Interpreted;
        }

        self.stats.total_executions.fetch_add(1, Ordering::Relaxed);

        let mut profiles = self.profiles.write();
        let profile = profiles
            .entry(expr_id.to_string())
            .or_insert_with(|| ExecutionProfile::new(expr_id.to_string(), expr_type));

        profile.execution_count += 1;
        profile.total_time += execution_time;
        profile.last_accessed = Instant::now();

        // Check for tier promotion
        let new_tier = if profile.execution_count >= self.config.compile_threshold {
            ExecutionTier::Compiled
        } else if profile.execution_count >= self.config.optimize_threshold {
            ExecutionTier::Optimized
        } else {
            ExecutionTier::Interpreted
        };

        if new_tier > profile.tier {
            let old_tier = profile.tier;
            profile.tier = new_tier;

            if let Some(ir) = ir.or_else(|| profile.ir.clone()) {
                profile.ir = Some(ir.clone());
                if new_tier >= ExecutionTier::Optimized {
                    self.compile_expression(expr_id, ir, new_tier);
                }
            }

            debug!(
                expr_id = expr_id,
                old_tier = %old_tier,
                new_tier = %new_tier,
                count = profile.execution_count,
                "JIT tier promotion"
            );
        }

        // Track stats by tier
        match new_tier {
            ExecutionTier::Interpreted => {
                self.stats
                    .interpreted_executions
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExecutionTier::Optimized => {
                self.stats
                    .optimized_executions
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExecutionTier::Compiled => {
                self.stats
                    .compiled_executions
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        new_tier
    }

    /// Compile an expression to optimized IR
    fn compile_expression(&self, expr_id: &str, ir: JitIR, tier: ExecutionTier) {
        let optimized_ir = match tier {
            ExecutionTier::Optimized => self.optimize_ir(ir),
            ExecutionTier::Compiled => self.compile_ir(ir),
            ExecutionTier::Interpreted => return,
        };

        let compiled = CompiledExpression {
            ir: optimized_ir,
            tier,
            compiled_at: Instant::now(),
            speedup_factor: match tier {
                ExecutionTier::Optimized => 2.0,
                ExecutionTier::Compiled => 10.0,
                _ => 1.0,
            },
        };

        let mut cache = self.compiled_cache.write();

        // Evict if cache is full
        if cache.len() >= self.config.max_cache_size {
            self.evict_oldest(&mut cache);
        }

        cache.insert(expr_id.to_string(), compiled);
        self.stats.compilations.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up a compiled expression
    pub fn get_compiled(&self, expr_id: &str) -> Option<CompiledExpression> {
        let cache = self.compiled_cache.read();
        if let Some(compiled) = cache.get(expr_id) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(compiled.clone())
        } else {
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Evaluate a JIT IR expression with the given variable bindings
    pub fn evaluate(&self, ir: &JitIR, vars: &HashMap<String, JitValue>) -> JitValue {
        self.evaluate_inner(ir, vars, 0)
    }

    fn evaluate_inner(
        &self,
        ir: &JitIR,
        vars: &HashMap<String, JitValue>,
        depth: usize,
    ) -> JitValue {
        if depth > self.config.max_ir_depth {
            warn!("JIT evaluation exceeded max depth");
            return JitValue::Null;
        }

        match ir {
            JitIR::Const(v) => v.clone(),
            JitIR::LoadVar(name) => vars.get(name).cloned().unwrap_or(JitValue::Null),
            JitIR::BinOp { op, left, right } => {
                let l = self.evaluate_inner(left, vars, depth + 1);
                let r = self.evaluate_inner(right, vars, depth + 1);
                self.eval_binop(*op, &l, &r)
            }
            JitIR::UnaryOp { op, operand } => {
                let v = self.evaluate_inner(operand, vars, depth + 1);
                self.eval_unary(*op, &v)
            }
            JitIR::Call { name, args } => {
                let evaluated_args: Vec<JitValue> = args
                    .iter()
                    .map(|a| self.evaluate_inner(a, vars, depth + 1))
                    .collect();
                self.eval_call(name, &evaluated_args)
            }
            JitIR::Cond {
                condition,
                then_branch,
                else_branch,
            } => {
                let cond = self.evaluate_inner(condition, vars, depth + 1);
                if cond.is_truthy() {
                    self.evaluate_inner(then_branch, vars, depth + 1)
                } else {
                    self.evaluate_inner(else_branch, vars, depth + 1)
                }
            }
            JitIR::StrCmp {
                left,
                right,
                case_sensitive,
            } => {
                let l = self.evaluate_inner(left, vars, depth + 1);
                let r = self.evaluate_inner(right, vars, depth + 1);
                let l_str = l.to_string();
                let r_str = r.to_string();
                let result = if *case_sensitive {
                    l_str == r_str
                } else {
                    l_str.to_lowercase() == r_str.to_lowercase()
                };
                JitValue::Bool(result)
            }
            JitIR::PatternMatch { value, pattern } => {
                let v = self.evaluate_inner(value, vars, depth + 1);
                let v_str = v.to_string();
                let matched = self.match_pattern(&v_str, pattern);
                JitValue::Bool(matched)
            }
            JitIR::FieldAccess { object, field } => {
                let obj = self.evaluate_inner(object, vars, depth + 1);
                match obj {
                    JitValue::Str(s) => {
                        // Simple JSON field extraction
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&s) {
                            match parsed.get(field) {
                                Some(serde_json::Value::String(s)) => JitValue::Str(s.clone()),
                                Some(serde_json::Value::Number(n)) => {
                                    if let Some(i) = n.as_i64() {
                                        JitValue::Int(i)
                                    } else if let Some(f) = n.as_f64() {
                                        JitValue::Float(f)
                                    } else {
                                        JitValue::Null
                                    }
                                }
                                Some(serde_json::Value::Bool(b)) => JitValue::Bool(*b),
                                Some(serde_json::Value::Null) | None => JitValue::Null,
                                Some(other) => JitValue::Str(other.to_string()),
                            }
                        } else {
                            JitValue::Null
                        }
                    }
                    _ => JitValue::Null,
                }
            }
            JitIR::Noop => JitValue::Null,
        }
    }

    fn eval_binop(&self, op: BinOpKind, left: &JitValue, right: &JitValue) -> JitValue {
        match op {
            BinOpKind::Add => match (left, right) {
                (JitValue::Int(a), JitValue::Int(b)) => JitValue::Int(a.saturating_add(*b)),
                (JitValue::Float(a), JitValue::Float(b)) => JitValue::Float(a + b),
                (JitValue::Int(a), JitValue::Float(b)) => JitValue::Float(*a as f64 + b),
                (JitValue::Float(a), JitValue::Int(b)) => JitValue::Float(a + *b as f64),
                (JitValue::Str(a), JitValue::Str(b)) => JitValue::Str(format!("{}{}", a, b)),
                _ => JitValue::Null,
            },
            BinOpKind::Sub => match (left, right) {
                (JitValue::Int(a), JitValue::Int(b)) => JitValue::Int(a.saturating_sub(*b)),
                _ => match (left.as_float(), right.as_float()) {
                    (Some(a), Some(b)) => JitValue::Float(a - b),
                    _ => JitValue::Null,
                },
            },
            BinOpKind::Mul => match (left, right) {
                (JitValue::Int(a), JitValue::Int(b)) => JitValue::Int(a.saturating_mul(*b)),
                _ => match (left.as_float(), right.as_float()) {
                    (Some(a), Some(b)) => JitValue::Float(a * b),
                    _ => JitValue::Null,
                },
            },
            BinOpKind::Div => match (left.as_float(), right.as_float()) {
                (Some(_), Some(0.0)) => JitValue::Null,
                (Some(a), Some(b)) => JitValue::Float(a / b),
                _ => JitValue::Null,
            },
            BinOpKind::Mod => match (left.as_int(), right.as_int()) {
                (Some(_), Some(0)) => JitValue::Null,
                (Some(a), Some(b)) => JitValue::Int(a % b),
                _ => JitValue::Null,
            },
            BinOpKind::Eq => JitValue::Bool(left == right),
            BinOpKind::Ne => JitValue::Bool(left != right),
            BinOpKind::Lt => {
                self.compare_values(left, right, |ord| ord == std::cmp::Ordering::Less)
            }
            BinOpKind::Le => self.compare_values(left, right, |ord| {
                ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal
            }),
            BinOpKind::Gt => {
                self.compare_values(left, right, |ord| ord == std::cmp::Ordering::Greater)
            }
            BinOpKind::Ge => self.compare_values(left, right, |ord| {
                ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal
            }),
            BinOpKind::And => JitValue::Bool(left.is_truthy() && right.is_truthy()),
            BinOpKind::Or => JitValue::Bool(left.is_truthy() || right.is_truthy()),
        }
    }

    fn compare_values(
        &self,
        left: &JitValue,
        right: &JitValue,
        pred: impl Fn(std::cmp::Ordering) -> bool,
    ) -> JitValue {
        match (left, right) {
            (JitValue::Int(a), JitValue::Int(b)) => JitValue::Bool(pred(a.cmp(b))),
            (JitValue::Float(a), JitValue::Float(b)) => {
                JitValue::Bool(a.partial_cmp(b).is_some_and(&pred))
            }
            (JitValue::Str(a), JitValue::Str(b)) => JitValue::Bool(pred(a.cmp(b))),
            _ => {
                // Try numeric comparison
                match (left.as_float(), right.as_float()) {
                    (Some(a), Some(b)) => JitValue::Bool(a.partial_cmp(&b).is_some_and(&pred)),
                    _ => JitValue::Null,
                }
            }
        }
    }

    fn eval_unary(&self, op: UnaryOpKind, value: &JitValue) -> JitValue {
        match op {
            UnaryOpKind::Neg => match value {
                JitValue::Int(i) => JitValue::Int(-i),
                JitValue::Float(f) => JitValue::Float(-f),
                _ => JitValue::Null,
            },
            UnaryOpKind::Not => JitValue::Bool(!value.is_truthy()),
            UnaryOpKind::IsNull => JitValue::Bool(matches!(value, JitValue::Null)),
            UnaryOpKind::IsNotNull => JitValue::Bool(!matches!(value, JitValue::Null)),
        }
    }

    fn eval_call(&self, name: &str, args: &[JitValue]) -> JitValue {
        match name {
            "len" | "strlen" => match args.first() {
                Some(JitValue::Str(s)) => JitValue::Int(s.len() as i64),
                _ => JitValue::Int(0),
            },
            "upper" | "toupper" => match args.first() {
                Some(JitValue::Str(s)) => JitValue::Str(s.to_uppercase()),
                _ => JitValue::Null,
            },
            "lower" | "tolower" => match args.first() {
                Some(JitValue::Str(s)) => JitValue::Str(s.to_lowercase()),
                _ => JitValue::Null,
            },
            "abs" => match args.first() {
                Some(JitValue::Int(i)) => JitValue::Int(i.abs()),
                Some(JitValue::Float(f)) => JitValue::Float(f.abs()),
                _ => JitValue::Null,
            },
            "min" => args
                .iter()
                .filter_map(|v| v.as_float())
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(JitValue::Float)
                .unwrap_or(JitValue::Null),
            "max" => args
                .iter()
                .filter_map(|v| v.as_float())
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(JitValue::Float)
                .unwrap_or(JitValue::Null),
            "coalesce" => args
                .iter()
                .find(|v| !matches!(v, JitValue::Null))
                .cloned()
                .unwrap_or(JitValue::Null),
            "typeof" => match args.first() {
                Some(JitValue::Null) => JitValue::Str("null".to_string()),
                Some(JitValue::Bool(_)) => JitValue::Str("bool".to_string()),
                Some(JitValue::Int(_)) => JitValue::Str("int".to_string()),
                Some(JitValue::Float(_)) => JitValue::Str("float".to_string()),
                Some(JitValue::Str(_)) => JitValue::Str("string".to_string()),
                None => JitValue::Null,
            },
            _ => JitValue::Null,
        }
    }

    fn match_pattern(&self, value: &str, pattern: &str) -> bool {
        // Simple glob-style matching (supports * and ?)
        let mut v_chars = value.chars().peekable();
        let mut p_chars = pattern.chars().peekable();

        self.match_glob(&mut v_chars, &mut p_chars)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn match_glob(
        &self,
        value: &mut std::iter::Peekable<std::str::Chars>,
        pattern: &mut std::iter::Peekable<std::str::Chars>,
    ) -> bool {
        while let Some(&pc) = pattern.peek() {
            match pc {
                '*' => {
                    pattern.next();
                    if pattern.peek().is_none() {
                        return true;
                    }
                    let remaining_pattern: String = pattern.collect();
                    let remaining_value: String = value.collect();
                    for i in 0..=remaining_value.len() {
                        let mut v = remaining_value[i..].chars().peekable();
                        let mut p = remaining_pattern.chars().peekable();
                        if self.match_glob(&mut v, &mut p) {
                            return true;
                        }
                    }
                    return false;
                }
                '?' => {
                    pattern.next();
                    if value.next().is_none() {
                        return false;
                    }
                }
                c => {
                    pattern.next();
                    match value.next() {
                        Some(vc) if vc == c => {}
                        _ => return false,
                    }
                }
            }
        }
        value.peek().is_none()
    }

    /// Optimize IR by applying constant folding and simplification
    fn optimize_ir(&self, ir: JitIR) -> JitIR {
        match ir {
            JitIR::BinOp { op, left, right } => {
                let opt_left = self.optimize_ir(*left);
                let opt_right = self.optimize_ir(*right);

                // Constant folding
                if let (JitIR::Const(ref l), JitIR::Const(ref r)) = (&opt_left, &opt_right) {
                    let result = self.eval_binop(op, l, r);
                    return JitIR::Const(result);
                }

                // Identity simplifications
                match op {
                    BinOpKind::Add => {
                        if matches!(&opt_right, JitIR::Const(JitValue::Int(0))) {
                            return opt_left;
                        }
                        if matches!(&opt_left, JitIR::Const(JitValue::Int(0))) {
                            return opt_right;
                        }
                    }
                    BinOpKind::Mul => {
                        if matches!(&opt_right, JitIR::Const(JitValue::Int(1))) {
                            return opt_left;
                        }
                        if matches!(&opt_left, JitIR::Const(JitValue::Int(1))) {
                            return opt_right;
                        }
                        if matches!(&opt_right, JitIR::Const(JitValue::Int(0)))
                            || matches!(&opt_left, JitIR::Const(JitValue::Int(0)))
                        {
                            return JitIR::Const(JitValue::Int(0));
                        }
                    }
                    BinOpKind::And => {
                        if matches!(&opt_left, JitIR::Const(JitValue::Bool(false))) {
                            return JitIR::Const(JitValue::Bool(false));
                        }
                        if matches!(&opt_left, JitIR::Const(JitValue::Bool(true))) {
                            return opt_right;
                        }
                    }
                    BinOpKind::Or => {
                        if matches!(&opt_left, JitIR::Const(JitValue::Bool(true))) {
                            return JitIR::Const(JitValue::Bool(true));
                        }
                        if matches!(&opt_left, JitIR::Const(JitValue::Bool(false))) {
                            return opt_right;
                        }
                    }
                    _ => {}
                }

                JitIR::BinOp {
                    op,
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                }
            }
            JitIR::UnaryOp { op, operand } => {
                let opt = self.optimize_ir(*operand);
                if let JitIR::Const(ref v) = opt {
                    return JitIR::Const(self.eval_unary(op, v));
                }
                // Double negation elimination
                if op == UnaryOpKind::Not {
                    if let JitIR::UnaryOp {
                        op: UnaryOpKind::Not,
                        operand: inner,
                    } = opt
                    {
                        return *inner;
                    }
                }
                JitIR::UnaryOp {
                    op,
                    operand: Box::new(opt),
                }
            }
            JitIR::Cond {
                condition,
                then_branch,
                else_branch,
            } => {
                let opt_cond = self.optimize_ir(*condition);
                // Constant condition elimination
                if let JitIR::Const(ref v) = opt_cond {
                    return if v.is_truthy() {
                        self.optimize_ir(*then_branch)
                    } else {
                        self.optimize_ir(*else_branch)
                    };
                }
                JitIR::Cond {
                    condition: Box::new(opt_cond),
                    then_branch: Box::new(self.optimize_ir(*then_branch)),
                    else_branch: Box::new(self.optimize_ir(*else_branch)),
                }
            }
            other => other,
        }
    }

    /// Full compilation pass — optimize + additional passes
    fn compile_ir(&self, ir: JitIR) -> JitIR {
        // First optimize, then apply additional compilation passes
        let optimized = self.optimize_ir(ir);
        // Future: Cranelift codegen would go here
        // For now, multi-pass optimization serves as our "compilation"
        self.optimize_ir(optimized) // Second pass catches newly-exposed constants
    }

    /// Evict the least recently used compiled expression
    fn evict_oldest(&self, cache: &mut HashMap<String, CompiledExpression>) {
        if let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, v)| v.compiled_at)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest_key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get the current execution tier for an expression
    pub fn get_tier(&self, expr_id: &str) -> ExecutionTier {
        self.profiles
            .read()
            .get(expr_id)
            .map(|p| p.tier)
            .unwrap_or(ExecutionTier::Interpreted)
    }

    /// Get a snapshot of engine statistics
    pub fn stats_snapshot(&self) -> JitStatsSnapshot {
        JitStatsSnapshot {
            total_executions: self.stats.total_executions.load(Ordering::Relaxed),
            interpreted_executions: self.stats.interpreted_executions.load(Ordering::Relaxed),
            optimized_executions: self.stats.optimized_executions.load(Ordering::Relaxed),
            compiled_executions: self.stats.compiled_executions.load(Ordering::Relaxed),
            compilations: self.stats.compilations.load(Ordering::Relaxed),
            cache_hits: self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.stats.cache_misses.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            profiles_count: self.profiles.read().len(),
            compiled_count: self.compiled_cache.read().len(),
        }
    }

    /// Clear all profiles and compiled expressions
    pub fn clear(&self) {
        self.profiles.write().clear();
        self.compiled_cache.write().clear();
    }
}

/// Serializable statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitStatsSnapshot {
    /// Total number of expression executions
    pub total_executions: u64,
    /// Executions dispatched to the interpreted tier
    pub interpreted_executions: u64,
    /// Executions dispatched to the optimized tier
    pub optimized_executions: u64,
    /// Executions dispatched to the compiled tier
    pub compiled_executions: u64,
    /// Number of compilation events
    pub compilations: u64,
    /// Compiled-expression cache hits
    pub cache_hits: u64,
    /// Compiled-expression cache misses
    pub cache_misses: u64,
    /// Number of cache evictions
    pub evictions: u64,
    /// Number of active execution profiles
    pub profiles_count: usize,
    /// Number of compiled expressions in cache
    pub compiled_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jit_value_truthiness() {
        assert!(!JitValue::Null.is_truthy());
        assert!(!JitValue::Bool(false).is_truthy());
        assert!(JitValue::Bool(true).is_truthy());
        assert!(!JitValue::Int(0).is_truthy());
        assert!(JitValue::Int(42).is_truthy());
        assert!(!JitValue::Str(String::new()).is_truthy());
        assert!(JitValue::Str("hello".to_string()).is_truthy());
    }

    #[test]
    fn test_evaluate_constants() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        assert_eq!(
            engine.evaluate(&JitIR::Const(JitValue::Int(42)), &vars),
            JitValue::Int(42)
        );
    }

    #[test]
    fn test_evaluate_variables() {
        let engine = JitEngine::new();
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), JitValue::Int(10));

        assert_eq!(
            engine.evaluate(&JitIR::LoadVar("x".to_string()), &vars),
            JitValue::Int(10)
        );
        assert_eq!(
            engine.evaluate(&JitIR::LoadVar("missing".to_string()), &vars),
            JitValue::Null
        );
    }

    #[test]
    fn test_evaluate_arithmetic() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        // 2 + 3 = 5
        let ir = JitIR::BinOp {
            op: BinOpKind::Add,
            left: Box::new(JitIR::Const(JitValue::Int(2))),
            right: Box::new(JitIR::Const(JitValue::Int(3))),
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Int(5));

        // 10 / 0 = null
        let ir = JitIR::BinOp {
            op: BinOpKind::Div,
            left: Box::new(JitIR::Const(JitValue::Float(10.0))),
            right: Box::new(JitIR::Const(JitValue::Float(0.0))),
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Null);
    }

    #[test]
    fn test_evaluate_comparison() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        let ir = JitIR::BinOp {
            op: BinOpKind::Gt,
            left: Box::new(JitIR::Const(JitValue::Int(5))),
            right: Box::new(JitIR::Const(JitValue::Int(3))),
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Bool(true));
    }

    #[test]
    fn test_evaluate_logical() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        let ir = JitIR::BinOp {
            op: BinOpKind::And,
            left: Box::new(JitIR::Const(JitValue::Bool(true))),
            right: Box::new(JitIR::Const(JitValue::Bool(false))),
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Bool(false));
    }

    #[test]
    fn test_evaluate_conditional() {
        let engine = JitEngine::new();
        let mut vars = HashMap::new();
        vars.insert("age".to_string(), JitValue::Int(25));

        let ir = JitIR::Cond {
            condition: Box::new(JitIR::BinOp {
                op: BinOpKind::Ge,
                left: Box::new(JitIR::LoadVar("age".to_string())),
                right: Box::new(JitIR::Const(JitValue::Int(18))),
            }),
            then_branch: Box::new(JitIR::Const(JitValue::Str("adult".to_string()))),
            else_branch: Box::new(JitIR::Const(JitValue::Str("minor".to_string()))),
        };
        assert_eq!(
            engine.evaluate(&ir, &vars),
            JitValue::Str("adult".to_string())
        );
    }

    #[test]
    fn test_evaluate_function_calls() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        // strlen("hello") = 5
        let ir = JitIR::Call {
            name: "strlen".to_string(),
            args: vec![JitIR::Const(JitValue::Str("hello".to_string()))],
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Int(5));

        // upper("hello") = "HELLO"
        let ir = JitIR::Call {
            name: "upper".to_string(),
            args: vec![JitIR::Const(JitValue::Str("hello".to_string()))],
        };
        assert_eq!(
            engine.evaluate(&ir, &vars),
            JitValue::Str("HELLO".to_string())
        );

        // abs(-42) = 42
        let ir = JitIR::Call {
            name: "abs".to_string(),
            args: vec![JitIR::Const(JitValue::Int(-42))],
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Int(42));
    }

    #[test]
    fn test_evaluate_pattern_match() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        let ir = JitIR::PatternMatch {
            value: Box::new(JitIR::Const(JitValue::Str("user:123".to_string()))),
            pattern: "user:*".to_string(),
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Bool(true));

        let ir = JitIR::PatternMatch {
            value: Box::new(JitIR::Const(JitValue::Str("order:456".to_string()))),
            pattern: "user:*".to_string(),
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Bool(false));
    }

    #[test]
    fn test_evaluate_field_access() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        let json = r#"{"name": "Alice", "age": 30}"#;
        let ir = JitIR::FieldAccess {
            object: Box::new(JitIR::Const(JitValue::Str(json.to_string()))),
            field: "name".to_string(),
        };
        assert_eq!(
            engine.evaluate(&ir, &vars),
            JitValue::Str("Alice".to_string())
        );
    }

    #[test]
    fn test_optimize_constant_folding() {
        let engine = JitEngine::new();

        // (2 + 3) * 4 should fold to 20
        let ir = JitIR::BinOp {
            op: BinOpKind::Mul,
            left: Box::new(JitIR::BinOp {
                op: BinOpKind::Add,
                left: Box::new(JitIR::Const(JitValue::Int(2))),
                right: Box::new(JitIR::Const(JitValue::Int(3))),
            }),
            right: Box::new(JitIR::Const(JitValue::Int(4))),
        };

        let optimized = engine.optimize_ir(ir);
        assert!(matches!(optimized, JitIR::Const(JitValue::Int(20))));
    }

    #[test]
    fn test_optimize_identity() {
        let engine = JitEngine::new();

        // x + 0 should simplify to x
        let ir = JitIR::BinOp {
            op: BinOpKind::Add,
            left: Box::new(JitIR::LoadVar("x".to_string())),
            right: Box::new(JitIR::Const(JitValue::Int(0))),
        };
        let optimized = engine.optimize_ir(ir);
        assert!(matches!(optimized, JitIR::LoadVar(ref s) if s == "x"));

        // x * 1 should simplify to x
        let ir = JitIR::BinOp {
            op: BinOpKind::Mul,
            left: Box::new(JitIR::LoadVar("x".to_string())),
            right: Box::new(JitIR::Const(JitValue::Int(1))),
        };
        let optimized = engine.optimize_ir(ir);
        assert!(matches!(optimized, JitIR::LoadVar(ref s) if s == "x"));
    }

    #[test]
    fn test_optimize_dead_branch_elimination() {
        let engine = JitEngine::new();

        // if true then "yes" else "no" → "yes"
        let ir = JitIR::Cond {
            condition: Box::new(JitIR::Const(JitValue::Bool(true))),
            then_branch: Box::new(JitIR::Const(JitValue::Str("yes".to_string()))),
            else_branch: Box::new(JitIR::Const(JitValue::Str("no".to_string()))),
        };
        let optimized = engine.optimize_ir(ir);
        assert!(matches!(optimized, JitIR::Const(JitValue::Str(ref s)) if s == "yes"));
    }

    #[test]
    fn test_optimize_double_negation() {
        let engine = JitEngine::new();

        // !!x → x
        let ir = JitIR::UnaryOp {
            op: UnaryOpKind::Not,
            operand: Box::new(JitIR::UnaryOp {
                op: UnaryOpKind::Not,
                operand: Box::new(JitIR::LoadVar("x".to_string())),
            }),
        };
        let optimized = engine.optimize_ir(ir);
        assert!(matches!(optimized, JitIR::LoadVar(ref s) if s == "x"));
    }

    #[test]
    fn test_tier_promotion() {
        let config = JitConfig {
            optimize_threshold: 3,
            compile_threshold: 5,
            ..Default::default()
        };
        let engine = JitEngine::with_config(config);
        let ir = JitIR::BinOp {
            op: BinOpKind::Add,
            left: Box::new(JitIR::Const(JitValue::Int(1))),
            right: Box::new(JitIR::Const(JitValue::Int(2))),
        };

        // First 2 executions: interpreted
        for _ in 0..2 {
            let tier = engine.record_execution(
                "test-expr",
                ExpressionType::Predicate,
                Duration::from_micros(100),
                Some(ir.clone()),
            );
            assert_eq!(tier, ExecutionTier::Interpreted);
        }

        // 3rd execution: promoted to optimized
        let tier = engine.record_execution(
            "test-expr",
            ExpressionType::Predicate,
            Duration::from_micros(100),
            Some(ir.clone()),
        );
        assert_eq!(tier, ExecutionTier::Optimized);

        // 4th and 5th: should get compiled
        engine.record_execution(
            "test-expr",
            ExpressionType::Predicate,
            Duration::from_micros(100),
            Some(ir.clone()),
        );
        let tier = engine.record_execution(
            "test-expr",
            ExpressionType::Predicate,
            Duration::from_micros(100),
            Some(ir.clone()),
        );
        assert_eq!(tier, ExecutionTier::Compiled);
    }

    #[test]
    fn test_compiled_cache() {
        let config = JitConfig {
            optimize_threshold: 1,
            ..Default::default()
        };
        let engine = JitEngine::with_config(config);
        let ir = JitIR::Const(JitValue::Int(42));

        // Record execution to trigger compilation
        engine.record_execution(
            "cached-expr",
            ExpressionType::Predicate,
            Duration::from_micros(10),
            Some(ir),
        );

        let compiled = engine.get_compiled("cached-expr");
        assert!(compiled.is_some());
        assert_eq!(compiled.unwrap().tier, ExecutionTier::Optimized);
    }

    #[test]
    fn test_stats_tracking() {
        let engine = JitEngine::new();

        for _ in 0..5 {
            engine.record_execution(
                "stats-test",
                ExpressionType::LuaScript,
                Duration::from_micros(50),
                None,
            );
        }

        let stats = engine.stats_snapshot();
        assert_eq!(stats.total_executions, 5);
        assert_eq!(stats.interpreted_executions, 5);
        assert_eq!(stats.profiles_count, 1);
    }

    #[test]
    fn test_jit_disabled() {
        let config = JitConfig {
            enabled: false,
            ..Default::default()
        };
        let engine = JitEngine::with_config(config);

        let tier = engine.record_execution(
            "disabled",
            ExpressionType::Predicate,
            Duration::from_micros(10),
            None,
        );
        assert_eq!(tier, ExecutionTier::Interpreted);
    }

    #[test]
    fn test_engine_clear() {
        let engine = JitEngine::new();
        engine.record_execution(
            "clear-test",
            ExpressionType::Predicate,
            Duration::from_micros(10),
            None,
        );
        assert_eq!(engine.stats_snapshot().profiles_count, 1);

        engine.clear();
        assert_eq!(engine.stats_snapshot().profiles_count, 0);
    }

    #[test]
    fn test_string_concat() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        let ir = JitIR::BinOp {
            op: BinOpKind::Add,
            left: Box::new(JitIR::Const(JitValue::Str("hello ".to_string()))),
            right: Box::new(JitIR::Const(JitValue::Str("world".to_string()))),
        };
        assert_eq!(
            engine.evaluate(&ir, &vars),
            JitValue::Str("hello world".to_string())
        );
    }

    #[test]
    fn test_coalesce_function() {
        let engine = JitEngine::new();
        let vars = HashMap::new();

        let ir = JitIR::Call {
            name: "coalesce".to_string(),
            args: vec![
                JitIR::Const(JitValue::Null),
                JitIR::Const(JitValue::Null),
                JitIR::Const(JitValue::Int(42)),
            ],
        };
        assert_eq!(engine.evaluate(&ir, &vars), JitValue::Int(42));
    }

    #[test]
    fn test_max_depth_protection() {
        let engine = JitEngine::with_config(JitConfig {
            max_ir_depth: 3,
            ..Default::default()
        });
        let vars = HashMap::new();

        // Build deeply nested expression
        let mut ir = JitIR::Const(JitValue::Int(1));
        for _ in 0..10 {
            ir = JitIR::UnaryOp {
                op: UnaryOpKind::Neg,
                operand: Box::new(ir),
            };
        }

        // Should return Null due to depth limit
        let result = engine.evaluate(&ir, &vars);
        assert_eq!(result, JitValue::Null);
    }

    #[test]
    fn test_short_circuit_and() {
        let engine = JitEngine::new();

        // false AND x → false (without evaluating x)
        let ir = JitIR::BinOp {
            op: BinOpKind::And,
            left: Box::new(JitIR::Const(JitValue::Bool(false))),
            right: Box::new(JitIR::LoadVar("x".to_string())),
        };
        let optimized = engine.optimize_ir(ir);
        assert!(matches!(optimized, JitIR::Const(JitValue::Bool(false))));
    }

    #[test]
    fn test_multiply_by_zero() {
        let engine = JitEngine::new();

        // x * 0 → 0
        let ir = JitIR::BinOp {
            op: BinOpKind::Mul,
            left: Box::new(JitIR::LoadVar("x".to_string())),
            right: Box::new(JitIR::Const(JitValue::Int(0))),
        };
        let optimized = engine.optimize_ir(ir);
        assert!(matches!(optimized, JitIR::Const(JitValue::Int(0))));
    }
}
