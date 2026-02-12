//! JSONPath query engine (RFC 9535 subset)
//!
//! Supports:
//! - `$.store.book` — Direct child access
//! - `$.store.book[0]` — Array index
//! - `$.store.book[-1]` — Negative index (from end)
//! - `$.store.book[0:3]` — Array slice
//! - `$.store.book[*]` — Wildcard (all elements)
//! - `$.store..price` — Recursive descent
//! - `$.store.book[?@.price < 10]` — Filter expressions

pub mod ast;
pub mod evaluator;
pub mod filter;
pub mod parser;

pub use ast::JsonPath;
pub use evaluator::{evaluate, evaluate_owned};
pub use parser::parse;
