//! JSONPath parser — parses JSONPath strings into AST

use super::ast::*;
use crate::document::DocumentStoreError;

/// Parse a JSONPath expression string into an AST
pub fn parse(input: &str) -> Result<JsonPath, DocumentStoreError> {
    let mut parser = Parser::new(input);
    parser.parse_path()
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn peek(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn advance(&mut self, n: usize) {
        self.pos += n;
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|c| c.is_whitespace()) {
            self.advance(1);
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), DocumentStoreError> {
        match self.peek() {
            Some(c) if c == expected => {
                self.advance(c.len_utf8());
                Ok(())
            }
            Some(c) => Err(DocumentStoreError::InvalidQuery(format!(
                "JSONPath: expected '{}', got '{}'",
                expected, c
            ))),
            None => Err(DocumentStoreError::InvalidQuery(format!(
                "JSONPath: expected '{}', got end of input",
                expected
            ))),
        }
    }

    fn parse_path(&mut self) -> Result<JsonPath, DocumentStoreError> {
        self.skip_whitespace();
        self.expect_char('$')?;

        let mut segments = Vec::new();
        while self.pos < self.input.len() {
            self.skip_whitespace();
            match self.peek() {
                Some('.') => {
                    self.advance(1);
                    if self.peek() == Some('.') {
                        // Recursive descent
                        self.advance(1);
                        let seg = self.parse_recursive_descent()?;
                        segments.push(seg);
                    } else if self.peek() == Some('*') {
                        self.advance(1);
                        segments.push(Segment::Wildcard);
                    } else {
                        let key = self.parse_identifier()?;
                        segments.push(Segment::Child(key));
                    }
                }
                Some('[') => {
                    let seg = self.parse_bracket()?;
                    segments.push(seg);
                }
                _ => break,
            }
        }

        Ok(JsonPath::new(segments))
    }

    fn parse_recursive_descent(&mut self) -> Result<Segment, DocumentStoreError> {
        if self.peek() == Some('*') {
            self.advance(1);
            return Ok(Segment::RecursiveWildcard);
        }
        let key = self.parse_identifier()?;
        Ok(Segment::RecursiveDescent(key))
    }

    fn parse_identifier(&mut self) -> Result<String, DocumentStoreError> {
        let start = self.pos;
        while self.peek().is_some_and(|c| c.is_alphanumeric() || c == '_') {
            self.advance(1);
        }
        if self.pos == start {
            return Err(DocumentStoreError::InvalidQuery(
                "JSONPath: expected identifier".into(),
            ));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_bracket(&mut self) -> Result<Segment, DocumentStoreError> {
        self.expect_char('[')?;
        self.skip_whitespace();

        let seg = match self.peek() {
            Some('*') => {
                self.advance(1);
                self.skip_whitespace();
                self.expect_char(']')?;
                Segment::Wildcard
            }
            Some('?') => {
                self.advance(1);
                self.skip_whitespace();
                let expr = self.parse_filter_expr()?;
                self.skip_whitespace();
                self.expect_char(']')?;
                Segment::Filter(expr)
            }
            Some('\'') | Some('"') => {
                let key = self.parse_quoted_string()?;
                self.skip_whitespace();
                self.expect_char(']')?;
                Segment::Child(key)
            }
            _ => {
                // Could be index, negative index, or slice
                self.parse_index_or_slice()?
            }
        };

        Ok(seg)
    }

    fn parse_index_or_slice(&mut self) -> Result<Segment, DocumentStoreError> {
        let first = self.parse_optional_integer();
        self.skip_whitespace();

        if self.peek() == Some(':') {
            // Slice
            self.advance(1);
            self.skip_whitespace();
            let end = self.parse_optional_integer();
            self.skip_whitespace();

            let step = if self.peek() == Some(':') {
                self.advance(1);
                self.skip_whitespace();
                self.parse_optional_integer()
            } else {
                None
            };

            self.skip_whitespace();
            self.expect_char(']')?;
            Ok(Segment::Slice {
                start: first,
                end,
                step,
            })
        } else {
            // Plain index
            self.skip_whitespace();
            self.expect_char(']')?;
            match first {
                Some(i) => Ok(Segment::Index(i)),
                None => Err(DocumentStoreError::InvalidQuery(
                    "JSONPath: expected index value".into(),
                )),
            }
        }
    }

    fn parse_optional_integer(&mut self) -> Option<i64> {
        self.skip_whitespace();
        let start = self.pos;
        if self.peek() == Some('-') {
            self.advance(1);
        }
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.advance(1);
        }
        if self.pos == start {
            return None;
        }
        self.input[start..self.pos].parse().ok()
    }

    fn parse_quoted_string(&mut self) -> Result<String, DocumentStoreError> {
        let quote = self.peek().ok_or_else(|| {
            DocumentStoreError::InvalidQuery("JSONPath: expected quoted string".into())
        })?;
        self.advance(1);
        let start = self.pos;
        while self.peek().is_some_and(|c| c != quote) {
            if self.peek() == Some('\\') {
                self.advance(1);
            }
            self.advance(1);
        }
        let s = self.input[start..self.pos].to_string();
        self.expect_char(quote)?;
        Ok(s)
    }

    fn parse_filter_expr(&mut self) -> Result<FilterExpr, DocumentStoreError> {
        self.skip_whitespace();
        let left = self.parse_filter_atom()?;
        self.skip_whitespace();

        // Check for logical operators
        if self.remaining().starts_with("&&") {
            self.advance(2);
            let right = self.parse_filter_expr()?;
            return Ok(FilterExpr::And(Box::new(left), Box::new(right)));
        }
        if self.remaining().starts_with("||") {
            self.advance(2);
            let right = self.parse_filter_expr()?;
            return Ok(FilterExpr::Or(Box::new(left), Box::new(right)));
        }

        Ok(left)
    }

    fn parse_filter_atom(&mut self) -> Result<FilterExpr, DocumentStoreError> {
        self.skip_whitespace();

        if self.peek() == Some('!') {
            self.advance(1);
            let expr = self.parse_filter_atom()?;
            return Ok(FilterExpr::Not(Box::new(expr)));
        }

        if self.peek() == Some('(') {
            self.advance(1);
            let expr = self.parse_filter_expr()?;
            self.skip_whitespace();
            self.expect_char(')')?;
            return Ok(expr);
        }

        let left = self.parse_filter_operand()?;
        self.skip_whitespace();

        // Check if there's a comparison operator
        if let Some(op) = self.try_parse_comparison_op() {
            self.skip_whitespace();
            let right = self.parse_filter_operand()?;
            Ok(FilterExpr::Comparison { left, op, right })
        } else {
            // Existence check — operand must be a path
            match left {
                FilterOperand::Path(p) => Ok(FilterExpr::Exists(p)),
                _ => Err(DocumentStoreError::InvalidQuery(
                    "JSONPath filter: expected comparison operator".into(),
                )),
            }
        }
    }

    fn try_parse_comparison_op(&mut self) -> Option<ComparisonOp> {
        let rem = self.remaining();
        if rem.starts_with("==") {
            self.advance(2);
            Some(ComparisonOp::Eq)
        } else if rem.starts_with("!=") {
            self.advance(2);
            Some(ComparisonOp::Ne)
        } else if rem.starts_with("<=") {
            self.advance(2);
            Some(ComparisonOp::Le)
        } else if rem.starts_with(">=") {
            self.advance(2);
            Some(ComparisonOp::Ge)
        } else if rem.starts_with('<') {
            self.advance(1);
            Some(ComparisonOp::Lt)
        } else if rem.starts_with('>') {
            self.advance(1);
            Some(ComparisonOp::Gt)
        } else {
            None
        }
    }

    fn parse_filter_operand(&mut self) -> Result<FilterOperand, DocumentStoreError> {
        self.skip_whitespace();

        if self.peek() == Some('@') {
            let path = self.parse_filter_path()?;
            Ok(FilterOperand::Path(path))
        } else {
            let lit = self.parse_literal()?;
            Ok(FilterOperand::Literal(lit))
        }
    }

    fn parse_filter_path(&mut self) -> Result<FilterPath, DocumentStoreError> {
        self.expect_char('@')?;
        let mut segments = Vec::new();

        while self.peek() == Some('.') {
            self.advance(1);
            let key = self.parse_identifier()?;
            segments.push(key);
        }

        Ok(FilterPath { segments })
    }

    fn parse_literal(&mut self) -> Result<LiteralValue, DocumentStoreError> {
        self.skip_whitespace();

        match self.peek() {
            Some('"') | Some('\'') => {
                let s = self.parse_quoted_string()?;
                Ok(LiteralValue::String(s))
            }
            Some('t') if self.remaining().starts_with("true") => {
                self.advance(4);
                Ok(LiteralValue::Bool(true))
            }
            Some('f') if self.remaining().starts_with("false") => {
                self.advance(5);
                Ok(LiteralValue::Bool(false))
            }
            Some('n') if self.remaining().starts_with("null") => {
                self.advance(4);
                Ok(LiteralValue::Null)
            }
            Some(c) if c == '-' || c.is_ascii_digit() => self.parse_number_literal(),
            _ => Err(DocumentStoreError::InvalidQuery(
                "JSONPath filter: expected literal value".into(),
            )),
        }
    }

    fn parse_number_literal(&mut self) -> Result<LiteralValue, DocumentStoreError> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.advance(1);
        }
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.advance(1);
        }
        if self.peek() == Some('.') {
            self.advance(1);
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.advance(1);
            }
            let val: f64 = self.input[start..self.pos].parse().map_err(|_| {
                DocumentStoreError::InvalidQuery("JSONPath: invalid number".into())
            })?;
            Ok(LiteralValue::Float(val))
        } else {
            let val: i64 = self.input[start..self.pos].parse().map_err(|_| {
                DocumentStoreError::InvalidQuery("JSONPath: invalid integer".into())
            })?;
            Ok(LiteralValue::Integer(val))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_root() {
        let path = parse("$").unwrap();
        assert!(path.is_root());
    }

    #[test]
    fn test_parse_child() {
        let path = parse("$.store.book").unwrap();
        assert_eq!(
            path.segments,
            vec![Segment::Child("store".into()), Segment::Child("book".into())]
        );
    }

    #[test]
    fn test_parse_index() {
        let path = parse("$.store.book[0]").unwrap();
        assert_eq!(path.segments.len(), 3);
        assert_eq!(path.segments[2], Segment::Index(0));
    }

    #[test]
    fn test_parse_negative_index() {
        let path = parse("$.store.book[-1]").unwrap();
        assert_eq!(path.segments[2], Segment::Index(-1));
    }

    #[test]
    fn test_parse_slice() {
        let path = parse("$.store.book[0:3]").unwrap();
        assert_eq!(
            path.segments[2],
            Segment::Slice {
                start: Some(0),
                end: Some(3),
                step: None,
            }
        );
    }

    #[test]
    fn test_parse_wildcard() {
        let path = parse("$.store.book[*]").unwrap();
        assert_eq!(path.segments[2], Segment::Wildcard);
    }

    #[test]
    fn test_parse_dot_wildcard() {
        let path = parse("$.store.*").unwrap();
        assert_eq!(path.segments[1], Segment::Wildcard);
    }

    #[test]
    fn test_parse_recursive_descent() {
        let path = parse("$.store..price").unwrap();
        assert_eq!(
            path.segments,
            vec![
                Segment::Child("store".into()),
                Segment::RecursiveDescent("price".into()),
            ]
        );
    }

    #[test]
    fn test_parse_filter_lt() {
        let path = parse("$.store.book[?@.price < 10]").unwrap();
        assert!(matches!(path.segments[2], Segment::Filter(_)));
    }

    #[test]
    fn test_parse_filter_eq_string() {
        let path = parse("$.store.book[?@.author == \"Tolkien\"]").unwrap();
        if let Segment::Filter(FilterExpr::Comparison { right, .. }) = &path.segments[2] {
            assert_eq!(
                *right,
                FilterOperand::Literal(LiteralValue::String("Tolkien".into()))
            );
        } else {
            panic!("Expected filter comparison");
        }
    }

    #[test]
    fn test_parse_bracket_string() {
        let path = parse("$['store']['book']").unwrap();
        assert_eq!(
            path.segments,
            vec![Segment::Child("store".into()), Segment::Child("book".into())]
        );
    }
}
