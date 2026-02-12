//! Cypher query parser
//!
//! A hand-written recursive descent parser for a subset of Cypher.

use std::collections::HashMap;

use crate::graph::PropertyValue;

use super::ast::*;

/// Token types produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Keywords
    Match,
    Where,
    Return,
    Create,
    OrderBy,
    Limit,
    Skip,
    As,
    And,
    Or,
    Not,
    Distinct,
    By,
    Asc,
    Desc,
    Contains,
    StartsWith,
    EndsWith,
    // Aggregates
    Count,
    Sum,
    Avg,
    Min,
    Max,
    // Literals
    Ident(String),
    StringLit(String),
    IntLit(i64),
    FloatLit(f64),
    BoolLit(bool),
    Null,
    // Symbols
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Comma,
    Dot,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Dash,
    Arrow,  // ->
    LArrow, // <-
    Star,
    DotDot, // ..
    // End
    Eof,
}

/// Tokenize a Cypher query string.
fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Skip whitespace
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        // Two-char tokens
        if i + 1 < len {
            let two = &input[i..i + 2];
            match two {
                "->" => {
                    tokens.push(Token::Arrow);
                    i += 2;
                    continue;
                }
                "<-" => {
                    tokens.push(Token::LArrow);
                    i += 2;
                    continue;
                }
                "<>" => {
                    tokens.push(Token::Ne);
                    i += 2;
                    continue;
                }
                "!=" => {
                    tokens.push(Token::Ne);
                    i += 2;
                    continue;
                }
                "<=" => {
                    tokens.push(Token::Le);
                    i += 2;
                    continue;
                }
                ">=" => {
                    tokens.push(Token::Ge);
                    i += 2;
                    continue;
                }
                ".." => {
                    tokens.push(Token::DotDot);
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char tokens
        match chars[i] {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            '{' => {
                tokens.push(Token::LBrace);
                i += 1;
            }
            '}' => {
                tokens.push(Token::RBrace);
                i += 1;
            }
            ':' => {
                tokens.push(Token::Colon);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            '<' => {
                tokens.push(Token::Lt);
                i += 1;
            }
            '>' => {
                tokens.push(Token::Gt);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Dash);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            // String literals
            '\'' | '"' => {
                let quote = chars[i];
                i += 1;
                let start = i;
                while i < len && chars[i] != quote {
                    if chars[i] == '\\' {
                        i += 1; // skip escaped char
                    }
                    i += 1;
                }
                if i >= len {
                    return Err("unterminated string literal".to_string());
                }
                let s: String = chars[start..i].iter().collect();
                tokens.push(Token::StringLit(s));
                i += 1; // skip closing quote
            }
            // Numbers
            c if c.is_ascii_digit() => {
                let start = i;
                while i < len && chars[i].is_ascii_digit() {
                    i += 1;
                }
                // Check for float (single dot NOT followed by another dot)
                if i < len && chars[i] == '.' && (i + 1 >= len || chars[i + 1] != '.') {
                    i += 1; // consume the dot
                    while i < len && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    let num_str: String = chars[start..i].iter().collect();
                    match num_str.parse::<f64>() {
                        Ok(f) => tokens.push(Token::FloatLit(f)),
                        Err(_) => return Err(format!("invalid float: {}", num_str)),
                    }
                } else {
                    let num_str: String = chars[start..i].iter().collect();
                    match num_str.parse::<i64>() {
                        Ok(n) => tokens.push(Token::IntLit(n)),
                        Err(_) => return Err(format!("invalid integer: {}", num_str)),
                    }
                }
            }
            // Identifiers and keywords
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let upper = word.to_uppercase();
                let tok = match upper.as_str() {
                    "MATCH" => Token::Match,
                    "WHERE" => Token::Where,
                    "RETURN" => Token::Return,
                    "CREATE" => Token::Create,
                    "ORDER" => Token::OrderBy,
                    "LIMIT" => Token::Limit,
                    "SKIP" => Token::Skip,
                    "AS" => Token::As,
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    "DISTINCT" => Token::Distinct,
                    "BY" => Token::By,
                    "ASC" => Token::Asc,
                    "DESC" => Token::Desc,
                    "CONTAINS" => Token::Contains,
                    "STARTS" => {
                        // Check for "STARTS WITH"
                        // peek ahead for WITH
                        let rest: String = chars[i..].iter().collect();
                        let trimmed = rest.trim_start();
                        if trimmed.to_uppercase().starts_with("WITH") {
                            // skip whitespace + "WITH"
                            let ws = rest.len() - trimmed.len();
                            i += ws + 4;
                            Token::StartsWith
                        } else {
                            Token::Ident(word)
                        }
                    }
                    "ENDS" => {
                        let rest: String = chars[i..].iter().collect();
                        let trimmed = rest.trim_start();
                        if trimmed.to_uppercase().starts_with("WITH") {
                            let ws = rest.len() - trimmed.len();
                            i += ws + 4;
                            Token::EndsWith
                        } else {
                            Token::Ident(word)
                        }
                    }
                    "COUNT" => Token::Count,
                    "SUM" => Token::Sum,
                    "AVG" => Token::Avg,
                    "MIN" => Token::Min,
                    "MAX" => Token::Max,
                    "TRUE" => Token::BoolLit(true),
                    "FALSE" => Token::BoolLit(false),
                    "NULL" => Token::Null,
                    _ => Token::Ident(word),
                };
                tokens.push(tok);
            }
            c => return Err(format!("unexpected character: '{}'", c)),
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

/// Recursive descent parser for Cypher.
pub struct CypherParser {
    tokens: Vec<Token>,
    pos: usize,
}

impl CypherParser {
    /// Parse a Cypher query string into an AST.
    pub fn parse(input: &str) -> Result<CypherStatement, String> {
        let tokens = tokenize(input.trim())?;
        let mut parser = Self { tokens, pos: 0 };
        parser.parse_statement()
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        let tok = self.advance();
        if &tok == expected {
            Ok(())
        } else {
            Err(format!("expected {:?}, got {:?}", expected, tok))
        }
    }

    fn parse_statement(&mut self) -> Result<CypherStatement, String> {
        match self.peek().clone() {
            Token::Match => {
                self.advance();
                let match_clause = self.parse_match_pattern()?;

                match self.peek().clone() {
                    Token::Create => {
                        self.advance();
                        let create_clause = self.parse_create_clause()?;
                        Ok(CypherStatement::MatchCreate {
                            match_clause,
                            create_clause,
                        })
                    }
                    _ => {
                        let where_clause = if *self.peek() == Token::Where {
                            self.advance();
                            Some(self.parse_where_expr()?)
                        } else {
                            None
                        };

                        self.expect(&Token::Return)?;
                        let return_clause = self.parse_return_clause()?;

                        let order_by = self.parse_order_by()?;
                        let limit = self.parse_limit()?;
                        let skip = self.parse_skip()?;

                        Ok(CypherStatement::Query(CypherQuery {
                            match_clause,
                            where_clause,
                            return_clause,
                            order_by,
                            limit,
                            skip,
                        }))
                    }
                }
            }
            Token::Create => {
                self.advance();
                let create_clause = self.parse_create_clause()?;
                Ok(CypherStatement::Create(create_clause))
            }
            _ => Err(format!("expected MATCH or CREATE, got {:?}", self.peek())),
        }
    }

    fn parse_match_pattern(&mut self) -> Result<MatchPattern, String> {
        let mut patterns = Vec::new();
        patterns.push(self.parse_pattern_part()?);
        while *self.peek() == Token::Comma {
            self.advance();
            patterns.push(self.parse_pattern_part()?);
        }
        Ok(MatchPattern { patterns })
    }

    fn parse_pattern_part(&mut self) -> Result<PatternPart, String> {
        let start = self.parse_node_pattern()?;
        let mut chain = Vec::new();

        while matches!(self.peek(), Token::Dash | Token::LArrow) {
            let rel = self.parse_rel_pattern()?;
            let node = self.parse_node_pattern()?;
            chain.push((rel, node));
        }

        Ok(PatternPart { start, chain })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePatternAst, String> {
        self.expect(&Token::LParen)?;

        let mut variable = None;
        let mut labels = Vec::new();
        let mut properties = None;

        // Optional variable name
        if let Token::Ident(_) = self.peek() {
            if let Token::Ident(name) = self.advance() {
                variable = Some(name);
            }
        }

        // Optional labels (:Label1:Label2)
        while *self.peek() == Token::Colon {
            self.advance();
            if let Token::Ident(label) = self.advance() {
                labels.push(label);
            } else {
                return Err("expected label name after ':'".to_string());
            }
        }

        // Optional properties
        if *self.peek() == Token::LBrace {
            properties = Some(self.parse_property_map()?);
        }

        self.expect(&Token::RParen)?;

        Ok(NodePatternAst {
            variable,
            labels,
            properties,
        })
    }

    fn parse_rel_pattern(&mut self) -> Result<RelPatternAst, String> {
        // Determine direction: -[...]-> or <-[...]- or -[...]-
        let left_arrow = *self.peek() == Token::LArrow;
        if left_arrow {
            self.advance(); // <-
        } else {
            self.expect(&Token::Dash)?;
        }

        let mut variable = None;
        let mut rel_type = None;
        let mut var_length = None;
        let mut properties = None;

        // Optional bracket section
        if *self.peek() == Token::LBracket {
            self.advance();

            // Optional variable
            if let Token::Ident(_) = self.peek() {
                if let Token::Ident(name) = self.advance() {
                    variable = Some(name);
                }
            }

            // Optional type
            if *self.peek() == Token::Colon {
                self.advance();
                if let Token::Ident(rt) = self.advance() {
                    rel_type = Some(rt);
                } else {
                    return Err("expected relationship type after ':'".to_string());
                }
            }

            // Optional variable length *min..max
            if *self.peek() == Token::Star {
                self.advance();
                let min = if let Token::IntLit(n) = self.peek() {
                    let n = *n as usize;
                    self.advance();
                    n
                } else {
                    1
                };
                let max = if *self.peek() == Token::DotDot {
                    self.advance();
                    if let Token::IntLit(n) = self.peek() {
                        let n = *n as usize;
                        self.advance();
                        n
                    } else {
                        10 // default max
                    }
                } else {
                    min // fixed length
                };
                var_length = Some((min, max));
            }

            // Optional properties
            if *self.peek() == Token::LBrace {
                properties = Some(self.parse_property_map()?);
            }

            self.expect(&Token::RBracket)?;
        }

        // Right side of the relationship
        let direction = if left_arrow {
            // <-[...]- format
            if *self.peek() == Token::Dash {
                self.advance();
            }
            RelDirection::In
        } else if *self.peek() == Token::Arrow {
            self.advance();
            RelDirection::Out
        } else if *self.peek() == Token::Dash {
            self.advance();
            RelDirection::Both
        } else {
            RelDirection::Out
        };

        Ok(RelPatternAst {
            variable,
            rel_type,
            direction,
            var_length,
            properties,
        })
    }

    fn parse_property_map(&mut self) -> Result<HashMap<String, PropertyValue>, String> {
        self.expect(&Token::LBrace)?;
        let mut props = HashMap::new();

        if *self.peek() != Token::RBrace {
            loop {
                let key = match self.advance() {
                    Token::Ident(k) => k,
                    tok => return Err(format!("expected property key, got {:?}", tok)),
                };
                self.expect(&Token::Colon)?;
                let value = self.parse_literal_value()?;
                props.insert(key, value);

                if *self.peek() != Token::Comma {
                    break;
                }
                self.advance(); // eat comma
            }
        }

        self.expect(&Token::RBrace)?;
        Ok(props)
    }

    fn parse_literal_value(&mut self) -> Result<PropertyValue, String> {
        match self.advance() {
            Token::StringLit(s) => Ok(PropertyValue::String(s)),
            Token::IntLit(n) => Ok(PropertyValue::Integer(n)),
            Token::FloatLit(f) => Ok(PropertyValue::Float(f)),
            Token::BoolLit(b) => Ok(PropertyValue::Boolean(b)),
            Token::Null => Ok(PropertyValue::Null),
            tok => Err(format!("expected literal value, got {:?}", tok)),
        }
    }

    fn parse_where_expr(&mut self) -> Result<WhereExpr, String> {
        let left = self.parse_where_primary()?;

        match self.peek().clone() {
            Token::And => {
                self.advance();
                let right = self.parse_where_expr()?;
                Ok(WhereExpr::And(Box::new(left), Box::new(right)))
            }
            Token::Or => {
                self.advance();
                let right = self.parse_where_expr()?;
                Ok(WhereExpr::Or(Box::new(left), Box::new(right)))
            }
            _ => Ok(left),
        }
    }

    fn parse_where_primary(&mut self) -> Result<WhereExpr, String> {
        if *self.peek() == Token::Not {
            self.advance();
            let expr = self.parse_where_primary()?;
            return Ok(WhereExpr::Not(Box::new(expr)));
        }

        if *self.peek() == Token::LParen {
            self.advance();
            let expr = self.parse_where_expr()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }

        // variable.property op value
        let variable = match self.advance() {
            Token::Ident(v) => v,
            tok => return Err(format!("expected variable in WHERE, got {:?}", tok)),
        };
        self.expect(&Token::Dot)?;
        let property = match self.advance() {
            Token::Ident(p) => p,
            tok => return Err(format!("expected property name, got {:?}", tok)),
        };

        let op = self.parse_comparison_op()?;
        let right = self.parse_expr()?;

        Ok(WhereExpr::Comparison {
            left: PropertyAccess { variable, property },
            op,
            right,
        })
    }

    fn parse_comparison_op(&mut self) -> Result<CypherOp, String> {
        match self.advance() {
            Token::Eq => Ok(CypherOp::Eq),
            Token::Ne => Ok(CypherOp::Ne),
            Token::Lt => Ok(CypherOp::Lt),
            Token::Le => Ok(CypherOp::Le),
            Token::Gt => Ok(CypherOp::Gt),
            Token::Ge => Ok(CypherOp::Ge),
            Token::Contains => Ok(CypherOp::Contains),
            Token::StartsWith => Ok(CypherOp::StartsWith),
            Token::EndsWith => Ok(CypherOp::EndsWith),
            tok => Err(format!("expected comparison operator, got {:?}", tok)),
        }
    }

    fn parse_return_clause(&mut self) -> Result<ReturnClause, String> {
        let distinct = if *self.peek() == Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let mut items = Vec::new();
        items.push(self.parse_return_item()?);

        while *self.peek() == Token::Comma {
            self.advance();
            items.push(self.parse_return_item()?);
        }

        Ok(ReturnClause { items, distinct })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, String> {
        let expr = self.parse_expr()?;

        let alias = if *self.peek() == Token::As {
            self.advance();
            match self.advance() {
                Token::Ident(a) => Some(a),
                tok => return Err(format!("expected alias name, got {:?}", tok)),
            }
        } else {
            None
        };

        Ok(ReturnItem { expr, alias })
    }

    fn parse_expr(&mut self) -> Result<Expr, String> {
        // Check for aggregate functions
        match self.peek().clone() {
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                let func = match self.advance() {
                    Token::Count => AggregateFunc::Count,
                    Token::Sum => AggregateFunc::Sum,
                    Token::Avg => AggregateFunc::Avg,
                    Token::Min => AggregateFunc::Min,
                    Token::Max => AggregateFunc::Max,
                    _ => unreachable!(),
                };
                self.expect(&Token::LParen)?;
                let inner = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                return Ok(Expr::Aggregate(func, Box::new(inner)));
            }
            _ => {}
        }

        // Literal values
        match self.peek().clone() {
            Token::StringLit(_)
            | Token::IntLit(_)
            | Token::FloatLit(_)
            | Token::BoolLit(_)
            | Token::Null => {
                let val = self.parse_literal_value()?;
                return Ok(Expr::Literal(val));
            }
            _ => {}
        }

        // Identifier — could be variable or variable.property
        if let Token::Ident(_) = self.peek() {
            let name = match self.advance() {
                Token::Ident(n) => n,
                _ => unreachable!(),
            };

            if *self.peek() == Token::Dot {
                self.advance();
                let prop = match self.advance() {
                    Token::Ident(p) => p,
                    tok => return Err(format!("expected property name, got {:?}", tok)),
                };
                return Ok(Expr::Property(PropertyAccess {
                    variable: name,
                    property: prop,
                }));
            }

            return Ok(Expr::Variable(name));
        }

        Err(format!("expected expression, got {:?}", self.peek()))
    }

    fn parse_order_by(&mut self) -> Result<Option<Vec<OrderByItem>>, String> {
        if *self.peek() != Token::OrderBy {
            return Ok(None);
        }
        self.advance(); // ORDER

        // Consume BY if present
        if *self.peek() == Token::By {
            self.advance();
        }

        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let ascending = match self.peek() {
                Token::Desc => {
                    self.advance();
                    false
                }
                Token::Asc => {
                    self.advance();
                    true
                }
                _ => true,
            };
            items.push(OrderByItem { expr, ascending });

            if *self.peek() != Token::Comma {
                break;
            }
            self.advance();
        }

        Ok(Some(items))
    }

    fn parse_limit(&mut self) -> Result<Option<usize>, String> {
        if *self.peek() != Token::Limit {
            return Ok(None);
        }
        self.advance();
        match self.advance() {
            Token::IntLit(n) => Ok(Some(n as usize)),
            tok => Err(format!("expected integer after LIMIT, got {:?}", tok)),
        }
    }

    fn parse_skip(&mut self) -> Result<Option<usize>, String> {
        if *self.peek() != Token::Skip {
            return Ok(None);
        }
        self.advance();
        match self.advance() {
            Token::IntLit(n) => Ok(Some(n as usize)),
            tok => Err(format!("expected integer after SKIP, got {:?}", tok)),
        }
    }

    fn parse_create_clause(&mut self) -> Result<CreateClause, String> {
        let mut elements = Vec::new();

        loop {
            if *self.peek() == Token::LParen {
                // Could be a node or start of a relationship pattern
                let node = self.parse_node_pattern()?;

                // Check if this is followed by a relationship
                if *self.peek() == Token::Dash || *self.peek() == Token::LArrow {
                    let from_var = node.variable.clone().ok_or_else(|| {
                        "relationship source must have a variable name".to_string()
                    })?;
                    let rel = self.parse_rel_pattern()?;
                    let to_node = self.parse_node_pattern()?;
                    let to_var = to_node.variable.clone().ok_or_else(|| {
                        "relationship target must have a variable name".to_string()
                    })?;
                    elements.push(CreateElement::Relationship {
                        from: from_var,
                        rel_type: rel.rel_type.unwrap_or_else(|| "RELATED_TO".to_string()),
                        to: to_var,
                        properties: rel.properties,
                    });
                } else {
                    elements.push(CreateElement::Node(node));
                }
            } else {
                break;
            }

            if *self.peek() != Token::Comma {
                break;
            }
            self.advance();
        }

        Ok(CreateClause { elements })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_match_return() {
        let stmt = CypherParser::parse("MATCH (n:Person) RETURN n").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                assert_eq!(q.match_clause.patterns.len(), 1);
                let pat = &q.match_clause.patterns[0];
                assert_eq!(pat.start.variable, Some("n".to_string()));
                assert_eq!(pat.start.labels, vec!["Person".to_string()]);
                assert!(q.where_clause.is_none());
                assert_eq!(q.return_clause.items.len(), 1);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_match_with_properties() {
        let stmt = CypherParser::parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let props = q.match_clause.patterns[0]
                    .start
                    .properties
                    .as_ref()
                    .unwrap();
                assert_eq!(
                    props.get("name"),
                    Some(&PropertyValue::String("Alice".to_string()))
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_relationship() {
        let stmt = CypherParser::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let pat = &q.match_clause.patterns[0];
                assert_eq!(pat.chain.len(), 1);
                let (rel, _) = &pat.chain[0];
                assert_eq!(rel.rel_type, Some("KNOWS".to_string()));
                assert_eq!(rel.direction, RelDirection::Out);
                assert_eq!(q.return_clause.items.len(), 2);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_variable_length_path() {
        let stmt = CypherParser::parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let (rel, _) = &q.match_clause.patterns[0].chain[0];
                assert_eq!(rel.var_length, Some((1, 3)));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_where_clause() {
        let stmt = CypherParser::parse("MATCH (n:Person) WHERE n.age > 10 RETURN n").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                assert!(q.where_clause.is_some());
                match q.where_clause.unwrap() {
                    WhereExpr::Comparison { left, op, .. } => {
                        assert_eq!(left.variable, "n");
                        assert_eq!(left.property, "age");
                        assert_eq!(op, CypherOp::Gt);
                    }
                    _ => panic!("expected Comparison"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_aggregation() {
        let stmt = CypherParser::parse("MATCH (n) RETURN count(n)").unwrap();
        match stmt {
            CypherStatement::Query(q) => match &q.return_clause.items[0].expr {
                Expr::Aggregate(AggregateFunc::Count, _) => {}
                _ => panic!("expected Count aggregate"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_order_limit() {
        let stmt = CypherParser::parse("MATCH (n) RETURN n ORDER BY n.name LIMIT 10").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                assert!(q.order_by.is_some());
                assert_eq!(q.limit, Some(10));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_create_node() {
        let stmt = CypherParser::parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        match stmt {
            CypherStatement::Create(c) => {
                assert_eq!(c.elements.len(), 1);
                match &c.elements[0] {
                    CreateElement::Node(n) => {
                        assert_eq!(n.labels, vec!["Person".to_string()]);
                        let props = n.properties.as_ref().unwrap();
                        assert_eq!(
                            props.get("name"),
                            Some(&PropertyValue::String("Alice".to_string()))
                        );
                        assert_eq!(props.get("age"), Some(&PropertyValue::Integer(30)));
                    }
                    _ => panic!("expected Node"),
                }
            }
            _ => panic!("expected Create"),
        }
    }

    #[test]
    fn test_parse_match_create_edge() {
        let stmt = CypherParser::parse("MATCH (a), (b) CREATE (a)-[:KNOWS]->(b)").unwrap();
        match stmt {
            CypherStatement::MatchCreate {
                match_clause,
                create_clause,
            } => {
                assert_eq!(match_clause.patterns.len(), 2);
                assert_eq!(create_clause.elements.len(), 1);
                match &create_clause.elements[0] {
                    CreateElement::Relationship {
                        from, rel_type, to, ..
                    } => {
                        assert_eq!(from, "a");
                        assert_eq!(rel_type, "KNOWS");
                        assert_eq!(to, "b");
                    }
                    _ => panic!("expected Relationship"),
                }
            }
            _ => panic!("expected MatchCreate"),
        }
    }

    #[test]
    fn test_parse_where_and() {
        let stmt = CypherParser::parse("MATCH (n:Person) WHERE n.age > 20 AND n.age < 40 RETURN n")
            .unwrap();
        match stmt {
            CypherStatement::Query(q) => match q.where_clause.unwrap() {
                WhereExpr::And(_, _) => {}
                _ => panic!("expected And"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_return_with_alias() {
        let stmt = CypherParser::parse("MATCH (n) RETURN n.name AS person_name").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let item = &q.return_clause.items[0];
                assert_eq!(item.alias, Some("person_name".to_string()));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_multiple_labels() {
        let stmt = CypherParser::parse("MATCH (n:Person:Employee) RETURN n").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let labels = &q.match_clause.patterns[0].start.labels;
                assert_eq!(labels, &vec!["Person".to_string(), "Employee".to_string()]);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_order_desc() {
        let stmt = CypherParser::parse("MATCH (n) RETURN n ORDER BY n.age DESC LIMIT 5").unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let order = q.order_by.unwrap();
                assert!(!order[0].ascending);
                assert_eq!(q.limit, Some(5));
            }
            _ => panic!("expected Query"),
        }
    }
}
