//! FerriteQL Parser
//!
//! Parses tokens into an Abstract Syntax Tree with rich error messages.

use crate::query::ast::*;
use crate::query::lexer::{Lexer, Span, Token, TokenWithSpan};
use crate::query::types::DataType;
use crate::query::QueryError;

/// Known SQL keywords for typo suggestions.
const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "NOT",
    "AS",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "CROSS",
    "ON",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "DISTINCT",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "DROP",
    "VIEW",
    "EXPLAIN",
    "EXISTS",
    "BETWEEN",
    "LIKE",
    "IN",
    "IS",
    "NULL",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "CAST",
    "TRUE",
    "FALSE",
    "PREPARE",
    "EXECUTE",
    "MATERIALIZED",
    "REFRESH",
    "INCREMENTAL",
    "COMPLETE",
    "FILTER",
    "OVER",
    "PARTITION",
];

/// Compute edit distance between two strings (case-insensitive).
fn edit_distance(a: &str, b: &str) -> usize {
    let a = a.to_uppercase();
    let b = b.to_uppercase();
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for (i, row) in dp.iter_mut().enumerate() {
        row[0] = i;
    }
    for j in 0..=n {
        dp[0][j] = j;
    }
    for i in 1..=m {
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[m][n]
}

/// Suggest a keyword correction for a misspelled identifier.
fn suggest_keyword(word: &str) -> Option<&'static str> {
    let upper = word.to_uppercase();
    // Only suggest if the word looks like it could be a keyword (short, all alpha)
    if word.len() < 2 || word.len() > 15 || word.contains(':') || word.contains('*') {
        return None;
    }
    let mut best: Option<(&str, usize)> = None;
    for &kw in KEYWORDS {
        let dist = edit_distance(&upper, kw);
        // Only suggest if edit distance is small relative to word length
        let threshold = if kw.len() <= 3 { 1 } else { 2 };
        if dist > 0 && dist <= threshold && best.map_or(true, |(_, d)| dist < d) {
            best = Some((kw, dist));
        }
    }
    best.map(|(kw, _)| kw)
}

/// Build a context snippet showing the problematic position in the source.
fn error_context(source: &str, span: &Span) -> String {
    let lines: Vec<&str> = source.lines().collect();
    if span.line == 0 || span.line > lines.len() {
        return String::new();
    }
    let line_text = lines[span.line - 1];
    let pointer = " ".repeat(span.column.saturating_sub(1)) + "^";
    format!("\n  | {}\n  | {}", line_text, pointer)
}

/// SQL Parser
pub struct QueryParser {
    source: String,
    tokens: Vec<TokenWithSpan>,
    position: usize,
}

impl QueryParser {
    /// Create a new parser
    pub fn new() -> Self {
        Self {
            source: String::new(),
            tokens: Vec::new(),
            position: 0,
        }
    }

    /// Parse a query string into a Statement
    pub fn parse(&self, query: &str) -> Result<Statement, QueryError> {
        let mut lexer = Lexer::new(query);
        let tokens = lexer.tokenize_with_spans()?;

        let mut parser = QueryParser {
            source: query.to_string(),
            tokens,
            position: 0,
        };

        parser.parse_statement()
    }

    /// Extract parameter types from a statement
    pub fn extract_param_types(&self, _statement: &Statement) -> Vec<DataType> {
        // For now, return empty - proper implementation would analyze the AST
        Vec::new()
    }

    fn parse_statement(&mut self) -> Result<Statement, QueryError> {
        match self.peek() {
            Token::Select => Ok(Statement::Select(self.parse_select()?)),
            Token::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            Token::Update => Ok(Statement::Update(self.parse_update()?)),
            Token::Delete => Ok(Statement::Delete(self.parse_delete()?)),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Explain => {
                self.advance();
                Ok(Statement::Explain(Box::new(self.parse_statement()?)))
            }
            Token::Prepare => Ok(Statement::Prepare(self.parse_prepare()?)),
            Token::Execute => Ok(Statement::Execute(self.parse_execute()?)),
            Token::Identifier(ref name) => {
                let suggestion = suggest_keyword(name);
                let span = self.current_span();
                let ctx = error_context(&self.source, &span);
                let msg = if let Some(kw) = suggestion {
                    format!(
                        "Unexpected token '{}' at {} — did you mean {}?{}",
                        name, span, kw, ctx
                    )
                } else {
                    format!("Unexpected token '{}' at {}{}", name, span, ctx)
                };
                Err(QueryError::Parse(msg))
            }
            token => {
                let span = self.current_span();
                let ctx = error_context(&self.source, &span);
                Err(QueryError::Parse(format!(
                    "Expected statement, got {:?} at {}{}",
                    token, span, ctx
                )))
            }
        }
    }

    fn parse_select(&mut self) -> Result<SelectStatement, QueryError> {
        self.expect(Token::Select)?;

        let mut stmt = SelectStatement::default();

        // DISTINCT
        if self.check(Token::Distinct) {
            self.advance();
            stmt.distinct = true;
        } else if self.check(Token::All) {
            self.advance();
        }

        // Columns
        stmt.columns = self.parse_select_columns()?;

        // FROM
        if self.check(Token::From) {
            self.advance();
            stmt.from = self.parse_from_clauses()?;
        }

        // JOINs
        while self.is_join_keyword() {
            stmt.joins.push(self.parse_join()?);
        }

        // WHERE
        if self.check(Token::Where) {
            self.advance();
            stmt.where_clause = Some(self.parse_expr()?);
        }

        // GROUP BY
        if self.check(Token::Group) {
            self.advance();
            self.expect(Token::By)?;
            stmt.group_by = self.parse_expr_list()?;

            // HAVING
            if self.check(Token::Having) {
                self.advance();
                stmt.having = Some(self.parse_expr()?);
            }
        }

        // ORDER BY
        if self.check(Token::Order) {
            self.advance();
            self.expect(Token::By)?;
            stmt.order_by = self.parse_order_by()?;
        }

        // LIMIT
        if self.check(Token::Limit) {
            self.advance();
            stmt.limit = Some(self.parse_integer()?);
        }

        // OFFSET
        if self.check(Token::Offset) {
            self.advance();
            stmt.offset = Some(self.parse_integer()?);
        }

        Ok(stmt)
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, QueryError> {
        let mut columns = Vec::new();

        loop {
            let col = if self.check(Token::Star) {
                self.advance();
                SelectColumn::All
            } else {
                let expr = self.parse_expr()?;

                // Check for table.* after identifier
                if let Expr::Column(ref col_ref) = expr {
                    if self.check(Token::Dot) {
                        self.advance();
                        if self.check(Token::Star) {
                            self.advance();
                            columns.push(SelectColumn::AllFrom(col_ref.column.clone()));
                            if !self.check(Token::Comma) {
                                break;
                            }
                            self.advance();
                            continue;
                        }
                    }
                }

                // Check for alias
                let alias = if self.check(Token::As) {
                    self.advance();
                    Some(self.parse_identifier()?)
                } else if let Token::Identifier(_) = self.peek() {
                    // Implicit alias
                    Some(self.parse_identifier()?)
                } else {
                    None
                };

                SelectColumn::Expr { expr, alias }
            };

            columns.push(col);

            if !self.check(Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(columns)
    }

    fn parse_from_clauses(&mut self) -> Result<Vec<FromClause>, QueryError> {
        let mut clauses = Vec::new();

        loop {
            let pattern = self.parse_table_pattern()?;
            let alias = if self.check(Token::As) {
                self.advance();
                Some(self.parse_identifier()?)
            } else if let Token::Identifier(_) = self.peek() {
                if !self.is_keyword() {
                    Some(self.parse_identifier()?)
                } else {
                    None
                }
            } else {
                None
            };

            clauses.push(FromClause { pattern, alias });

            if !self.check(Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(clauses)
    }

    fn parse_table_pattern(&mut self) -> Result<String, QueryError> {
        match self.peek() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            token => {
                let span = self.current_span();
                let ctx = error_context(&self.source, &span);
                Err(QueryError::Parse(format!(
                    "Expected table/pattern name, got {:?} at {}{}",
                    token, span, ctx
                )))
            }
        }
    }

    fn is_join_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Join | Token::Inner | Token::Left | Token::Right | Token::Full | Token::Cross
        )
    }

    fn parse_join(&mut self) -> Result<JoinClause, QueryError> {
        let join_type = match self.peek() {
            Token::Join | Token::Inner => {
                if self.check(Token::Inner) {
                    self.advance();
                }
                self.expect(Token::Join)?;
                JoinType::Inner
            }
            Token::Left => {
                self.advance();
                let _ = self.check(Token::Join) && {
                    self.advance();
                    true
                };
                JoinType::Left
            }
            Token::Right => {
                self.advance();
                let _ = self.check(Token::Join) && {
                    self.advance();
                    true
                };
                JoinType::Right
            }
            Token::Full => {
                self.advance();
                let _ = self.check(Token::Join) && {
                    self.advance();
                    true
                };
                JoinType::Full
            }
            Token::Cross => {
                self.advance();
                self.expect(Token::Join)?;
                JoinType::Cross
            }
            _ => return Err(QueryError::Parse("Expected JOIN keyword".to_string())),
        };

        let pattern = self.parse_table_pattern()?;
        let alias = if self.check(Token::As) {
            self.advance();
            Some(self.parse_identifier()?)
        } else if let Token::Identifier(_) = self.peek() {
            if !self.is_keyword() && !self.check(Token::On) {
                Some(self.parse_identifier()?)
            } else {
                None
            }
        } else {
            None
        };

        let source = FromClause { pattern, alias };

        let on = if self.check(Token::On) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(JoinClause {
            join_type,
            source,
            on,
        })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByItem>, QueryError> {
        let mut items = Vec::new();

        loop {
            let expr = self.parse_expr()?;

            let direction = if self.check(Token::Asc) {
                self.advance();
                SortDirection::Asc
            } else if self.check(Token::Desc) {
                self.advance();
                SortDirection::Desc
            } else {
                SortDirection::Asc
            };

            let nulls = if self.check(Token::Nulls) {
                self.advance();
                if self.check(Token::First) {
                    self.advance();
                    Some(NullsOrder::First)
                } else if self.check(Token::Last) {
                    self.advance();
                    Some(NullsOrder::Last)
                } else {
                    return Err(QueryError::Parse(
                        "Expected FIRST or LAST after NULLS".to_string(),
                    ));
                }
            } else {
                None
            };

            items.push(OrderByItem {
                expr,
                direction,
                nulls,
            });

            if !self.check(Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(items)
    }

    fn parse_expr(&mut self) -> Result<Expr, QueryError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, QueryError> {
        let mut left = self.parse_and_expr()?;

        while self.check(Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, QueryError> {
        let mut left = self.parse_not_expr()?;

        while self.check(Token::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, QueryError> {
        if self.check(Token::Not) {
            self.advance();
            let expr = self.parse_not_expr()?;
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expr()
        }
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr, QueryError> {
        let mut left = self.parse_additive_expr()?;

        loop {
            let op = match self.peek() {
                Token::Equal => BinaryOperator::Equal,
                Token::NotEqual => BinaryOperator::NotEqual,
                Token::LessThan => BinaryOperator::LessThan,
                Token::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
                Token::GreaterThan => BinaryOperator::GreaterThan,
                Token::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
                Token::Is => {
                    self.advance();
                    let negated = if self.check(Token::Not) {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    self.expect(Token::Null)?;
                    return Ok(Expr::IsNull {
                        expr: Box::new(left),
                        negated,
                    });
                }
                Token::In => {
                    self.advance();
                    let negated = false;
                    self.expect(Token::LeftParen)?;
                    if self.check(Token::Select) {
                        let subquery = self.parse_select()?;
                        self.expect(Token::RightParen)?;
                        return Ok(Expr::InSubquery {
                            expr: Box::new(left),
                            subquery: Box::new(subquery),
                            negated,
                        });
                    }
                    let list = self.parse_expr_list()?;
                    self.expect(Token::RightParen)?;
                    return Ok(Expr::In {
                        expr: Box::new(left),
                        list,
                        negated,
                    });
                }
                Token::Not => {
                    self.advance();
                    if self.check(Token::In) {
                        self.advance();
                        self.expect(Token::LeftParen)?;
                        if self.check(Token::Select) {
                            let subquery = self.parse_select()?;
                            self.expect(Token::RightParen)?;
                            return Ok(Expr::InSubquery {
                                expr: Box::new(left),
                                subquery: Box::new(subquery),
                                negated: true,
                            });
                        }
                        let list = self.parse_expr_list()?;
                        self.expect(Token::RightParen)?;
                        return Ok(Expr::In {
                            expr: Box::new(left),
                            list,
                            negated: true,
                        });
                    } else if self.check(Token::Like) {
                        self.advance();
                        let pattern = self.parse_additive_expr()?;
                        return Ok(Expr::Like {
                            expr: Box::new(left),
                            pattern: Box::new(pattern),
                            negated: true,
                        });
                    } else if self.check(Token::Between) {
                        self.advance();
                        let low = self.parse_additive_expr()?;
                        self.expect(Token::And)?;
                        let high = self.parse_additive_expr()?;
                        return Ok(Expr::Between {
                            expr: Box::new(left),
                            low: Box::new(low),
                            high: Box::new(high),
                            negated: true,
                        });
                    } else {
                        return Err(QueryError::Parse(
                            "Expected IN, LIKE, or BETWEEN after NOT".to_string(),
                        ));
                    }
                }
                Token::Like => {
                    self.advance();
                    let pattern = self.parse_additive_expr()?;
                    return Ok(Expr::Like {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        negated: false,
                    });
                }
                Token::Between => {
                    self.advance();
                    let low = self.parse_additive_expr()?;
                    self.expect(Token::And)?;
                    let high = self.parse_additive_expr()?;
                    return Ok(Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: false,
                    });
                }
                _ => break,
            };

            self.advance();
            let right = self.parse_additive_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_additive_expr(&mut self) -> Result<Expr, QueryError> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOperator::Add,
                Token::Minus => BinaryOperator::Subtract,
                Token::Concat => BinaryOperator::Concat,
                _ => break,
            };

            self.advance();
            let right = self.parse_multiplicative_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr, QueryError> {
        let mut left = self.parse_unary_expr()?;

        loop {
            let op = match self.peek() {
                Token::Star => BinaryOperator::Multiply,
                Token::Slash => BinaryOperator::Divide,
                Token::Percent => BinaryOperator::Modulo,
                _ => break,
            };

            self.advance();
            let right = self.parse_unary_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr, QueryError> {
        match self.peek() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            Token::Plus => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expr(),
        }
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, QueryError> {
        match self.peek() {
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(Token::RightParen)?;
                Ok(Expr::Nested(Box::new(expr)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Token::Integer(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(n)))
            }
            Token::Float(f) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(f)))
            }
            Token::String(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Token::Parameter(n) => {
                self.advance();
                Ok(Expr::Parameter(n))
            }
            Token::Case => self.parse_case_expr(),
            Token::Cast => self.parse_cast_expr(),
            Token::Exists => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let subquery = self.parse_select()?;
                self.expect(Token::RightParen)?;
                Ok(Expr::Exists(Box::new(subquery)))
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();

                // Check if it's a function call
                if self.check(Token::LeftParen) {
                    self.parse_function_call(name)
                } else if self.check(Token::Dot) {
                    // Qualified column reference
                    self.advance();
                    let column = self.parse_identifier()?;
                    Ok(Expr::Column(ColumnRef::qualified(name, column)))
                } else {
                    Ok(Expr::Column(ColumnRef::new(name)))
                }
            }
            token => {
                let span = self.current_span();
                let ctx = error_context(&self.source, &span);
                Err(QueryError::Parse(format!(
                    "Unexpected token in expression: {:?} at {}{}",
                    token, span, ctx
                )))
            }
        }
    }

    fn parse_function_call(&mut self, name: String) -> Result<Expr, QueryError> {
        self.expect(Token::LeftParen)?;

        let distinct = if self.check(Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        let args = if self.check(Token::RightParen) {
            Vec::new()
        } else if self.check(Token::Star) {
            self.advance();
            vec![Expr::Column(ColumnRef::new("*"))]
        } else {
            self.parse_expr_list()?
        };

        self.expect(Token::RightParen)?;

        // Check for FILTER clause
        let filter = if self.check(Token::Filter) {
            self.advance();
            self.expect(Token::LeftParen)?;
            self.expect(Token::Where)?;
            let expr = self.parse_expr()?;
            self.expect(Token::RightParen)?;
            Some(Box::new(expr))
        } else {
            None
        };

        // Check for OVER clause (window functions)
        let over = if self.check(Token::Over) {
            self.advance();
            Some(self.parse_window_spec()?)
        } else {
            None
        };

        Ok(Expr::Function(FunctionCall {
            name,
            args,
            distinct,
            filter,
            over,
        }))
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, QueryError> {
        self.expect(Token::LeftParen)?;

        let partition_by = if self.check(Token::Partition) {
            self.advance();
            self.expect(Token::By)?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };

        let order_by = if self.check(Token::Order) {
            self.advance();
            self.expect(Token::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        let frame =
            if self.check(Token::Rows) || self.check(Token::Range) || self.check(Token::Groups) {
                Some(self.parse_window_frame()?)
            } else {
                None
            };

        self.expect(Token::RightParen)?;

        Ok(WindowSpec {
            partition_by,
            order_by,
            frame,
        })
    }

    fn parse_window_frame(&mut self) -> Result<WindowFrame, QueryError> {
        let frame_type = match self.peek() {
            Token::Rows => {
                self.advance();
                FrameType::Rows
            }
            Token::Range => {
                self.advance();
                FrameType::Range
            }
            Token::Groups => {
                self.advance();
                FrameType::Groups
            }
            _ => {
                return Err(QueryError::Parse(
                    "Expected ROWS, RANGE, or GROUPS".to_string(),
                ))
            }
        };

        let start = self.parse_frame_bound()?;

        let end = if self.check(Token::And) {
            self.advance();
            Some(self.parse_frame_bound()?)
        } else {
            None
        };

        Ok(WindowFrame {
            frame_type,
            start,
            end,
        })
    }

    fn parse_frame_bound(&mut self) -> Result<FrameBound, QueryError> {
        if self.check(Token::Current) {
            self.advance();
            self.expect(Token::Row)?;
            Ok(FrameBound::CurrentRow)
        } else if self.check(Token::Unbounded) {
            self.advance();
            if self.check(Token::Preceding) {
                self.advance();
                Ok(FrameBound::Preceding(None))
            } else if self.check(Token::Following) {
                self.advance();
                Ok(FrameBound::Following(None))
            } else {
                Err(QueryError::Parse(
                    "Expected PRECEDING or FOLLOWING".to_string(),
                ))
            }
        } else if let Token::Integer(n) = self.peek() {
            let n = n as u64;
            self.advance();
            if self.check(Token::Preceding) {
                self.advance();
                Ok(FrameBound::Preceding(Some(n)))
            } else if self.check(Token::Following) {
                self.advance();
                Ok(FrameBound::Following(Some(n)))
            } else {
                Err(QueryError::Parse(
                    "Expected PRECEDING or FOLLOWING".to_string(),
                ))
            }
        } else {
            Err(QueryError::Parse("Expected frame bound".to_string()))
        }
    }

    fn parse_case_expr(&mut self) -> Result<Expr, QueryError> {
        self.expect(Token::Case)?;

        let operand = if !self.check(Token::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.check(Token::When) {
            self.advance();
            let condition = self.parse_expr()?;
            self.expect(Token::Then)?;
            let result = self.parse_expr()?;
            when_clauses.push((condition, result));
        }

        let else_clause = if self.check(Token::Else) {
            self.advance();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.expect(Token::End)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn parse_cast_expr(&mut self) -> Result<Expr, QueryError> {
        self.expect(Token::Cast)?;
        self.expect(Token::LeftParen)?;
        let expr = self.parse_expr()?;
        self.expect(Token::As)?;
        let data_type = self.parse_identifier()?;
        self.expect(Token::RightParen)?;

        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, QueryError> {
        let mut exprs = Vec::new();

        loop {
            exprs.push(self.parse_expr()?);
            if !self.check(Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(exprs)
    }

    fn parse_insert(&mut self) -> Result<InsertStatement, QueryError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let target = self.parse_identifier()?;

        let columns = if self.check(Token::LeftParen) {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect(Token::RightParen)?;
            cols
        } else {
            Vec::new()
        };

        self.expect(Token::Values)?;

        let mut values = Vec::new();
        loop {
            self.expect(Token::LeftParen)?;
            let row = self.parse_expr_list()?;
            self.expect(Token::RightParen)?;
            values.push(row);

            if !self.check(Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(InsertStatement {
            target,
            columns,
            values,
        })
    }

    fn parse_update(&mut self) -> Result<UpdateStatement, QueryError> {
        self.expect(Token::Update)?;

        let target = self.parse_identifier()?;

        self.expect(Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.parse_identifier()?;
            self.expect(Token::Equal)?;
            let value = self.parse_expr()?;
            assignments.push((column, value));

            if !self.check(Token::Comma) {
                break;
            }
            self.advance();
        }

        let where_clause = if self.check(Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(UpdateStatement {
            target,
            assignments,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteStatement, QueryError> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;

        let target = self.parse_identifier()?;

        let where_clause = if self.check(Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(DeleteStatement {
            target,
            where_clause,
        })
    }

    fn parse_create(&mut self) -> Result<Statement, QueryError> {
        self.expect(Token::Create)?;

        let or_replace = if self.check_identifier("OR") {
            self.advance();
            self.expect_identifier("REPLACE")?;
            true
        } else {
            false
        };

        if self.check(Token::Materialized) {
            self.advance();
            self.expect(Token::View)?;
            let name = self.parse_identifier()?;
            self.expect(Token::As)?;
            let query = self.parse_select()?;

            let materialized = Some(MaterializedOptions {
                refresh: if self.check(Token::Refresh) {
                    self.advance();
                    if self.check(Token::Incremental) {
                        self.advance();
                        RefreshType::Incremental
                    } else if self.check(Token::Complete) {
                        self.advance();
                        RefreshType::Complete
                    } else {
                        RefreshType::OnDemand
                    }
                } else {
                    RefreshType::OnDemand
                },
                refresh_interval_ms: None,
            });

            Ok(Statement::CreateView(CreateViewStatement {
                name,
                query,
                or_replace,
                materialized,
            }))
        } else if self.check(Token::View) {
            self.advance();
            let name = self.parse_identifier()?;
            self.expect(Token::As)?;
            let query = self.parse_select()?;

            Ok(Statement::CreateView(CreateViewStatement {
                name,
                query,
                or_replace,
                materialized: None,
            }))
        } else {
            Err(QueryError::Parse(
                "Expected VIEW or MATERIALIZED VIEW".to_string(),
            ))
        }
    }

    fn parse_drop(&mut self) -> Result<Statement, QueryError> {
        self.expect(Token::Drop)?;

        let _ = self.check(Token::Materialized) && {
            self.advance();
            true
        };

        self.expect(Token::View)?;

        let if_exists = if self.check_identifier("IF") {
            self.advance();
            if self.check(Token::Exists) {
                self.advance();
            } else {
                self.expect_identifier("EXISTS")?;
            }
            true
        } else {
            false
        };

        let name = self.parse_identifier()?;

        Ok(Statement::DropView(DropViewStatement { name, if_exists }))
    }

    fn parse_prepare(&mut self) -> Result<PrepareStatement, QueryError> {
        self.expect(Token::Prepare)?;
        let name = self.parse_identifier()?;
        self.expect(Token::As)?;
        let query = self.parse_statement()?;

        Ok(PrepareStatement {
            name,
            query: Box::new(query),
        })
    }

    fn parse_execute(&mut self) -> Result<ExecuteStatement, QueryError> {
        self.expect(Token::Execute)?;
        let name = self.parse_identifier()?;

        let params = if self.check(Token::LeftParen) {
            self.advance();
            let p = self.parse_expr_list()?;
            self.expect(Token::RightParen)?;
            p
        } else {
            Vec::new()
        };

        Ok(ExecuteStatement { name, params })
    }

    fn parse_identifier(&mut self) -> Result<String, QueryError> {
        match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(name)
            }
            token => {
                let span = self.current_span();
                let ctx = error_context(&self.source, &span);
                Err(QueryError::Parse(format!(
                    "Expected identifier, got {:?} at {}{}",
                    token, span, ctx
                )))
            }
        }
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, QueryError> {
        let mut idents = Vec::new();

        loop {
            idents.push(self.parse_identifier()?);
            if !self.check(Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(idents)
    }

    fn parse_integer(&mut self) -> Result<u64, QueryError> {
        match self.peek() {
            Token::Integer(n) => {
                let n = n as u64;
                self.advance();
                Ok(n)
            }
            token => {
                let span = self.current_span();
                let ctx = error_context(&self.source, &span);
                Err(QueryError::Parse(format!(
                    "Expected integer, got {:?} at {}{}",
                    token, span, ctx
                )))
            }
        }
    }

    fn peek(&self) -> Token {
        self.tokens
            .get(self.position)
            .map(|ts| ts.token.clone())
            .unwrap_or(Token::Eof)
    }

    fn current_span(&self) -> Span {
        self.tokens
            .get(self.position)
            .map(|ts| ts.span.clone())
            .unwrap_or(Span {
                offset: self.source.len(),
                line: 1,
                column: 1,
            })
    }

    fn advance(&mut self) -> Token {
        let token = self.peek();
        self.position += 1;
        token
    }

    fn check(&self, token: Token) -> bool {
        std::mem::discriminant(&self.peek()) == std::mem::discriminant(&token)
    }

    fn check_identifier(&self, name: &str) -> bool {
        matches!(self.peek(), Token::Identifier(ref n) if n.eq_ignore_ascii_case(name))
    }

    fn expect(&mut self, expected: Token) -> Result<(), QueryError> {
        if self.check(expected.clone()) {
            self.advance();
            Ok(())
        } else {
            let span = self.current_span();
            let ctx = error_context(&self.source, &span);
            Err(QueryError::Parse(format!(
                "Expected {:?}, got {:?} at {}{}",
                expected,
                self.peek(),
                span,
                ctx
            )))
        }
    }

    fn expect_identifier(&mut self, name: &str) -> Result<(), QueryError> {
        if self.check_identifier(name) {
            self.advance();
            Ok(())
        } else {
            let span = self.current_span();
            let ctx = error_context(&self.source, &span);
            Err(QueryError::Parse(format!(
                "Expected identifier '{}', got {:?} at {}{}",
                name,
                self.peek(),
                span,
                ctx
            )))
        }
    }

    fn is_keyword(&self) -> bool {
        self.peek().is_keyword()
    }
}

impl Default for QueryParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let parser = QueryParser::new();
        let stmt = parser.parse("SELECT * FROM users").unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(select.columns[0], SelectColumn::All));
            assert_eq!(select.from[0].pattern, "users");
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_alias() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT name AS n, age FROM users:* AS u")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 2);
            assert_eq!(select.from[0].alias, Some("u".to_string()));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_where() {
        let parser = QueryParser::new();
        let stmt = parser.parse("SELECT * FROM users WHERE age > 21").unwrap();

        if let Statement::Select(select) = stmt {
            assert!(select.where_clause.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_join() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse(
                "SELECT u.name, o.total FROM users:* AS u JOIN orders:* AS o ON o.user_id = u.id",
            )
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins.len(), 1);
            assert_eq!(select.joins[0].join_type, JoinType::Inner);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_group_by() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT category, COUNT(*) FROM products GROUP BY category")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.group_by.len(), 1);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_order_by() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users ORDER BY name ASC, age DESC")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.order_by.len(), 2);
            assert_eq!(select.order_by[0].direction, SortDirection::Asc);
            assert_eq!(select.order_by[1].direction, SortDirection::Desc);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_create_view() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("CREATE VIEW top_users AS SELECT * FROM users WHERE score > 100")
            .unwrap();

        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.name, "top_users");
            assert!(cv.materialized.is_none());
        } else {
            panic!("Expected CREATE VIEW statement");
        }
    }

    #[test]
    fn test_expression_parsing() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT a + b * c, (d - e) / f FROM t")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 2);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_distinct() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT DISTINCT status FROM hash:user:*")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(select.distinct);
            assert_eq!(select.columns.len(), 1);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_limit_offset() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys LIMIT 10 OFFSET 5")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.limit, Some(10));
            assert_eq!(select.offset, Some(5));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_where_like() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys WHERE key LIKE 'user:*'")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(select.where_clause, Some(Expr::Like { .. })));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_where_between() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys WHERE ttl BETWEEN 100 AND 3600")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::Between { negated: false, .. })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_where_in() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys WHERE type IN ('hash', 'string', 'list')")
            .unwrap();

        if let Statement::Select(select) = stmt {
            if let Some(Expr::In { list, negated, .. }) = &select.where_clause {
                assert!(!negated);
                assert_eq!(list.len(), 3);
            } else {
                panic!("Expected IN expression");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_where_not_in() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys WHERE type NOT IN ('stream')")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::In { negated: true, .. })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_where_is_null() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys WHERE ttl IS NULL")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::IsNull { negated: false, .. })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_where_is_not_null() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys WHERE ttl IS NOT NULL")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::IsNull { negated: true, .. })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_aggregate_functions() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT COUNT(*), SUM(total), AVG(total), MIN(price), MAX(price) FROM orders:*")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 5);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_group_by_having() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT category, COUNT(*) cnt FROM orders:* GROUP BY category HAVING cnt > 5")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.group_by.len(), 1);
            assert!(select.having.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_left_join() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT u.name FROM users:* AS u LEFT JOIN orders:* AS o ON o.user_id = u.id")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins.len(), 1);
            assert_eq!(select.joins[0].join_type, JoinType::Left);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_insert_statement() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("INSERT INTO users (name, age) VALUES ('Alice', 30)")
            .unwrap();

        if let Statement::Insert(insert) = stmt {
            assert_eq!(insert.target, "users");
            assert_eq!(insert.columns, vec!["name", "age"]);
            assert_eq!(insert.values.len(), 1);
            assert_eq!(insert.values[0].len(), 2);
        } else {
            panic!("Expected INSERT statement");
        }
    }

    #[test]
    fn test_update_statement() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("UPDATE users SET name = 'Bob', age = 25 WHERE id = 1")
            .unwrap();

        if let Statement::Update(update) = stmt {
            assert_eq!(update.target, "users");
            assert_eq!(update.assignments.len(), 2);
            assert!(update.where_clause.is_some());
        } else {
            panic!("Expected UPDATE statement");
        }
    }

    #[test]
    fn test_delete_statement() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("DELETE FROM keys WHERE key LIKE 'temp:*'")
            .unwrap();

        if let Statement::Delete(delete) = stmt {
            assert_eq!(delete.target, "keys");
            assert!(delete.where_clause.is_some());
        } else {
            panic!("Expected DELETE statement");
        }
    }

    #[test]
    fn test_explain_statement() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("EXPLAIN SELECT * FROM users WHERE age > 21")
            .unwrap();

        assert!(matches!(stmt, Statement::Explain(_)));
    }

    #[test]
    fn test_prepare_execute() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("PREPARE get_user AS SELECT * FROM users WHERE id = $1")
            .unwrap();

        if let Statement::Prepare(prep) = stmt {
            assert_eq!(prep.name, "get_user");
        } else {
            panic!("Expected PREPARE statement");
        }

        let stmt = parser.parse("EXECUTE get_user (42)").unwrap();

        if let Statement::Execute(exec) = stmt {
            assert_eq!(exec.name, "get_user");
            assert_eq!(exec.params.len(), 1);
        } else {
            panic!("Expected EXECUTE statement");
        }
    }

    #[test]
    fn test_case_expression() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS tier FROM users")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 1);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_create_materialized_view() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) FROM orders")
            .unwrap();

        if let Statement::CreateView(cv) = stmt {
            assert_eq!(cv.name, "stats");
            assert!(cv.materialized.is_some());
        } else {
            panic!("Expected CREATE VIEW statement");
        }
    }

    #[test]
    fn test_drop_view() {
        let parser = QueryParser::new();
        let stmt = parser.parse("DROP VIEW my_view").unwrap();

        if let Statement::DropView(dv) = stmt {
            assert_eq!(dv.name, "my_view");
            assert!(!dv.if_exists);
        } else {
            panic!("Expected DROP VIEW statement");
        }
    }

    #[test]
    fn test_drop_view_if_exists() {
        let parser = QueryParser::new();
        let stmt = parser.parse("DROP VIEW IF EXISTS my_view").unwrap();

        if let Statement::DropView(dv) = stmt {
            assert_eq!(dv.name, "my_view");
            assert!(dv.if_exists);
        } else {
            panic!("Expected DROP VIEW statement");
        }
    }

    #[test]
    fn test_compound_where() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM keys WHERE type = 'hash' AND ttl > 3600")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_order_by_with_nulls() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users ORDER BY age DESC NULLS LAST")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.order_by.len(), 1);
            assert_eq!(select.order_by[0].direction, SortDirection::Desc);
            assert_eq!(select.order_by[0].nulls, Some(NullsOrder::Last));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_function_with_distinct() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT COUNT(DISTINCT category) FROM products")
            .unwrap();

        if let Statement::Select(select) = stmt {
            if let SelectColumn::Expr {
                expr: Expr::Function(func),
                ..
            } = &select.columns[0]
            {
                assert!(func.distinct);
                assert_eq!(func.name, "COUNT");
            } else {
                panic!("Expected function expression");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_nested_expressions() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE (age > 18 AND status = 'active') OR role = 'admin'")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::BinaryOp {
                    op: BinaryOperator::Or,
                    ..
                })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parse_error_on_invalid_sql() {
        let parser = QueryParser::new();
        assert!(parser.parse("XYZZY UNKNOWN SYNTAX").is_err());
        assert!(parser.parse("SELECT FROM").is_err());
        assert!(parser.parse("").is_err());
    }

    #[test]
    fn test_sql_comments() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users -- get all users")
            .unwrap();

        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_cast_expression() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT CAST(age AS INTEGER) FROM users")
            .unwrap();

        if let Statement::Select(select) = stmt {
            if let SelectColumn::Expr {
                expr: Expr::Cast { data_type, .. },
                ..
            } = &select.columns[0]
            {
                assert_eq!(data_type, "INTEGER");
            } else {
                panic!("Expected CAST expression");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_string_concatenation() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT first_name || ' ' || last_name AS full_name FROM users")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 1);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parameter_placeholders() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE id = $1 AND status = $2")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(select.where_clause.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    // ───── Enhanced Error Message Tests ─────

    #[test]
    fn test_error_includes_line_and_column() {
        let parser = QueryParser::new();
        let err = parser.parse("SELCT * FROM users").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line"), "Error should include line: {}", msg);
        assert!(
            msg.contains("column"),
            "Error should include column: {}",
            msg
        );
    }

    #[test]
    fn test_error_suggests_select_for_selct() {
        let parser = QueryParser::new();
        let err = parser.parse("SELCT * FROM users").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("SELECT"),
            "Should suggest SELECT for SELCT: {}",
            msg
        );
        assert!(
            msg.contains("did you mean"),
            "Should say 'did you mean': {}",
            msg
        );
    }

    #[test]
    fn test_error_suggests_where_for_whre() {
        let parser = QueryParser::new();
        // Use a query where WHRE cannot be parsed as an alias (e.g., after WHERE clause position)
        let err = parser.parse("WHRE * FROM users").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("WHERE"),
            "Should suggest WHERE for WHRE: {}",
            msg
        );
    }

    #[test]
    fn test_error_shows_context_snippet() {
        let parser = QueryParser::new();
        let err = parser.parse("SELCT * FROM users").unwrap_err();
        let msg = err.to_string();
        // The error should show the source line with a caret pointer
        assert!(msg.contains("SELCT"), "Should show source line: {}", msg);
        assert!(msg.contains('^'), "Should show caret pointer: {}", msg);
    }

    #[test]
    fn test_error_no_suggestion_for_valid_identifier() {
        let parser = QueryParser::new();
        let err = parser.parse("my_variable_name * FROM users").unwrap_err();
        let msg = err.to_string();
        // Should not suggest a keyword for a long identifier that's clearly not a typo
        assert!(
            !msg.contains("did you mean"),
            "Should not suggest for unrelated identifiers: {}",
            msg
        );
    }

    // ───── IN Subquery Tests ─────

    #[test]
    fn test_in_subquery() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::InSubquery { negated: false, .. })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_not_in_subquery() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::InSubquery { negated: true, .. })
            ));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_exists_subquery() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse(
                "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
            )
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(select.where_clause, Some(Expr::Exists(_))));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_in_list_still_works() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE status IN ('active', 'pending')")
            .unwrap();

        if let Statement::Select(select) = stmt {
            if let Some(Expr::In { list, negated, .. }) = &select.where_clause {
                assert!(!negated);
                assert_eq!(list.len(), 2);
            } else {
                panic!("Expected IN list expression");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    // ───── All operations already supported (COUNT, GROUP BY, ORDER BY, LIMIT/OFFSET, DISTINCT, BETWEEN) ─────

    #[test]
    fn test_count_aggregation() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT COUNT(*) as total FROM orders:*")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 1);
            if let SelectColumn::Expr {
                expr: Expr::Function(func),
                alias,
            } = &select.columns[0]
            {
                assert_eq!(func.name, "COUNT");
                assert_eq!(alias, &Some("total".to_string()));
            } else {
                panic!("Expected COUNT function");
            }
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_group_by_with_multiple_aggregates() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse(
                "SELECT category, COUNT(*), SUM(price), AVG(price) FROM products GROUP BY category",
            )
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 4);
            assert_eq!(select.group_by.len(), 1);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_order_by_asc_desc() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users ORDER BY created_at DESC, name ASC")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.order_by.len(), 2);
            assert_eq!(select.order_by[0].direction, SortDirection::Desc);
            assert_eq!(select.order_by[1].direction, SortDirection::Asc);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_limit_offset_pagination() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM products LIMIT 20 OFFSET 40")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.limit, Some(20));
            assert_eq!(select.offset, Some(40));
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_distinct_keyword() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT DISTINCT category FROM products:*")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(select.distinct);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_between_range_query() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM products WHERE price BETWEEN 10 AND 100")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::Between { negated: false, .. })
            ));
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_not_between() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM products WHERE price NOT BETWEEN 10 AND 100")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert!(matches!(
                select.where_clause,
                Some(Expr::Between { negated: true, .. })
            ));
        } else {
            panic!("Expected SELECT");
        }
    }

    // ───── JOIN Tests ─────

    #[test]
    fn test_inner_join_explicit() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users:* AS u INNER JOIN orders:* AS o ON o.user_id = u.id")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins.len(), 1);
            assert_eq!(select.joins[0].join_type, JoinType::Inner);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_cross_join() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM colors CROSS JOIN sizes")
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins.len(), 1);
            assert_eq!(select.joins[0].join_type, JoinType::Cross);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_multiple_joins() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse(
                "SELECT * FROM users:* AS u \
                 JOIN orders:* AS o ON o.user_id = u.id \
                 LEFT JOIN products:* AS p ON p.id = o.product_id",
            )
            .unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins.len(), 2);
            assert_eq!(select.joins[0].join_type, JoinType::Inner);
            assert_eq!(select.joins[1].join_type, JoinType::Left);
        } else {
            panic!("Expected SELECT");
        }
    }

    // ───── EXPLAIN Tests ─────

    #[test]
    fn test_explain_select() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("EXPLAIN SELECT * FROM users WHERE age > 21")
            .unwrap();

        if let Statement::Explain(inner) = stmt {
            assert!(matches!(*inner, Statement::Select(_)));
        } else {
            panic!("Expected EXPLAIN");
        }
    }

    #[test]
    fn test_explain_with_join() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("EXPLAIN SELECT * FROM users:* AS u JOIN orders:* AS o ON o.user_id = u.id")
            .unwrap();

        assert!(matches!(stmt, Statement::Explain(_)));
    }
}
