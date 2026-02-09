//! FerriteQL Lexer
//!
//! Tokenizes SQL-like query strings into tokens with source location tracking.

use std::iter::Peekable;
use std::str::Chars;

use crate::query::QueryError;

/// Source location within a query string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Span {
    /// Byte offset from the start of input.
    pub offset: usize,
    /// 1-based line number.
    pub line: usize,
    /// 1-based column number.
    pub column: usize,
}

impl std::fmt::Display for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

/// A token paired with its source location.
#[derive(Clone, Debug)]
pub struct TokenWithSpan {
    /// The token kind.
    pub token: Token,
    /// Source location where this token starts.
    pub span: Span,
}

/// Token types
#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    // Keywords
    /// `SELECT` keyword
    Select,
    /// `FROM` keyword
    From,
    /// `WHERE` keyword
    Where,
    /// `AND` keyword
    And,
    /// `OR` keyword
    Or,
    /// `NOT` keyword
    Not,
    /// `AS` keyword for aliasing
    As,
    /// `JOIN` keyword
    Join,
    /// `INNER` keyword for inner joins
    Inner,
    /// `LEFT` keyword for left joins
    Left,
    /// `RIGHT` keyword for right joins
    Right,
    /// `FULL` keyword for full outer joins
    Full,
    /// `CROSS` keyword for cross joins
    Cross,
    /// `ON` keyword for join conditions
    On,
    /// `GROUP` keyword
    Group,
    /// `BY` keyword
    By,
    /// `HAVING` keyword for group filters
    Having,
    /// `ORDER` keyword
    Order,
    /// `ASC` keyword for ascending order
    Asc,
    /// `DESC` keyword for descending order
    Desc,
    /// `LIMIT` keyword for result count
    Limit,
    /// `OFFSET` keyword for result offset
    Offset,
    /// `DISTINCT` keyword for unique results
    Distinct,
    /// `ALL` keyword
    All,
    /// `NULL` keyword
    Null,
    /// `IS` keyword for null checks
    Is,
    /// `IN` keyword for set membership
    In,
    /// `BETWEEN` keyword for range checks
    Between,
    /// `LIKE` keyword for pattern matching
    Like,
    /// `EXISTS` keyword for subquery existence
    Exists,
    /// `CASE` keyword for conditional expressions
    Case,
    /// `WHEN` keyword in case expressions
    When,
    /// `THEN` keyword in case expressions
    Then,
    /// `ELSE` keyword in case expressions
    Else,
    /// `END` keyword to close case expressions
    End,
    /// `CAST` keyword for type casting
    Cast,
    /// `TRUE` boolean literal
    True,
    /// `FALSE` boolean literal
    False,
    /// `CREATE` keyword
    Create,
    /// `DROP` keyword
    Drop,
    /// `VIEW` keyword
    View,
    /// `MATERIALIZED` keyword for materialized views
    Materialized,
    /// `REFRESH` keyword for refreshing views
    Refresh,
    /// `INCREMENTAL` keyword for incremental refresh
    Incremental,
    /// `COMPLETE` keyword for complete refresh
    Complete,
    /// `PREPARE` keyword for prepared statements
    Prepare,
    /// `EXECUTE` keyword for executing prepared statements
    Execute,
    /// `INSERT` keyword
    Insert,
    /// `INTO` keyword
    Into,
    /// `VALUES` keyword for insert values
    Values,
    /// `UPDATE` keyword
    Update,
    /// `SET` keyword for update assignments
    Set,
    /// `DELETE` keyword
    Delete,
    /// `EXPLAIN` keyword for query plans
    Explain,
    /// `NULLS` keyword for null ordering
    Nulls,
    /// `FIRST` keyword for null ordering
    First,
    /// `LAST` keyword for null ordering
    Last,
    /// `OVER` keyword for window functions
    Over,
    /// `PARTITION` keyword for window partitioning
    Partition,
    /// `ROWS` keyword for window frame type
    Rows,
    /// `RANGE` keyword for window frame type
    Range,
    /// `GROUPS` keyword for window frame type
    Groups,
    /// `UNBOUNDED` keyword for window frame bounds
    Unbounded,
    /// `PRECEDING` keyword for window frame bounds
    Preceding,
    /// `FOLLOWING` keyword for window frame bounds
    Following,
    /// `CURRENT` keyword for window frame bounds
    Current,
    /// `ROW` keyword for window frame bounds
    Row,
    /// `FILTER` keyword for aggregate filtering
    Filter,

    // Identifiers and literals
    /// An identifier (column name, table name, etc.)
    Identifier(String),
    /// A string literal
    String(String),
    /// An integer literal
    Integer(i64),
    /// A floating-point literal
    Float(f64),
    /// A positional parameter (`$1`, `$2`, etc.)
    Parameter(usize),

    // Operators
    /// `+` addition operator
    Plus,
    /// `-` subtraction operator
    Minus,
    /// `*` multiplication operator or wildcard
    Star,
    /// `/` division operator
    Slash,
    /// `%` modulo operator
    Percent,
    /// `=` equality operator
    Equal,
    /// `!=` or `<>` inequality operator
    NotEqual,
    /// `<` less-than operator
    LessThan,
    /// `<=` less-than-or-equal operator
    LessThanOrEqual,
    /// `>` greater-than operator
    GreaterThan,
    /// `>=` greater-than-or-equal operator
    GreaterThanOrEqual,
    /// `||` string concatenation operator
    Concat,

    // Punctuation
    /// `(` left parenthesis
    LeftParen,
    /// `)` right parenthesis
    RightParen,
    /// `,` comma separator
    Comma,
    /// `.` dot separator
    Dot,
    /// `;` statement terminator
    Semicolon,
    /// `:` colon
    Colon,

    // Special
    /// End of input
    Eof,
}

impl Token {
    /// Check if this token is a keyword
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            Token::Select
                | Token::From
                | Token::Where
                | Token::And
                | Token::Or
                | Token::Not
                | Token::As
                | Token::Join
                | Token::Inner
                | Token::Left
                | Token::Right
                | Token::Full
                | Token::Cross
                | Token::On
                | Token::Group
                | Token::By
                | Token::Having
                | Token::Order
                | Token::Asc
                | Token::Desc
                | Token::Limit
                | Token::Offset
                | Token::Distinct
                | Token::All
                | Token::Null
                | Token::Is
                | Token::In
                | Token::Between
                | Token::Like
                | Token::Exists
                | Token::Case
                | Token::When
                | Token::Then
                | Token::Else
                | Token::End
                | Token::Cast
                | Token::True
                | Token::False
                | Token::Create
                | Token::Drop
                | Token::View
                | Token::Materialized
                | Token::Refresh
                | Token::Incremental
                | Token::Complete
                | Token::Prepare
                | Token::Execute
                | Token::Insert
                | Token::Into
                | Token::Values
                | Token::Update
                | Token::Set
                | Token::Delete
                | Token::Explain
        )
    }
}

/// SQL Lexer with source location tracking.
pub struct Lexer<'a> {
    source: &'a str,
    input: Peekable<Chars<'a>>,
    position: usize,
    line: usize,
    column: usize,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer
    pub fn new(input: &'a str) -> Self {
        Self {
            source: input,
            input: input.chars().peekable(),
            position: 0,
            line: 1,
            column: 1,
        }
    }

    /// Returns the original source string.
    pub fn source(&self) -> &str {
        self.source
    }

    /// Returns the current source span.
    fn current_span(&self) -> Span {
        Span {
            offset: self.position,
            line: self.line,
            column: self.column,
        }
    }

    /// Get the next token paired with its source span.
    pub fn next_token_with_span(&mut self) -> Result<TokenWithSpan, QueryError> {
        self.skip_whitespace();
        let span = self.current_span();
        let token = self.next_token_inner()?;
        Ok(TokenWithSpan { token, span })
    }

    /// Get the next token
    pub fn next_token(&mut self) -> Result<Token, QueryError> {
        self.skip_whitespace();
        self.next_token_inner()
    }

    fn next_token_inner(&mut self) -> Result<Token, QueryError> {
        match self.peek() {
            None => Ok(Token::Eof),
            Some(c) => match c {
                // Single-character tokens
                '(' => {
                    self.advance();
                    Ok(Token::LeftParen)
                }
                ')' => {
                    self.advance();
                    Ok(Token::RightParen)
                }
                ',' => {
                    self.advance();
                    Ok(Token::Comma)
                }
                ';' => {
                    self.advance();
                    Ok(Token::Semicolon)
                }
                '+' => {
                    self.advance();
                    Ok(Token::Plus)
                }
                '-' => {
                    self.advance();
                    if self.peek() == Some('-') {
                        // Comment
                        self.skip_line_comment();
                        self.next_token()
                    } else {
                        Ok(Token::Minus)
                    }
                }
                '*' => {
                    self.advance();
                    Ok(Token::Star)
                }
                '/' => {
                    self.advance();
                    if self.peek() == Some('*') {
                        // Block comment
                        self.skip_block_comment()?;
                        self.next_token()
                    } else {
                        Ok(Token::Slash)
                    }
                }
                '%' => {
                    self.advance();
                    Ok(Token::Percent)
                }
                '=' => {
                    self.advance();
                    Ok(Token::Equal)
                }
                '<' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Ok(Token::LessThanOrEqual)
                    } else if self.peek() == Some('>') {
                        self.advance();
                        Ok(Token::NotEqual)
                    } else {
                        Ok(Token::LessThan)
                    }
                }
                '>' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Ok(Token::GreaterThanOrEqual)
                    } else {
                        Ok(Token::GreaterThan)
                    }
                }
                '!' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Ok(Token::NotEqual)
                    } else {
                        let span = self.current_span();
                        Err(QueryError::Syntax(format!(
                            "Unexpected character '!' at {}",
                            span
                        )))
                    }
                }
                '|' => {
                    self.advance();
                    if self.peek() == Some('|') {
                        self.advance();
                        Ok(Token::Concat)
                    } else {
                        let span = self.current_span();
                        Err(QueryError::Syntax(format!(
                            "Unexpected character '|' at {}",
                            span
                        )))
                    }
                }
                '.' => {
                    self.advance();
                    Ok(Token::Dot)
                }
                ':' => {
                    self.advance();
                    Ok(Token::Colon)
                }
                '$' => self.scan_parameter(),
                '\'' => self.scan_string(),
                '"' => self.scan_quoted_identifier(),
                c if c.is_ascii_digit() => self.scan_number(),
                c if c.is_alphabetic() || c == '_' => self.scan_identifier(),
                c => {
                    let span = self.current_span();
                    Err(QueryError::Syntax(format!(
                        "Unexpected character '{}' at {}",
                        c, span
                    )))
                }
            },
        }
    }

    /// Tokenize the entire input into tokens with source spans.
    pub fn tokenize_with_spans(&mut self) -> Result<Vec<TokenWithSpan>, QueryError> {
        let mut tokens = Vec::new();
        loop {
            let ts = self.next_token_with_span()?;
            let is_eof = ts.token == Token::Eof;
            tokens.push(ts);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>, QueryError> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }

    fn peek(&mut self) -> Option<char> {
        self.input.peek().copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.input.next();
        if let Some(c) = ch {
            self.position += c.len_utf8();
            if c == '\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
        }
        ch
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn skip_line_comment(&mut self) {
        while let Some(c) = self.advance() {
            if c == '\n' {
                break;
            }
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), QueryError> {
        self.advance(); // Skip '*'
        let mut depth = 1;

        while depth > 0 {
            match self.advance() {
                Some('*') if self.peek() == Some('/') => {
                    self.advance();
                    depth -= 1;
                }
                Some('/') if self.peek() == Some('*') => {
                    self.advance();
                    depth += 1;
                }
                None => {
                    return Err(QueryError::Syntax("Unterminated block comment".to_string()));
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn scan_string(&mut self) -> Result<Token, QueryError> {
        self.advance(); // Skip opening quote
        let mut value = String::new();

        loop {
            match self.advance() {
                Some('\'') => {
                    // Check for escaped quote
                    if self.peek() == Some('\'') {
                        self.advance();
                        value.push('\'');
                    } else {
                        break;
                    }
                }
                Some(c) => value.push(c),
                None => {
                    return Err(QueryError::Syntax(
                        "Unterminated string literal".to_string(),
                    ));
                }
            }
        }

        Ok(Token::String(value))
    }

    fn scan_quoted_identifier(&mut self) -> Result<Token, QueryError> {
        self.advance(); // Skip opening quote
        let mut value = String::new();

        loop {
            match self.advance() {
                Some('"') => {
                    // Check for escaped quote
                    if self.peek() == Some('"') {
                        self.advance();
                        value.push('"');
                    } else {
                        break;
                    }
                }
                Some(c) => value.push(c),
                None => {
                    return Err(QueryError::Syntax(
                        "Unterminated quoted identifier".to_string(),
                    ));
                }
            }
        }

        Ok(Token::Identifier(value))
    }

    fn scan_number(&mut self) -> Result<Token, QueryError> {
        let mut value = String::new();
        let mut is_float = false;

        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.advance();
                value.push(c);
            } else if c == '.' && !is_float {
                is_float = true;
                self.advance();
                value.push(c);
            } else if (c == 'e' || c == 'E') && !value.is_empty() {
                is_float = true;
                self.advance();
                value.push(c);
                if let Some(sign) = self.peek() {
                    if sign == '+' || sign == '-' {
                        self.advance();
                        value.push(sign);
                    }
                }
            } else {
                break;
            }
        }

        if is_float {
            value
                .parse::<f64>()
                .map(Token::Float)
                .map_err(|_| QueryError::Syntax(format!("Invalid float: {}", value)))
        } else {
            value
                .parse::<i64>()
                .map(Token::Integer)
                .map_err(|_| QueryError::Syntax(format!("Invalid integer: {}", value)))
        }
    }

    fn scan_identifier(&mut self) -> Result<Token, QueryError> {
        let mut value = String::new();

        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' || c == ':' || c == '*' {
                self.advance();
                value.push(c);
            } else {
                break;
            }
        }

        // Check for keywords
        let token = match value.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "AS" => Token::As,
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "CROSS" => Token::Cross,
            "ON" => Token::On,
            "GROUP" => Token::Group,
            "BY" => Token::By,
            "HAVING" => Token::Having,
            "ORDER" => Token::Order,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "DISTINCT" => Token::Distinct,
            "ALL" => Token::All,
            "NULL" => Token::Null,
            "IS" => Token::Is,
            "IN" => Token::In,
            "BETWEEN" => Token::Between,
            "LIKE" => Token::Like,
            "EXISTS" => Token::Exists,
            "CASE" => Token::Case,
            "WHEN" => Token::When,
            "THEN" => Token::Then,
            "ELSE" => Token::Else,
            "END" => Token::End,
            "CAST" => Token::Cast,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "CREATE" => Token::Create,
            "DROP" => Token::Drop,
            "VIEW" => Token::View,
            "MATERIALIZED" => Token::Materialized,
            "REFRESH" => Token::Refresh,
            "INCREMENTAL" => Token::Incremental,
            "COMPLETE" => Token::Complete,
            "PREPARE" => Token::Prepare,
            "EXECUTE" => Token::Execute,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "EXPLAIN" => Token::Explain,
            "NULLS" => Token::Nulls,
            "FIRST" => Token::First,
            "LAST" => Token::Last,
            "OVER" => Token::Over,
            "PARTITION" => Token::Partition,
            "ROWS" => Token::Rows,
            "RANGE" => Token::Range,
            "GROUPS" => Token::Groups,
            "UNBOUNDED" => Token::Unbounded,
            "PRECEDING" => Token::Preceding,
            "FOLLOWING" => Token::Following,
            "CURRENT" => Token::Current,
            "ROW" => Token::Row,
            "FILTER" => Token::Filter,
            _ => Token::Identifier(value),
        };

        Ok(token)
    }

    fn scan_parameter(&mut self) -> Result<Token, QueryError> {
        self.advance(); // Skip '$'
        let mut value = String::new();

        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.advance();
                value.push(c);
            } else {
                break;
            }
        }

        if value.is_empty() {
            return Err(QueryError::Syntax(
                "Expected parameter number after '$'".to_string(),
            ));
        }

        value
            .parse::<usize>()
            .map(Token::Parameter)
            .map_err(|_| QueryError::Syntax(format!("Invalid parameter number: {}", value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Star,
                Token::From,
                Token::Identifier("users".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_select_with_where() {
        let mut lexer = Lexer::new("SELECT name FROM users:* WHERE id = 1");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Identifier("name".to_string()),
                Token::From,
                Token::Identifier("users:*".to_string()),
                Token::Where,
                Token::Identifier("id".to_string()),
                Token::Equal,
                Token::Integer(1),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_string_literal() {
        let mut lexer = Lexer::new("'hello world'");
        let token = lexer.next_token().unwrap();
        assert_eq!(token, Token::String("hello world".to_string()));
    }

    #[test]
    fn test_escaped_string() {
        let mut lexer = Lexer::new("'it''s a test'");
        let token = lexer.next_token().unwrap();
        assert_eq!(token, Token::String("it's a test".to_string()));
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("42 3.14 1e10");
        assert_eq!(lexer.next_token().unwrap(), Token::Integer(42));
        assert_eq!(lexer.next_token().unwrap(), Token::Float(3.14));
        assert_eq!(lexer.next_token().unwrap(), Token::Float(1e10));
    }

    #[test]
    fn test_operators() {
        let mut lexer = Lexer::new("+ - * / = != < <= > >= ||");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::Equal,
                Token::NotEqual,
                Token::LessThan,
                Token::LessThanOrEqual,
                Token::GreaterThan,
                Token::GreaterThanOrEqual,
                Token::Concat,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_parameters() {
        let mut lexer = Lexer::new("$1 $2 $10");
        assert_eq!(lexer.next_token().unwrap(), Token::Parameter(1));
        assert_eq!(lexer.next_token().unwrap(), Token::Parameter(2));
        assert_eq!(lexer.next_token().unwrap(), Token::Parameter(10));
    }

    #[test]
    fn test_comments() {
        let mut lexer = Lexer::new("SELECT -- this is a comment\n*");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::Select, Token::Star, Token::Eof]);
    }

    #[test]
    fn test_block_comments() {
        let mut lexer = Lexer::new("SELECT /* block comment */ *");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::Select, Token::Star, Token::Eof]);
    }

    #[test]
    fn test_span_tracking_single_line() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize_with_spans().unwrap();

        assert_eq!(tokens[0].span.line, 1);
        assert_eq!(tokens[0].span.column, 1);
        assert_eq!(tokens[0].token, Token::Select);

        // Star should be after "SELECT "
        assert_eq!(tokens[1].span.line, 1);
        assert_eq!(tokens[1].span.column, 8);
        assert_eq!(tokens[1].token, Token::Star);
    }

    #[test]
    fn test_span_tracking_multiline() {
        let mut lexer = Lexer::new("SELECT *\nFROM users\nWHERE id = 1");
        let tokens = lexer.tokenize_with_spans().unwrap();

        // FROM should be on line 2
        let from_tok = tokens.iter().find(|t| t.token == Token::From).unwrap();
        assert_eq!(from_tok.span.line, 2);
        assert_eq!(from_tok.span.column, 1);

        // WHERE should be on line 3
        let where_tok = tokens.iter().find(|t| t.token == Token::Where).unwrap();
        assert_eq!(where_tok.span.line, 3);
        assert_eq!(where_tok.span.column, 1);
    }

    #[test]
    fn test_span_display() {
        let span = Span {
            offset: 0,
            line: 3,
            column: 5,
        };
        assert_eq!(span.to_string(), "line 3, column 5");
    }
}
