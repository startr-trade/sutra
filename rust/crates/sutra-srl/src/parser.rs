//! Recursive-descent parser for the `.srl` DSL, in the hand-written style of
//! `sutra_feel::parser`. The parser consumes the token stream from [`crate::lexer`] and, for each
//! embedded FEEL region, slices the *raw* character substring and compiles it with
//! `sutra_feel::expressions::parse`.
//!
//! ## Delimiting the embedded FEEL
//!
//! FEEL itself has `if / then / else`, so the section keyword `then` that ends a `when` condition
//! is the **first `then` token at paren/bracket depth 0 outside any string literal** after `when`
//! (strings are atomic tokens, so this is a pure token-depth scan). Consequently a FEEL
//! `if a then b else c` used inside a condition **must be parenthesised** by the author
//! (`when (if a then b else c) …`) so its inner `then` sits at depth ≥ 1. The `end` keyword
//! likewise terminates a rule only at depth 0 — the action parser consumes balanced parens, so
//! after each fully-parsed action the depth is 0 and the next `report` / `set` / `end` token is
//! unambiguous.
//!
//! Inside `report(…)` / `set(…)`, the argument list is split on **top-level commas** (depth 1
//! relative to the action's own parens); each argument substring is compiled as its own FEEL
//! expression.

use sutra_feel::positions::FeelSourcePositions;
use sutra_feel::FeelExpr;

use crate::ast::{Action, Rule, Ruleset};
use crate::codes;
use crate::error::SrlError;
use crate::lexer::{SrlLexer, Token, TokenKind};

/// Parse `.srl` source into a [`Ruleset`]. Fail-closed: the first error aborts the parse.
///
/// This is the entry point the deploy-time lint calls for fail-closed validation.
pub fn parse(src: &str) -> Result<Ruleset, SrlError> {
    let chars: Vec<char> = src.chars().collect();
    let positions = FeelSourcePositions::new(src, "srl:inline");
    let (tokens, masked) = SrlLexer::new(&chars, &positions).tokenize()?;
    SrlParser::new(tokens, &masked, &positions).parse_ruleset()
}

pub(crate) struct SrlParser<'a> {
    tokens: Vec<Token>,
    /// The comment-masked character source (see [`crate::lexer`]); embedded-FEEL substrings are
    /// sliced from this so comments never reach the FEEL parser. Token offsets index it directly.
    src: &'a [char],
    positions: &'a FeelSourcePositions,
    pos: usize,
}

impl<'a> SrlParser<'a> {
    pub(crate) fn new(
        tokens: Vec<Token>,
        src: &'a [char],
        positions: &'a FeelSourcePositions,
    ) -> Self {
        SrlParser {
            tokens,
            src,
            positions,
            pos: 0,
        }
    }

    pub(crate) fn parse_ruleset(mut self) -> Result<Ruleset, SrlError> {
        let mut rules = Vec::new();
        let mut decl_index = 0usize;
        while self.peek().kind != TokenKind::Eof {
            rules.push(self.parse_rule(decl_index)?);
            decl_index += 1;
        }
        Ok(Ruleset { rules })
    }

    fn parse_rule(&mut self, decl_index: usize) -> Result<Rule, SrlError> {
        self.expect(TokenKind::Rule, "rule")?;
        let name = self.expect_string("a rule name")?;

        let mut salience = 0i64;
        let mut activation_group = None;
        loop {
            match self.peek().kind {
                TokenKind::Salience => {
                    self.advance();
                    salience = self.parse_signed_int()?;
                }
                TokenKind::ActivationGroup => {
                    self.advance();
                    activation_group = Some(self.expect_string("an activation-group name")?);
                }
                _ => break,
            }
        }

        let when_tok = self.expect(TokenKind::When, "when")?;
        let cond_start = when_tok.end;
        // Locate the section `then` at depth 0 and slice the raw condition text.
        let then_idx = self.find_section_then()?;
        let cond_end = self.tokens[then_idx].start;
        let condition = self.feel_parse(cond_start, cond_end)?;
        self.pos = then_idx;
        self.expect(TokenKind::Then, "then")?;

        let actions = self.parse_actions()?;
        Ok(Rule {
            name,
            salience,
            activation_group,
            condition,
            condition_span: (cond_start, cond_end),
            actions,
            decl_index,
        })
    }

    /// Scan forward from the current position for the first `then` token at bracket depth 0,
    /// returning its token index. Errors (`missing then`) if the ruleset ends first.
    fn find_section_then(&self) -> Result<usize, SrlError> {
        let mut depth = 0i32;
        let mut i = self.pos;
        loop {
            let t = &self.tokens[i];
            match t.kind {
                TokenKind::LParen | TokenKind::LBracket => depth += 1,
                TokenKind::RParen | TokenKind::RBracket => depth -= 1,
                TokenKind::Then if depth == 0 => return Ok(i),
                TokenKind::Eof => {
                    return Err(SrlError::at(
                        codes::SRL_SYNTAX_ERROR,
                        "missing 'then' — a rule's `when` condition must be closed by a \
                         top-level `then` (parenthesise any FEEL `if/then/else` in the condition)",
                        t.start,
                        self.positions,
                    ));
                }
                _ => {}
            }
            i += 1;
        }
    }

    fn parse_actions(&mut self) -> Result<Vec<Action>, SrlError> {
        let mut actions = Vec::new();
        loop {
            match self.peek().kind {
                TokenKind::End => {
                    self.advance();
                    return Ok(actions);
                }
                TokenKind::Eof => {
                    let t = self.peek();
                    return Err(SrlError::at(
                        codes::SRL_SYNTAX_ERROR,
                        "missing 'end' — a rule's `then` block must be closed by `end`",
                        t.start,
                        self.positions,
                    ));
                }
                TokenKind::Report => actions.push(self.parse_report()?),
                TokenKind::Set => actions.push(self.parse_set()?),
                TokenKind::Insert | TokenKind::Retract => {
                    let t = self.peek().clone();
                    return Err(SrlError::at(
                        codes::SRL_RESERVED_VERB,
                        "insert/retract are reserved for a stateful rules engine and are not \
                         available in .srl",
                        t.start,
                        self.positions,
                    )
                    .with_construct(t.text));
                }
                _ => {
                    let t = self.peek().clone();
                    return Err(SrlError::at(
                        codes::SRL_UNKNOWN_VERB,
                        format!(
                            "unknown action verb '{}' — only `report` and `set` are available \
                             in .srl",
                            t.text
                        ),
                        t.start,
                        self.positions,
                    )
                    .with_construct(t.text));
                }
            }
        }
    }

    fn parse_report(&mut self) -> Result<Action, SrlError> {
        let verb = self.expect(TokenKind::Report, "report")?;
        let open = self.expect(TokenKind::LParen, "(")?;
        let ranges = self.split_paren_args(open.end)?;
        if ranges.len() != 3 {
            return Err(SrlError::at(
                codes::SRL_BAD_ARITY,
                format!(
                    "report(...) requires exactly 3 arguments (code, path, message), got {}",
                    ranges.len()
                ),
                verb.start,
                self.positions,
            )
            .with_construct("report"));
        }
        let code = self.feel_parse(ranges[0].0, ranges[0].1)?;
        let path = self.feel_parse(ranges[1].0, ranges[1].1)?;
        let message = self.feel_parse(ranges[2].0, ranges[2].1)?;
        self.expect(TokenKind::Semicolon, ";")?;
        Ok(Action::Report {
            code: Box::new(code),
            path: Box::new(path),
            message: Box::new(message),
            arg_spans: [ranges[0], ranges[1], ranges[2]],
        })
    }

    fn parse_set(&mut self) -> Result<Action, SrlError> {
        let verb = self.expect(TokenKind::Set, "set")?;
        let open = self.expect(TokenKind::LParen, "(")?;
        let ranges = self.split_paren_args(open.end)?;
        if ranges.len() != 2 {
            return Err(SrlError::at(
                codes::SRL_BAD_ARITY,
                format!(
                    "set(...) requires exactly 2 arguments (target, expr), got {}",
                    ranges.len()
                ),
                verb.start,
                self.positions,
            )
            .with_construct("set"));
        }
        let target = self.ident_in_range(ranges[0])?;
        let expr = self.feel_parse(ranges[1].0, ranges[1].1)?;
        self.expect(TokenKind::Semicolon, ";")?;
        Ok(Action::Set {
            target,
            expr,
            expr_span: ranges[1],
        })
    }

    /// The first `set` argument must be a bare identifier (the assignment target). Validate the
    /// raw range is exactly one `Ident` token and return its text.
    fn ident_in_range(&self, range: (usize, usize)) -> Result<String, SrlError> {
        let inner: Vec<&Token> = self
            .tokens
            .iter()
            .filter(|t| t.start >= range.0 && t.end <= range.1 && t.kind != TokenKind::Eof)
            .collect();
        if inner.len() == 1 && inner[0].kind == TokenKind::Ident {
            Ok(inner[0].text.clone())
        } else {
            Err(SrlError::at(
                codes::SRL_SYNTAX_ERROR,
                "the first argument of set(...) must be a bare identifier (the assignment target)",
                range.0,
                self.positions,
            ))
        }
    }

    /// Split an action's parenthesised argument list on top-level commas. `body_start` is the
    /// character offset immediately after the opening `(`. Advances `self.pos` past the matching
    /// `)`. Returns each argument's raw `(start, end)` character span (empty list body yields one
    /// empty-range "argument").
    fn split_paren_args(&mut self, body_start: usize) -> Result<Vec<(usize, usize)>, SrlError> {
        let mut depth = 1i32;
        let mut commas: Vec<(usize, usize)> = Vec::new();
        let close_start;
        loop {
            let t = self.peek().clone();
            match t.kind {
                TokenKind::LParen | TokenKind::LBracket => {
                    depth += 1;
                    self.advance();
                }
                TokenKind::RParen | TokenKind::RBracket => {
                    depth -= 1;
                    self.advance();
                    if depth == 0 {
                        close_start = t.start;
                        break;
                    }
                }
                TokenKind::Comma if depth == 1 => {
                    commas.push((t.start, t.end));
                    self.advance();
                }
                TokenKind::Eof => {
                    return Err(SrlError::at(
                        codes::SRL_UNTERMINATED_PAREN,
                        "unterminated '(' in action argument list",
                        body_start,
                        self.positions,
                    ));
                }
                _ => {
                    self.advance();
                }
            }
        }
        let mut ranges = Vec::with_capacity(commas.len() + 1);
        let mut seg_start = body_start;
        for (cs, ce) in &commas {
            ranges.push((seg_start, *cs));
            seg_start = *ce;
        }
        ranges.push((seg_start, close_start));
        Ok(ranges)
    }

    /// Compile a raw `.srl` character span `[start, end)` as a FEEL expression, wrapping any FEEL
    /// diagnostic with the `.srl` line/column of the offending sub-expression (FEEL offset
    /// composed onto the region origin).
    fn feel_parse(&self, start: usize, end: usize) -> Result<FeelExpr, SrlError> {
        let text: String = self.src[start..end].iter().collect();
        sutra_feel::expressions::parse(&text).map_err(|e| {
            let abs = start + e.offset.unwrap_or(0);
            SrlError::at(
                codes::SRL_FEEL_PARSE_ERROR,
                format!("invalid FEEL expression: [{}] {}", e.code, e.message),
                abs,
                self.positions,
            )
            .with_construct(text.trim().to_string())
        })
    }

    // ----- signed integer / string helpers -----

    fn parse_signed_int(&mut self) -> Result<i64, SrlError> {
        let mut negative = false;
        if self.peek().kind == TokenKind::Other {
            match self.peek().text.as_str() {
                "-" => {
                    negative = true;
                    self.advance();
                }
                "+" => {
                    self.advance();
                }
                _ => {}
            }
        }
        let tok = self.expect(TokenKind::Int, "an integer")?;
        let magnitude: i64 = tok.text.parse().map_err(|_| {
            SrlError::at(
                codes::SRL_SYNTAX_ERROR,
                format!(
                    "salience value '{}' is out of range for a 64-bit integer",
                    tok.text
                ),
                tok.start,
                self.positions,
            )
        })?;
        Ok(if negative { -magnitude } else { magnitude })
    }

    fn expect_string(&mut self, what: &str) -> Result<String, SrlError> {
        let tok = self.peek().clone();
        if tok.kind != TokenKind::Str {
            return Err(self.unexpected(&tok, what));
        }
        if tok.quote != '"' {
            return Err(SrlError::at(
                codes::SRL_SYNTAX_ERROR,
                format!("expected {what} as a double-quoted string"),
                tok.start,
                self.positions,
            )
            .with_construct(tok.text));
        }
        self.advance();
        Ok(tok.str_value)
    }

    // ----- token cursor helpers -----

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, kind: TokenKind, what: &str) -> Result<Token, SrlError> {
        let tok = self.peek().clone();
        if tok.kind != kind {
            return Err(self.unexpected(&tok, what));
        }
        Ok(self.advance())
    }

    fn unexpected(&self, tok: &Token, what: &str) -> SrlError {
        let seen = if tok.kind == TokenKind::Eof {
            "end of input".to_string()
        } else {
            format!("'{}'", tok.text)
        };
        SrlError::at(
            codes::SRL_SYNTAX_ERROR,
            format!("expected {what} but found {seen}"),
            tok.start,
            self.positions,
        )
        .with_construct(tok.text.clone())
    }
}
