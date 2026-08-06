//! Recursive-descent parser for the FEEL subset. Precedence (low → high):
//!
//! 1. `or`
//! 2. `and`
//! 3. Comparison (`= != < <= > >=`)
//! 4. Additive (`+ -`)
//! 5. Multiplicative (`* /`)
//! 6. Unary (`not`, unary `-`)
//! 7. Primary (literal, path, call, parenthesised, if-then-else)

use crate::ast::{ArithOp, CompareOp, FeelExpr, LogicalOp};
use crate::codes;
use crate::error::FeelError;
use crate::lexer::{Token, TokenKind};
use crate::positions::FeelSourcePositions;
use crate::value::FeelValue;

/// A parsed argument list: the argument expressions, a parallel list of optional argument names
/// (`Some` for `name: value`), and the closing `)` end offset.
type CallArgs = (Vec<FeelExpr>, Vec<Option<String>>, usize);

/// Whether a token kind begins a comparison unary test (`< e`, `<= e`, `> e`, `>= e`, `= e`,
/// `!= e`) in an `in` right-hand side — also the leading token of a comparison-operator range
/// value at primary position (`(< 10)`; see [`FeelParser::parse_primary`]'s `OpenRange` arm).
fn is_compare_op(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Lt
            | TokenKind::Le
            | TokenKind::Gt
            | TokenKind::Ge
            | TokenKind::Eq
            | TokenKind::Neq
    )
}

/// Whether a token kind can begin a FEEL primary/unary expression — used to tell a genuine
/// postfix filter's `[` (always followed by a predicate) from the alternate ISO interval-closing
/// spelling `[a..b[` (followed by whatever ends the enclosing construct: `Eof`, `,`, `)`, …), so
/// [`FeelParser::parse_postfix`] can leave the latter for the endpoint's caller to consume.
fn can_start_expr(kind: TokenKind) -> bool {
    is_compare_op(kind)
        || matches!(
            kind,
            TokenKind::Number
                | TokenKind::Str
                | TokenKind::Bool
                | TokenKind::Null
                | TokenKind::Temporal
                | TokenKind::LParen
                | TokenKind::RBracket
                | TokenKind::LBracket
                | TokenKind::LBrace
                | TokenKind::If
                | TokenKind::Some
                | TokenKind::Every
                | TokenKind::For
                | TokenKind::Ident
                | TokenKind::Not
                | TokenKind::Minus
        )
}

pub(crate) struct FeelParser<'a> {
    tokens: Vec<Token>,
    positions: &'a FeelSourcePositions,
    pos: usize,
    /// Set while parsing an interval endpoint (the bound just before a closing `)`/`]`/`[`) — see
    /// [`Self::parse_range_endpoint`]. Disambiguates the alternate ISO closing spelling
    /// (`[a..b[`) from a genuine postfix filter starting at the same `[`.
    in_range_endpoint: bool,
}

impl<'a> FeelParser<'a> {
    pub fn new(tokens: Vec<Token>, positions: &'a FeelSourcePositions) -> Self {
        FeelParser {
            tokens,
            positions,
            pos: 0,
            in_range_endpoint: false,
        }
    }

    pub fn parse(mut self) -> Result<FeelExpr, FeelError> {
        let expr = self.parse_or()?;
        self.expect(TokenKind::Eof)?;
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<FeelExpr, FeelError> {
        let mut left = self.parse_and()?;
        while self.peek().kind == TokenKind::Or {
            self.advance();
            let right = self.parse_and()?;
            left = FeelExpr::BoolOp {
                start: left.start(),
                end: right.end(),
                left: Box::new(left),
                op: LogicalOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<FeelExpr, FeelError> {
        let mut left = self.parse_comparison()?;
        while self.peek().kind == TokenKind::And {
            self.advance();
            let right = self.parse_comparison()?;
            left = FeelExpr::BoolOp {
                start: left.start(),
                end: right.end(),
                left: Box::new(left),
                op: LogicalOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<FeelExpr, FeelError> {
        let left = self.parse_additive()?;
        // `expr instance of <type>` — a type test (§10.3.4.6). `instance`/`of` stay ordinary
        // identifiers in the lexer; they are only special in this position.
        if self.peek().kind == TokenKind::Ident
            && self.peek().text == "instance"
            && self.peek_next().map(|t| t.text.as_str()) == Some("of")
        {
            self.advance(); // instance
            self.advance(); // of
            let type_shape = self.parse_type_expr()?;
            return Ok(FeelExpr::InstanceOf {
                start: left.start(),
                end: self.prev_end(),
                expr: Box::new(left),
                type_shape,
            });
        }
        // `value between low and high` — desugar to `value >= low and value <= high`.
        if self.peek().kind == TokenKind::Ident && self.peek().text == "between" {
            self.advance();
            let low = self.parse_additive()?;
            self.expect(TokenKind::And)?;
            let high = self.parse_additive()?;
            let (start, end) = (left.start(), high.end());
            let ge = FeelExpr::Compare {
                start,
                end: low.end(),
                left: Box::new(left.clone()),
                op: CompareOp::Ge,
                right: Box::new(low),
            };
            let le = FeelExpr::Compare {
                start,
                end,
                left: Box::new(left),
                op: CompareOp::Le,
                right: Box::new(high),
            };
            return Ok(FeelExpr::BoolOp {
                start,
                end,
                left: Box::new(ge),
                op: LogicalOp::And,
                right: Box::new(le),
            });
        }
        // `value in <positive unary tests>` — membership against a value, list, interval, or a
        // list of comparison/interval tests (`x in < 10`, `x in (1, 2, >= 5)`, `x in [[1..3],[5..7]]`).
        if self.peek().kind == TokenKind::In {
            self.advance();
            return self.parse_in(left);
        }
        let op = match self.peek().kind {
            TokenKind::Eq => Some(CompareOp::Eq),
            TokenKind::Neq => Some(CompareOp::Neq),
            TokenKind::Lt => Some(CompareOp::Lt),
            TokenKind::Le => Some(CompareOp::Le),
            TokenKind::Gt => Some(CompareOp::Gt),
            TokenKind::Ge => Some(CompareOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive()?;
            return Ok(FeelExpr::Compare {
                start: left.start(),
                end: right.end(),
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<FeelExpr, FeelError> {
        let mut left = self.parse_multiplicative()?;
        while self.peek().kind == TokenKind::Plus || self.peek().kind == TokenKind::Minus {
            let op = if self.peek().kind == TokenKind::Plus {
                ArithOp::Plus
            } else {
                ArithOp::Minus
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = FeelExpr::Arith {
                start: left.start(),
                end: right.end(),
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<FeelExpr, FeelError> {
        let mut left = self.parse_power()?;
        while self.peek().kind == TokenKind::Times || self.peek().kind == TokenKind::Div {
            let op = if self.peek().kind == TokenKind::Times {
                ArithOp::Times
            } else {
                ArithOp::Div
            };
            self.advance();
            let right = self.parse_power()?;
            left = FeelExpr::Arith {
                start: left.start(),
                end: right.end(),
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Exponentiation `**` — binds tighter than `* /`. The right operand is a full unary
    /// expression so `base ** -exp` parses (the exponent may be negated).
    fn parse_power(&mut self) -> Result<FeelExpr, FeelError> {
        let mut left = self.parse_unary()?;
        while self.peek().kind == TokenKind::Pow {
            self.advance();
            let right = self.parse_unary()?;
            left = FeelExpr::Arith {
                start: left.start(),
                end: right.end(),
                left: Box::new(left),
                op: ArithOp::Pow,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<FeelExpr, FeelError> {
        if self.peek().kind == TokenKind::Not {
            let tok_start = self.peek().start;
            self.advance();
            // 'not' may appear as the prefix operator OR as a function call 'not(x)';
            // both forms parse the same — argument is the next primary.
            let arg = if self.peek().kind == TokenKind::LParen {
                self.advance();
                let inner = self.parse_or()?;
                self.expect(TokenKind::RParen)?;
                inner
            } else {
                self.parse_unary()?
            };
            return Ok(FeelExpr::Not {
                start: tok_start,
                end: arg.end(),
                arg: Box::new(arg),
            });
        }
        if self.peek().kind == TokenKind::Minus {
            let tok_start = self.peek().start;
            self.advance();
            let arg = self.parse_unary()?;
            // Unary negation is its OWN construct, not `0 - arg` (DMN-TCK
            // 0099-arithmetic-negation`#003`/`#004`: `-@"P1D"` must negate the duration directly
            // — desugaring to a literal-zero subtraction would route it through binary
            // number-minus-duration arithmetic, which FEEL doesn't define at all).
            return Ok(FeelExpr::Negate {
                start: tok_start,
                end: arg.end(),
                arg: Box::new(arg),
            });
        }
        self.parse_postfix()
    }

    /// A primary followed by zero or more postfix operators: filter/index `[…]`, field access
    /// `.field` (the latter for bases the leading-dot path parser doesn't cover — filter results,
    /// list/context literals, parenthesised expressions), and invocation `(args)` of the
    /// already-parsed expression (a parenthesised function literal, a call chain returning
    /// another function, or any other value — invoking a non-function is a clean runtime type
    /// error, not a parse failure).
    fn parse_postfix(&mut self) -> Result<FeelExpr, FeelError> {
        let mut expr = self.parse_primary()?;
        loop {
            match self.peek().kind {
                TokenKind::LBracket => {
                    // Inside an interval endpoint, a `[` that can't itself start a predicate
                    // expression is the alternate exclusive-upper closing bracket (`[a..b[`), not
                    // a filter — leave it for the caller's `expect_interval_close`.
                    if self.in_range_endpoint
                        && !self.peek_next().is_some_and(|t| can_start_expr(t.kind))
                    {
                        break;
                    }
                    self.advance();
                    let predicate = self.parse_or()?;
                    let close = self.expect(TokenKind::RBracket)?;
                    expr = FeelExpr::Filter {
                        start: expr.start(),
                        end: close.end,
                        source: Box::new(expr),
                        predicate: Box::new(predicate),
                    };
                }
                TokenKind::Dot => {
                    self.advance();
                    let field = self.expect(TokenKind::Ident)?;
                    expr = FeelExpr::FieldAccess {
                        start: expr.start(),
                        end: field.end,
                        base: Box::new(expr),
                        field: field.text,
                    };
                }
                TokenKind::LParen => {
                    let (args, arg_names, end) = self.parse_call_args()?;
                    expr = FeelExpr::Invoke {
                        start: expr.start(),
                        end,
                        callee: Box::new(expr),
                        args,
                        arg_names,
                    };
                }
                _ => break,
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<FeelExpr, FeelError> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Number
            | TokenKind::Str
            | TokenKind::Bool
            | TokenKind::Null
            | TokenKind::Temporal => {
                self.advance();
                Ok(FeelExpr::Literal {
                    start: tok.start,
                    end: tok.end,
                    value: tok.value,
                })
            }
            TokenKind::LParen => {
                let open = self.advance();
                let expr = self.parse_or()?;
                // A parenthesised interval `(a..b)` / `(a..b]` — an exclusive lower bound.
                if self.peek().kind == TokenKind::DotDot {
                    self.advance();
                    let to = self.parse_range_endpoint()?;
                    let (to_inclusive, end) = self.expect_interval_close()?;
                    return Ok(FeelExpr::Range {
                        start: open.start,
                        end,
                        from: Box::new(expr),
                        to: Box::new(to),
                        from_inclusive: false,
                        to_inclusive,
                        bracketed: true,
                    });
                }
                self.expect(TokenKind::RParen)?;
                Ok(expr)
            }
            // The alternate ISO "French" exclusive-lower-bound spelling `]a..b]` / `]a..b)` —
            // `]` means the same as `(` here (DMN 1.4 §10.3.1.2); mirrors the `LParen` branch
            // above exactly, just entered via the other opening spelling.
            TokenKind::RBracket => {
                let open = self.advance();
                let expr = self.parse_or()?;
                self.expect(TokenKind::DotDot)?;
                let to = self.parse_range_endpoint()?;
                let (to_inclusive, end) = self.expect_interval_close()?;
                Ok(FeelExpr::Range {
                    start: open.start,
                    end,
                    from: Box::new(expr),
                    to: Box::new(to),
                    from_inclusive: false,
                    to_inclusive,
                    bracketed: true,
                })
            }
            TokenKind::If => self.parse_if_then_else(),
            TokenKind::Some | TokenKind::Every => self.parse_quantifier(),
            TokenKind::For => self.parse_for(),
            TokenKind::LBracket => self.parse_list_literal(),
            TokenKind::LBrace => self.parse_context_literal(),
            TokenKind::Ident => self.parse_path_or_call(),
            // A comparison-operator range value used as a first-class expression (`(< 10)`,
            // `(>= 5)`, `(=10)`, `(!=10)`) — DMN 1.4 §10.3.2.11. Only reachable at primary
            // position (an operand is expected here, never mid-expression), so this can never
            // fire inside an ordinary infix comparison (`a < b`) — purely additive.
            k if is_compare_op(k) => {
                let op_tok = self.advance();
                let op = match op_tok.kind {
                    TokenKind::Lt => CompareOp::Lt,
                    TokenKind::Le => CompareOp::Le,
                    TokenKind::Gt => CompareOp::Gt,
                    TokenKind::Ge => CompareOp::Ge,
                    TokenKind::Eq => CompareOp::Eq,
                    TokenKind::Neq => CompareOp::Neq,
                    _ => unreachable!("is_compare_op gates this arm"),
                };
                let bound = self.parse_additive()?;
                Ok(FeelExpr::OpenRange {
                    start: op_tok.start,
                    end: bound.end(),
                    op,
                    bound: Box::new(bound),
                })
            }
            _ => Err(FeelError::at(
                codes::FEEL_SYNTAX_UNEXPECTED_TOKEN,
                format!("Unexpected token '{}'", tok.text),
                tok.start,
                Some(self.positions.location_for(tok.start)),
            )),
        }
    }

    fn parse_if_then_else(&mut self) -> Result<FeelExpr, FeelError> {
        let if_start = self.peek().start;
        self.advance();
        let cond = self.parse_or()?;
        self.expect(TokenKind::Then)?;
        let then = self.parse_or()?;
        self.expect(TokenKind::Else)?;
        let otherwise = self.parse_or()?;
        Ok(FeelExpr::IfThenElse {
            start: if_start,
            end: otherwise.end(),
            cond: Box::new(cond),
            then: Box::new(then),
            otherwise: Box::new(otherwise),
        })
    }

    /// FEEL list literal `[a, b, c]` (also the empty list `[]`). Each element is a full
    /// expression. Interval/range forms (`[1..10]`) are not handled here — a `..` after the first
    /// element surfaces as an unexpected token.
    fn parse_list_literal(&mut self) -> Result<FeelExpr, FeelError> {
        let open = self.expect(TokenKind::LBracket)?;
        if self.peek().kind == TokenKind::RBracket {
            let close = self.advance();
            return Ok(FeelExpr::ListLit {
                start: open.start,
                end: close.end,
                items: Vec::new(),
            });
        }
        let first = self.parse_or()?;
        // A bracketed interval `[a..b]` / `[a..b)` — an inclusive lower bound.
        if self.peek().kind == TokenKind::DotDot {
            self.advance();
            let to = self.parse_range_endpoint()?;
            let (to_inclusive, end) = self.expect_interval_close()?;
            return Ok(FeelExpr::Range {
                start: open.start,
                end,
                from: Box::new(first),
                to: Box::new(to),
                from_inclusive: true,
                to_inclusive,
                bracketed: true,
            });
        }
        let mut items = vec![first];
        while self.peek().kind == TokenKind::Comma {
            self.advance();
            items.push(self.parse_or()?);
        }
        let close = self.expect(TokenKind::RBracket)?;
        Ok(FeelExpr::ListLit {
            start: open.start,
            end: close.end,
            items,
        })
    }

    /// The iteration source of a `for`/`some`/`every` clause — a full expression, optionally an
    /// unbracketed inclusive range `a..b`.
    fn parse_iterable(&mut self) -> Result<FeelExpr, FeelError> {
        let first = self.parse_or()?;
        if self.peek().kind == TokenKind::DotDot {
            self.advance();
            let to = self.parse_or()?;
            return Ok(FeelExpr::Range {
                start: first.start(),
                end: to.end(),
                from: Box::new(first),
                to: Box::new(to),
                from_inclusive: true,
                to_inclusive: true,
                bracketed: false,
            });
        }
        Ok(first)
    }

    /// Consume the closing bracket of an interval: `]` (inclusive upper) or `)`/`[` (exclusive
    /// upper — `[` is the alternate ISO "French" spelling, `[a..b[`); returns
    /// `(upper_inclusive, end_offset)`.
    fn expect_interval_close(&mut self) -> Result<(bool, usize), FeelError> {
        match self.peek().kind {
            TokenKind::RBracket => Ok((true, self.advance().end)),
            TokenKind::RParen | TokenKind::LBracket => Ok((false, self.advance().end)),
            _ => {
                let tok = self.peek();
                Err(FeelError::at(
                    codes::FEEL_SYNTAX_UNEXPECTED_TOKEN,
                    format!("Expected ']' or ')' to close a range, got '{}'", tok.text),
                    tok.start,
                    Some(self.positions.location_for(tok.start)),
                ))
            }
        }
    }

    /// Parse an interval endpoint: the same grammar as a full expression, except a trailing `[`
    /// is never chased as a postfix filter (that would consume the alternate ISO exclusive-upper
    /// closing bracket, `[a..b[`, as the start of an empty filter instead — see
    /// [`Self::parse_postfix`]'s `LBracket` arm). Used for every interval upper bound.
    fn parse_range_endpoint(&mut self) -> Result<FeelExpr, FeelError> {
        let prev = self.in_range_endpoint;
        self.in_range_endpoint = true;
        let result = self.parse_or();
        self.in_range_endpoint = prev;
        result
    }

    /// The right-hand side of `value in …`: parse the positive unary tests and OR the resulting
    /// boolean checks (`value in < 10` → `value < 10`; `value in (1, >= 5)` → `value = 1 or value
    /// >= 5`; `value in [[1..3],[5..7]]` → containment in either interval).
    fn parse_in(&mut self, value: FeelExpr) -> Result<FeelExpr, FeelError> {
        let start = value.start();
        let tests = self.parse_positive_unary_tests(&value)?;
        // OR the checks (three-valued). An empty test set is vacuously false.
        let end = tests.last().map(FeelExpr::end).unwrap_or(start);
        let mut it = tests.into_iter();
        let first = it.next().unwrap_or(FeelExpr::Literal {
            start,
            end,
            value: FeelValue::Boolean(false),
        });
        Ok(it.fold(first, |acc, t| FeelExpr::BoolOp {
            start,
            end,
            left: Box::new(acc),
            op: LogicalOp::Or,
            right: Box::new(t),
        }))
    }

    /// A single unary-test group `( … )` / `[ … ]` (which may be one interval test or a
    /// comma-separated test list) or a bare single test.
    fn parse_positive_unary_tests(&mut self, value: &FeelExpr) -> Result<Vec<FeelExpr>, FeelError> {
        if !matches!(self.peek().kind, TokenKind::LParen | TokenKind::LBracket) {
            return Ok(vec![self.parse_unary_test(value)?]);
        }
        let open = self.advance();
        let open_inclusive = open.kind == TokenKind::LBracket;
        // A group beginning with a comparison operator is a test list (intervals never start with
        // an operator).
        if is_compare_op(self.peek().kind) {
            let mut tests = vec![self.parse_unary_test(value)?];
            while self.peek().kind == TokenKind::Comma {
                self.advance();
                tests.push(self.parse_unary_test(value)?);
            }
            self.expect_interval_close()?;
            return Ok(tests);
        }
        // Otherwise the first token is a value: an interval lower bound or the first list element.
        let first = self.parse_or()?;
        if self.peek().kind == TokenKind::DotDot {
            self.advance();
            let to = self.parse_range_endpoint()?;
            let (to_inclusive, end) = self.expect_interval_close()?;
            let range = FeelExpr::Range {
                start: open.start,
                end,
                from: Box::new(first),
                to: Box::new(to),
                from_inclusive: open_inclusive,
                to_inclusive,
                bracketed: true,
            };
            return Ok(vec![self.in_test(value, range)]);
        }
        // A `[`-bracketed group with no top-level `..` is EITHER a genuine FEEL list VALUE (DMN
        // 1.4 §10.3.2.13: `value in [x, y, z]` as ONE membership test over the whole list) OR
        // this engine's own long-standing `[`-bracketed disjunction-of-tests idiom (`x in
        // [<10,>=20]`, `x in [[1..3],[4..7]]` — each element independently a range/comparison
        // TEST, OR'd). The two coincide for a flat list of plain scalars (`a in [1,2,3]` ≡ `a=1
        // or a=2 or a=3`), which is why the list-value bug (DMN-TCK 0072-feel-in
        // list_001/list_011_a: `[1,2,3] in [[1,2,3,4],[1,2,3]]` must ask "does `[1,2,3]` equal
        // EITHER inner list", not "is `[1,2,3]` a member of either inner list's own elements")
        // stayed hidden — it only diverges when an element is itself a list. Disambiguated by
        // element SHAPE: collect every comma-separated element as a plain value (`parse_or`); if
        // any of them is itself a range/comparison-operator test, keep the OR'd-disjunction
        // reading (unchanged; range-containment semantics only make sense per-element, never as
        // "equals this Range value"); otherwise every element is a plain value (a scalar or a
        // nested list) and the whole group collapses to ONE `ListLit`, tested once. `(`-grouped
        // comma lists always keep the OR'd-disjunction reading (the DMN decision-table unary-test
        // idiom — `x in (1, >= 5)` — never a list-VALUE position).
        if open.kind == TokenKind::LBracket {
            let mut items = vec![first];
            while self.peek().kind == TokenKind::Comma {
                self.advance();
                items.push(self.parse_or()?);
            }
            let close = self.expect(TokenKind::RBracket)?;
            let is_test_disjunction = items
                .iter()
                .any(|it| matches!(it, FeelExpr::Range { .. } | FeelExpr::OpenRange { .. }));
            if is_test_disjunction {
                return Ok(items
                    .into_iter()
                    .map(|it| self.in_test(value, it))
                    .collect());
            }
            let list = FeelExpr::ListLit {
                start: open.start,
                end: close.end,
                items,
            };
            return Ok(vec![self.in_test(value, list)]);
        }
        let mut tests = vec![self.in_test(value, first)];
        while self.peek().kind == TokenKind::Comma {
            self.advance();
            tests.push(self.parse_unary_test(value)?);
        }
        self.expect_interval_close()?;
        Ok(tests)
    }

    /// One positive unary test producing a boolean check against `value`: a leading comparison
    /// operator (`< e`, `>= e`, `= e`, `!= e`) → a comparison; anything else → a membership /
    /// containment / equality check via [`FeelExpr::In`].
    fn parse_unary_test(&mut self, value: &FeelExpr) -> Result<FeelExpr, FeelError> {
        let op = match self.peek().kind {
            TokenKind::Lt => Some(CompareOp::Lt),
            TokenKind::Le => Some(CompareOp::Le),
            TokenKind::Gt => Some(CompareOp::Gt),
            TokenKind::Ge => Some(CompareOp::Ge),
            TokenKind::Eq => Some(CompareOp::Eq),
            TokenKind::Neq => Some(CompareOp::Neq),
            _ => None,
        };
        if let Some(op) = op {
            self.advance();
            let operand = self.parse_additive()?;
            return Ok(FeelExpr::Compare {
                start: value.start(),
                end: operand.end(),
                left: Box::new(value.clone()),
                op,
                right: Box::new(operand),
            });
        }
        let test = self.parse_or()?;
        Ok(self.in_test(value, test))
    }

    /// Build `value in test` (membership / containment / equality) as a boolean node.
    fn in_test(&self, value: &FeelExpr, test: FeelExpr) -> FeelExpr {
        FeelExpr::In {
            start: value.start(),
            end: test.end(),
            value: Box::new(value.clone()),
            test: Box::new(test),
        }
    }

    /// `some|every <var> in <source> satisfies <condition>`.
    fn parse_quantifier(&mut self) -> Result<FeelExpr, FeelError> {
        let head = self.advance(); // some | every
        let every = head.kind == TokenKind::Every;
        let var = self.expect(TokenKind::Ident)?.text;
        self.expect(TokenKind::In)?;
        let source = self.parse_iterable()?;
        self.expect(TokenKind::Satisfies)?;
        let condition = self.parse_or()?;
        Ok(FeelExpr::Quantifier {
            start: head.start,
            end: condition.end(),
            every,
            var,
            source: Box::new(source),
            condition: Box::new(condition),
        })
    }

    /// `for <var> in <source>[, <var> in <source>]* return <body>`.
    fn parse_for(&mut self) -> Result<FeelExpr, FeelError> {
        let head = self.advance(); // for
        let mut bindings = Vec::new();
        loop {
            let var = self.expect(TokenKind::Ident)?.text;
            self.expect(TokenKind::In)?;
            let source = self.parse_iterable()?;
            bindings.push((var, source));
            if self.peek().kind == TokenKind::Comma {
                self.advance();
                continue;
            }
            break;
        }
        self.expect(TokenKind::Return)?;
        let body = self.parse_or()?;
        Ok(FeelExpr::For {
            start: head.start,
            end: body.end(),
            bindings,
            body: Box::new(body),
        })
    }

    /// FEEL inline context `{ key: expr, ... }` (also the empty context `{}`). A key is a name
    /// (`Ident`) or a string literal (`"key"`); the value is a full expression.
    fn parse_context_literal(&mut self) -> Result<FeelExpr, FeelError> {
        let open = self.expect(TokenKind::LBrace)?;
        let mut entries = Vec::new();
        if self.peek().kind != TokenKind::RBrace {
            entries.push(self.parse_context_entry()?);
            while self.peek().kind == TokenKind::Comma {
                self.advance();
                entries.push(self.parse_context_entry()?);
            }
        }
        let close = self.expect(TokenKind::RBrace)?;
        Ok(FeelExpr::ContextLit {
            start: open.start,
            end: close.end,
            entries,
        })
    }

    fn parse_context_entry(&mut self) -> Result<(String, FeelExpr), FeelError> {
        let key_tok = self.peek().clone();
        let key = match key_tok.kind {
            // A string-literal key carries the parsed (unquoted) text in its value.
            TokenKind::Str => {
                self.advance();
                match key_tok.value {
                    FeelValue::String(s) => s,
                    _ => key_tok.text,
                }
            }
            // A context-entry key that isn't a quoted string is FEEL's own permissive "Name"
            // grammar (§10.3.1.2), distinct from a general expression: it accepts a RAW run of
            // source text up to the next top-level `:`, including characters that are ordinary
            // OPERATORS everywhere else (DMN-TCK 0057-feel-context#004/#005 — `{foo bar: "foo"}`'s
            // key is the "names with spaces" run `"foo bar"`; `{foo+bar: "foo"}`'s is literally
            // `"foo+bar"`, NOT the arithmetic expression `foo + bar`). Reconstructed from the
            // ORIGINAL source span (`positions.slice`) rather than token text joined with a fixed
            // separator, so both the spaced and tight shapes round-trip correctly — a bare single
            // identifier (the overwhelmingly common case) is unaffected either way.
            TokenKind::Ident => {
                let start = key_tok.start;
                let mut end = key_tok.end;
                self.advance();
                while !matches!(self.peek().kind, TokenKind::Colon | TokenKind::Eof) {
                    end = self.peek().end;
                    self.advance();
                }
                self.positions.slice(start, end).trim().to_string()
            }
            _ => {
                return Err(FeelError::at(
                    codes::FEEL_SYNTAX_UNEXPECTED_TOKEN,
                    format!(
                        "Expected a context key (name or string) but got '{}'",
                        key_tok.text
                    ),
                    key_tok.start,
                    Some(self.positions.location_for(key_tok.start)),
                ));
            }
        };
        self.expect(TokenKind::Colon)?;
        let value = self.parse_or()?;
        Ok((key, value))
    }

    fn parse_path_or_call(&mut self) -> Result<FeelExpr, FeelError> {
        // `function(params) body` — a function-definition literal (the word `function` stays an
        // ordinary identifier; it is only special immediately before a parameter list).
        if self.peek().text == "function"
            && self.peek_next().map(|t| t.kind) == Some(TokenKind::LParen)
        {
            return self.parse_function_def();
        }
        // Builtins whose canonical name contains the `and` keyword — the lexer splits them, so
        // recognize the exact token run before a `(`: `date and time(…)`, `years and months
        // duration(…)`, `days and time duration(…)`.
        if let Some((name, ntokens)) = self.match_and_name() {
            let start = self.peek().start;
            for _ in 0..ntokens {
                self.advance();
            }
            let (args, arg_names, end) = self.parse_call_args()?;
            return Ok(FeelExpr::Call {
                start,
                end,
                name,
                args,
                arg_names,
            });
        }
        let head = self.peek().clone();
        self.advance();
        if self.peek().kind == TokenKind::LParen {
            let (args, arg_names, end) = self.parse_call_args()?;
            return Ok(FeelExpr::Call {
                start: head.start,
                end,
                name: head.text,
                args,
                arg_names,
            });
        }
        // Path expression: head + (. ident)*
        let mut segments = vec![head.text];
        let mut end = head.end;
        while self.peek().kind == TokenKind::Dot {
            self.advance();
            let seg = self.expect(TokenKind::Ident)?;
            segments.push(seg.text);
            end = seg.end;
        }
        Ok(FeelExpr::Path {
            start: head.start,
            end,
            segments,
        })
    }

    /// Parse a parenthesised argument list `( … )`, positional and/or named (`name: value`).
    /// Returns the argument expressions, a parallel list of optional names, and the `)` end offset.
    fn parse_call_args(&mut self) -> Result<CallArgs, FeelError> {
        self.expect(TokenKind::LParen)?;
        let mut args = Vec::new();
        let mut names = Vec::new();
        if self.peek().kind != TokenKind::RParen {
            loop {
                let (name, value) = self.parse_call_arg()?;
                names.push(name);
                args.push(value);
                if self.peek().kind == TokenKind::Comma {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        let close = self.expect(TokenKind::RParen)?;
        Ok((args, names, close.end))
    }

    /// One argument: `name: value` (named) or a bare expression (positional). A named argument is
    /// a run of one or more `Ident` tokens immediately followed by `:` — the parameter name
    /// itself may be a multi-word FEEL name (`grouping separator:`, `start position:`; DMN 1.4
    /// §10.3.4/§10.3.5's built-in parameter names). This is a different mechanism from the
    /// names-with-spaces lexer merge (`SPACED_BUILTIN_ALIASES`): a callee's own parameter names
    /// are static per-builtin metadata, never context keys, so they can't be pre-merged — the
    /// scan below finds the run directly. Purely additive: an `Ident` run followed by `:` is
    /// otherwise a guaranteed parse error (a bare positional expression never consumes more than
    /// one leading `Ident` before needing an operator/comma/paren), so this can only convert a
    /// certain failure into a named argument — never a regression of an existing passing parse.
    fn parse_call_arg(&mut self) -> Result<(Option<String>, FeelExpr), FeelError> {
        if self.peek().kind == TokenKind::Ident {
            let mut j = self.pos;
            while self.tokens.get(j).map(|t| t.kind) == Some(TokenKind::Ident) {
                j += 1;
            }
            if j > self.pos && self.tokens.get(j).map(|t| t.kind) == Some(TokenKind::Colon) {
                let words: Vec<String> = self.tokens[self.pos..j]
                    .iter()
                    .map(|t| t.text.clone())
                    .collect();
                for _ in self.pos..j {
                    self.advance();
                }
                self.advance(); // ':'
                let value = self.parse_or()?;
                return Ok((Some(words.join(" ")), value));
            }
        }
        Ok((None, self.parse_or()?))
    }

    /// `function(p1[, p2]*) body` — parameters are identifiers with an optional `: type`
    /// annotation (retained as the parameter's declared [`crate::value::FeelTypeShape`] — a
    /// non-conforming argument at call time makes the call `null`, DMN-TCK
    /// 0082-feel-coercion#fd_002); the body is a full expression.
    fn parse_function_def(&mut self) -> Result<FeelExpr, FeelError> {
        let start = self.advance().start; // `function`
        self.expect(TokenKind::LParen)?;
        let mut params = Vec::new();
        let mut param_shapes = Vec::new();
        if self.peek().kind != TokenKind::RParen {
            loop {
                params.push(self.expect(TokenKind::Ident)?.text);
                if self.peek().kind == TokenKind::Colon {
                    self.advance(); // ':'
                    param_shapes.push(self.parse_type_expr()?);
                } else {
                    param_shapes.push(crate::value::FeelTypeShape::Any);
                }
                if self.peek().kind == TokenKind::Comma {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        // Grammar rule 55's optional `external` marker. Contextual, NOT a reserved word: only the
        // exact position immediately after the parameter list's `)` reads it as the keyword, so
        // `external` keeps working as an ordinary variable/context-key name everywhere else.
        let external = self.peek().kind == TokenKind::Ident && self.peek().text == "external";
        if external {
            self.advance();
        }
        let body = self.parse_or()?;
        Ok(FeelExpr::FunctionDef {
            start,
            end: body.end(),
            params,
            param_shapes,
            external,
            body: Box::new(body),
        })
    }

    // ----- helpers -----

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn peek_next(&self) -> Option<&Token> {
        self.tokens.get(self.pos + 1)
    }

    /// If the tokens at the cursor spell one of the `and`-containing builtin names and are followed
    /// by `(`, return `(canonical_name, token_count)`. The lexer emits `and` as a keyword, so these
    /// multi-word names can't be recognized by the names-with-spaces merge.
    fn match_and_name(&self) -> Option<(String, usize)> {
        // (word0, [and], word2[, word3]) patterns — encoded as expected token texts, with `and`
        // matched by kind.
        const NAMES: &[(&[&str], &str)] = &[
            (&["date", "and", "time"], "date and time"),
            (
                &["years", "and", "months", "duration"],
                "years and months duration",
            ),
            (
                &["days", "and", "time", "duration"],
                "days and time duration",
            ),
        ];
        for (words, canonical) in NAMES {
            let matches = words.iter().enumerate().all(|(i, w)| {
                self.tokens.get(self.pos + i).is_some_and(|t| {
                    if *w == "and" {
                        t.kind == TokenKind::And
                    } else {
                        t.text == *w
                    }
                })
            });
            if matches
                && self.tokens.get(self.pos + words.len()).map(|t| t.kind)
                    == Some(TokenKind::LParen)
            {
                return Some((canonical.to_string(), words.len()));
            }
        }
        None
    }

    /// The end offset of the token just consumed (for spans that close on a keyword/type token).
    fn prev_end(&self) -> usize {
        self.tokens[self.pos.saturating_sub(1)].end
    }

    /// Parse a FEEL type expression after `instance of` (or, recursively, inside a generic's
    /// `<...>`): a base type that may be multi-word (`date and time`, `days and time duration`,
    /// `years and months duration`), or a structural generic — `list<T>` (→
    /// [`FeelTypeShape::Collection`]), `context<k1: T1, k2: T2, ...>` (→
    /// [`FeelTypeShape::Record`]), `range<T>` (→ [`FeelTypeShape::Range`]) — parsed into the real
    /// shape (DMN-TCK 0070-feel-instance-of `list_018`/`list_019`/`list_020`/`context_018..024`;
    /// generic content was previously discarded entirely). `function<T1, T2, ...> -> R` is parsed
    /// (both the parameter list and the arrow return type) but not retained structurally — no
    /// active TCK case exercises function-type covariance/contravariance at that granularity
    /// (DMN-TCK 0070's own `function_0NN` cases beyond `#010` are commented out in the corpus
    /// itself); `instance of` still correctly checks "is this a function value" via the plain
    /// `"function"` base-name arm. Any OTHER (unrecognized) generic head is left as a bare
    /// [`FeelTypeShape::Base`] with its own `<...>` skipped — a forward-compatible fallback, not
    /// currently reachable by the corpus.
    fn parse_type_expr(&mut self) -> Result<crate::value::FeelTypeShape, FeelError> {
        use crate::value::FeelTypeShape;
        let head = self.expect(TokenKind::Ident)?.text;
        let mut name = head.clone();
        // The three multi-word FEEL types.
        let words = |p: &mut Self, seq: &[&str]| -> bool {
            for (i, w) in seq.iter().enumerate() {
                let ok = match (i, p.tokens.get(p.pos + i)) {
                    (_, Some(t)) => (*w == "and" && t.kind == TokenKind::And) || t.text == *w,
                    _ => false,
                };
                if !ok {
                    return false;
                }
            }
            for _ in seq {
                p.advance();
            }
            true
        };
        match head.as_str() {
            "date" if words(self, &["and", "time"]) => name = "date and time".to_string(),
            "days" if words(self, &["and", "time", "duration"]) => {
                name = "days and time duration".to_string()
            }
            "years" if words(self, &["and", "months", "duration"]) => {
                name = "years and months duration".to_string()
            }
            _ => {}
        }
        if self.peek().kind != TokenKind::Lt {
            return Ok(FeelTypeShape::Base(name));
        }
        self.advance(); // '<'
        let shape = match name.as_str() {
            "list" => {
                let inner = self.parse_type_expr()?;
                self.expect(TokenKind::Gt)?;
                FeelTypeShape::Collection(Box::new(inner))
            }
            "range" => {
                let inner = self.parse_type_expr()?;
                self.expect(TokenKind::Gt)?;
                FeelTypeShape::Range(Box::new(inner))
            }
            "context" => {
                let mut comps = Vec::new();
                if self.peek().kind != TokenKind::Gt {
                    loop {
                        let cname = self.expect(TokenKind::Ident)?.text;
                        self.expect(TokenKind::Colon)?;
                        let cshape = self.parse_type_expr()?;
                        comps.push((cname, cshape));
                        if self.peek().kind == TokenKind::Comma {
                            self.advance();
                            continue;
                        }
                        break;
                    }
                }
                self.expect(TokenKind::Gt)?;
                FeelTypeShape::Record(comps)
            }
            "function" => {
                if self.peek().kind != TokenKind::Gt {
                    loop {
                        self.parse_type_expr()?; // parameter type, discarded
                        if self.peek().kind == TokenKind::Comma {
                            self.advance();
                            continue;
                        }
                        break;
                    }
                }
                self.expect(TokenKind::Gt)?;
                FeelTypeShape::Base("function".to_string())
            }
            _ => {
                // An unrecognized generic head (not exercised by the corpus) — skip the balanced
                // `<...>` the old (pre-structural-parsing) way and keep the bare base name.
                let mut depth = 1usize;
                while depth > 0 {
                    match self.peek().kind {
                        TokenKind::Lt => {
                            depth += 1;
                            self.advance();
                        }
                        TokenKind::Gt => {
                            depth -= 1;
                            self.advance();
                        }
                        TokenKind::Eof => break,
                        _ => {
                            self.advance();
                        }
                    }
                }
                FeelTypeShape::Base(name)
            }
        };
        // A `function<...> -> ReturnType` arrow — parsed (so the tokens are consumed) but the
        // return type itself is discarded; see the doc comment above. `->` is two tokens (Minus,
        // Gt). Only ever follows a closed `<...>`, so this check is shared by every generic head.
        if self.peek().kind == TokenKind::Minus
            && self.peek_next().map(|t| t.kind) == Some(TokenKind::Gt)
        {
            self.advance(); // '-'
            self.advance(); // '>'
            self.parse_type_expr()?; // return type, discarded
        }
        Ok(shape)
    }

    fn advance(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, kind: TokenKind) -> Result<Token, FeelError> {
        let tok = self.peek();
        if tok.kind != kind {
            return Err(FeelError::at(
                codes::FEEL_SYNTAX_UNEXPECTED_TOKEN,
                format!("Expected {} but got '{}'", kind.token_name(), tok.text),
                tok.start,
                Some(self.positions.location_for(tok.start)),
            ));
        }
        Ok(self.advance())
    }
}
