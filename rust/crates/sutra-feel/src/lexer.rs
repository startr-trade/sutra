//! Lexer for the FEEL subset. Produces a token stream consumed by the
//! parser.
//!
//! Recognises identifiers, numbers (including a leading-dot form, `.5`), strings (single or
//! double quoted; an apostrophe immediately between two letters is treated as possessive
//! punctuation inside an identifier, not a string delimiter — `Student's`), operators
//! (`= != <= >= < > + - * /`), punctuation (`( ) , .`), keywords (`and or not if then else
//! true false null`), and skips whitespace plus `// line` and `/* block */` comments (which may
//! interleave with whitespace and each other).
//!
//! Character classes use `char::is_alphabetic/is_alphanumeric/is_whitespace` — identical to
//! the reference implementation for ASCII, with minor Unicode-category differences at the
//! fringe (documented divergence; the conformance corpus is ASCII). Number lexing uses ASCII
//! digits (the reference implementation accepts Unicode `Nd` digits and then fails on decimal
//! construction — this rejects them at the lexer instead).

use bigdecimal::BigDecimal;

use crate::codes;
use crate::error::FeelError;
use crate::positions::FeelSourcePositions;
use crate::value::FeelValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenKind {
    Ident,
    Number,
    Str,
    Bool,
    Null,
    LParen,
    RParen,
    Comma,
    Dot,
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Times,
    Div,
    Pow,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    And,
    Or,
    Not,
    If,
    Then,
    Else,
    Some,
    Every,
    For,
    In,
    Satisfies,
    Return,
    DotDot,
    /// A parsed `@"…"` temporal literal; its `value` carries the decoded date/time/duration.
    Temporal,
    Eof,
}

impl TokenKind {
    /// The token's display name, as it appears in the parser's "Expected X but got 'y'"
    /// error messages.
    pub(crate) fn token_name(self) -> &'static str {
        match self {
            TokenKind::Ident => "IDENT",
            TokenKind::Number => "NUMBER",
            TokenKind::Str => "STRING",
            TokenKind::Bool => "BOOL",
            TokenKind::Null => "NULL",
            TokenKind::LParen => "LPAREN",
            TokenKind::RParen => "RPAREN",
            TokenKind::Comma => "COMMA",
            TokenKind::Dot => "DOT",
            TokenKind::Eq => "EQ",
            TokenKind::Neq => "NEQ",
            TokenKind::Lt => "LT",
            TokenKind::Le => "LE",
            TokenKind::Gt => "GT",
            TokenKind::Ge => "GE",
            TokenKind::Plus => "PLUS",
            TokenKind::Minus => "MINUS",
            TokenKind::Times => "TIMES",
            TokenKind::Div => "DIV",
            TokenKind::Pow => "POW",
            TokenKind::LBracket => "LBRACKET",
            TokenKind::RBracket => "RBRACKET",
            TokenKind::LBrace => "LBRACE",
            TokenKind::RBrace => "RBRACE",
            TokenKind::Colon => "COLON",
            TokenKind::And => "AND",
            TokenKind::Or => "OR",
            TokenKind::Not => "NOT",
            TokenKind::If => "IF",
            TokenKind::Then => "THEN",
            TokenKind::Else => "ELSE",
            TokenKind::Some => "SOME",
            TokenKind::Every => "EVERY",
            TokenKind::For => "FOR",
            TokenKind::In => "IN",
            TokenKind::Satisfies => "SATISFIES",
            TokenKind::Return => "RETURN",
            TokenKind::DotDot => "RANGE",
            TokenKind::Temporal => "TEMPORAL",
            TokenKind::Eof => "EOF",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub kind: TokenKind,
    pub text: String,
    /// Literal payload for NUMBER / STRING / BOOL tokens; `Null` otherwise.
    pub value: FeelValue,
    pub start: usize,
    pub end: usize,
}

pub(crate) struct FeelLexer<'a> {
    src: Vec<char>,
    positions: &'a FeelSourcePositions,
    pos: usize,
}

impl<'a> FeelLexer<'a> {
    pub fn new(src: &str, positions: &'a FeelSourcePositions) -> Self {
        FeelLexer {
            src: src.chars().collect(),
            positions,
            pos: 0,
        }
    }

    pub fn tokenize(mut self) -> Result<Vec<Token>, FeelError> {
        let mut tokens = Vec::new();
        while self.pos < self.src.len() {
            self.skip_whitespace()?;
            if self.pos >= self.src.len() {
                break;
            }
            let start = self.pos;
            let c = self.src[self.pos];
            // A leading-dot literal (`.5`) — a digit run with no integer part. `number()` is
            // parked exactly on the '.'; its own fraction-scanning logic handles this unchanged.
            let leading_dot_digit = c == '.'
                && self.pos + 1 < self.src.len()
                && self.src[self.pos + 1].is_ascii_digit();
            if c.is_ascii_digit() || leading_dot_digit {
                tokens.push(self.number(start));
            } else if c == '@' {
                tokens.push(self.temporal(start)?);
            } else if c == '"' || c == '\'' {
                tokens.push(self.string(start, c)?);
            } else if c.is_alphabetic() || c == '_' || is_extended_name_char(c) {
                tokens.push(self.identifier_or_keyword(start));
            } else {
                tokens.push(self.operator(start)?);
            }
        }
        tokens.push(Token {
            kind: TokenKind::Eof,
            text: String::new(),
            value: FeelValue::Null,
            start: self.src.len(),
            end: self.src.len(),
        });
        Ok(tokens)
    }

    fn number(&mut self, start: usize) -> Token {
        while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos < self.src.len()
            && self.src[self.pos] == '.'
            && self.pos + 1 < self.src.len()
            && self.src[self.pos + 1].is_ascii_digit()
        {
            self.pos += 1; // consume '.'
            while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        // Scientific-notation exponent: `e`/`E`, an optional sign, then a digit run
        // (DMN-TCK 0068-feel-equality number_008/009/010 — `1.23e4`, `1.23e+4`, `1.23e-4`).
        // Only consumed when at least one digit follows the (optional) sign — a bare trailing
        // `e`/`E` is never part of the numeric literal, so it's left for the identifier lexer
        // (unreachable in valid FEEL, but keeps this narrowly scoped to genuine exponents).
        if self.pos < self.src.len() && (self.src[self.pos] == 'e' || self.src[self.pos] == 'E') {
            let mut look = self.pos + 1;
            if look < self.src.len() && (self.src[look] == '+' || self.src[look] == '-') {
                look += 1;
            }
            if look < self.src.len() && self.src[look].is_ascii_digit() {
                self.pos = look;
                while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
                    self.pos += 1;
                }
            }
        }
        let text: String = self.src[start..self.pos].iter().collect();
        let value: BigDecimal = text.parse().expect("digit run is a valid decimal");
        Token {
            kind: TokenKind::Number,
            text,
            value: FeelValue::Number(value),
            start,
            end: self.pos,
        }
    }

    /// Decode a `\u`/`\U` escape starting at the escape letter (`self.pos`). `ndigits` hex digits
    /// follow; on success `self.pos` is left at the last consumed char (the outer loop's `+= 1`
    /// advances past it). A `\u` high surrogate recombines with a following low-surrogate `\u`
    /// into one astral code point. A malformed escape is emitted verbatim (`\u…`).
    fn push_unicode_escape(&mut self, out: &mut String, ndigits: usize) {
        let esc_letter = self.src[self.pos];
        let hex_start = self.pos + 1;
        let hex_end = hex_start + ndigits;
        let read_hex = |lo: usize, hi: usize| -> Option<u32> {
            if hi > self.src.len() {
                return None;
            }
            let s: String = self.src[lo..hi].iter().collect();
            u32::from_str_radix(&s, 16).ok()
        };
        let Some(code) = read_hex(hex_start, hex_end) else {
            out.push('\\'); // malformed — keep the backslash + letter verbatim
            out.push(esc_letter);
            return;
        };
        // UTF-16 surrogate-pair recombination (only for the 4-digit `\u` form).
        if ndigits == 4 && (0xD800..=0xDBFF).contains(&code) {
            let lo_start = hex_end;
            if lo_start + 6 <= self.src.len()
                && self.src[lo_start] == '\\'
                && self.src[lo_start + 1] == 'u'
            {
                if let Some(low) = read_hex(lo_start + 2, lo_start + 6) {
                    if (0xDC00..=0xDFFF).contains(&low) {
                        let combined = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                        if let Some(ch) = char::from_u32(combined) {
                            out.push(ch);
                            self.pos = lo_start + 5; // last char of the low escape
                            return;
                        }
                    }
                }
            }
        }
        match char::from_u32(code) {
            Some(ch) => {
                out.push(ch);
                self.pos = hex_end - 1; // last hex digit
            }
            None => {
                out.push('\\'); // lone surrogate / invalid — verbatim
                out.push(esc_letter);
            }
        }
    }

    fn string(&mut self, start: usize, quote: char) -> Result<Token, FeelError> {
        self.pos += 1; // consume opening quote
        let mut out = String::new();
        while self.pos < self.src.len() && self.src[self.pos] != quote {
            let c = self.src[self.pos];
            if c == '\\' && self.pos + 1 < self.src.len() {
                self.pos += 1;
                let next = self.src[self.pos];
                match next {
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    '\\' | '"' | '\'' => out.push(next),
                    // FEEL `\uXXXX` (a UTF-16 code unit; a high surrogate recombines with a
                    // following low-surrogate `\uXXXX` into one astral code point).
                    'u' => self.push_unicode_escape(&mut out, 4),
                    // FEEL `\UXXXXXX` — a 6-hex-digit code point.
                    'U' => self.push_unicode_escape(&mut out, 6),
                    // Any other escape (`\d`, `\s`, `\p`, `\1`, …) is preserved VERBATIM — FEEL
                    // string literals carry regex source into matches/replace/split, so the
                    // backslash must survive rather than silently defusing the pattern.
                    other => {
                        out.push('\\');
                        out.push(other);
                    }
                }
            } else {
                out.push(c);
            }
            self.pos += 1;
        }
        if self.pos >= self.src.len() {
            return Err(FeelError::at(
                codes::FEEL_SYNTAX_UNCLOSED_BRACKET,
                "Unclosed string literal",
                start,
                Some(self.positions.range_for(start, self.src.len())),
            ));
        }
        self.pos += 1; // consume closing quote
        let text: String = self.src[start..self.pos].iter().collect();
        Ok(Token {
            kind: TokenKind::Str,
            text,
            value: FeelValue::String(out),
            start,
            end: self.pos,
        })
    }

    /// A FEEL temporal literal `@"…"` — the `@` prefix then a quoted ISO string, decoded to a
    /// date/time/duration value at lex time. A MALFORMED token shape (no quote at all after `@`)
    /// is a genuine syntax error; a well-formed but UNRECOGNIZED temporal string (DMN-TCK
    /// 0093-feel-at-literals#test_001: `@"foo"`) is a semantic rejection instead — the same
    /// `FEEL_COMPILE_TYPE_MISMATCH` code every other "couldn't parse this temporal string" site
    /// uses (`date()`/`time()`/`date and time()`'s own `temporal_err`), not a SYNTAX code: the
    /// TCK harness's own errorResult rule only counts a non-SYNTAX error (or `null`) as the
    /// conformant "rejected" outcome — a SYNTAX code means "the engine couldn't even parse this",
    /// which isn't true here (the token shape is fine; its content just isn't a valid temporal
    /// value, exactly like an invalid `date("not a date")` argument).
    fn temporal(&mut self, start: usize) -> Result<Token, FeelError> {
        self.pos += 1; // consume '@'
        if self.pos >= self.src.len() || (self.src[self.pos] != '"' && self.src[self.pos] != '\'') {
            return Err(FeelError::at(
                codes::FEEL_SYNTAX_UNEXPECTED_TOKEN,
                "Expected a quoted temporal literal after '@'".to_string(),
                start,
                Some(self.positions.location_for(start)),
            ));
        }
        let quote = self.src[self.pos];
        let string_tok = self.string(self.pos, quote)?;
        let inner = match &string_tok.value {
            FeelValue::String(s) => s.clone(),
            _ => String::new(),
        };
        let value = crate::temporal::parse_at_literal(&inner).ok_or_else(|| {
            FeelError::at(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("Unrecognised temporal literal @\"{inner}\""),
                start,
                Some(self.positions.range_for(start, self.pos)),
            )
        })?;
        Ok(Token {
            kind: TokenKind::Temporal,
            text: format!("@\"{inner}\""),
            value,
            start,
            end: self.pos,
        })
    }

    /// Whether the character at `at` continues an identifier: alphanumeric/underscore, or a
    /// possessive apostrophe (`'`) immediately followed by another alphanumeric char
    /// (`Student's`, `Teacher's`) — a non-standard leniency this lexer also applies to `'…'`
    /// string literals (module doc), so a bare apostrophe must be claimed here first, before the
    /// main dispatch loop ever offers it to `string()` as a quote.
    fn continues_identifier(&self, at: usize) -> bool {
        let c = self.src[at];
        c.is_alphanumeric()
            || c == '_'
            || is_extended_name_char(c)
            || (c == '\'' && self.src.get(at + 1).is_some_and(|n| n.is_alphanumeric()))
            // DMN/FEEL "special character names" permit an internal hyphen mid-identifier
            // (`Date-Time2`, `Pre-bureauRiskCategory`) as long as it's immediately followed by a
            // name-start character (letter/underscore, not a digit) — this is what disambiguates
            // a hyphenated name from a tight subtraction (`x-1` still tokenizes as `x MINUS 1`;
            // only `identifier-identifier` adjacency, never valid subtraction syntax, changes).
            || (c == '-'
                && self
                    .src
                    .get(at + 1)
                    .is_some_and(|n| n.is_alphabetic() || *n == '_'))
    }

    fn identifier_or_keyword(&mut self, start: usize) -> Token {
        while self.pos < self.src.len() && self.continues_identifier(self.pos) {
            self.pos += 1;
        }
        let text: String = self.src[start..self.pos].iter().collect();
        word_token(text, start, self.pos)
    }

    fn operator(&mut self, start: usize) -> Result<Token, FeelError> {
        let c = self.src[self.pos];
        self.pos += 1;
        let simple = |kind: TokenKind, text: &str, end: usize| Token {
            kind,
            text: text.to_string(),
            value: FeelValue::Null,
            start,
            end,
        };
        match c {
            '=' => Ok(simple(TokenKind::Eq, "=", self.pos)),
            '<' => {
                if self.pos < self.src.len() && self.src[self.pos] == '=' {
                    self.pos += 1;
                    Ok(simple(TokenKind::Le, "<=", self.pos))
                } else {
                    Ok(simple(TokenKind::Lt, "<", self.pos))
                }
            }
            '>' => {
                if self.pos < self.src.len() && self.src[self.pos] == '=' {
                    self.pos += 1;
                    Ok(simple(TokenKind::Ge, ">=", self.pos))
                } else {
                    Ok(simple(TokenKind::Gt, ">", self.pos))
                }
            }
            '!' => {
                if self.pos < self.src.len() && self.src[self.pos] == '=' {
                    self.pos += 1;
                    Ok(simple(TokenKind::Neq, "!=", self.pos))
                } else {
                    Err(self.unexpected(c, start))
                }
            }
            '+' => Ok(simple(TokenKind::Plus, "+", self.pos)),
            '-' => Ok(simple(TokenKind::Minus, "-", self.pos)),
            '*' => {
                if self.pos < self.src.len() && self.src[self.pos] == '*' {
                    self.pos += 1;
                    Ok(simple(TokenKind::Pow, "**", self.pos))
                } else {
                    Ok(simple(TokenKind::Times, "*", self.pos))
                }
            }
            '/' => Ok(simple(TokenKind::Div, "/", self.pos)),
            '(' => Ok(simple(TokenKind::LParen, "(", self.pos)),
            ')' => Ok(simple(TokenKind::RParen, ")", self.pos)),
            '[' => Ok(simple(TokenKind::LBracket, "[", self.pos)),
            ']' => Ok(simple(TokenKind::RBracket, "]", self.pos)),
            '{' => Ok(simple(TokenKind::LBrace, "{", self.pos)),
            '}' => Ok(simple(TokenKind::RBrace, "}", self.pos)),
            ':' => Ok(simple(TokenKind::Colon, ":", self.pos)),
            ',' => Ok(simple(TokenKind::Comma, ",", self.pos)),
            '.' => {
                if self.pos < self.src.len() && self.src[self.pos] == '.' {
                    self.pos += 1;
                    Ok(simple(TokenKind::DotDot, "..", self.pos))
                } else {
                    Ok(simple(TokenKind::Dot, ".", self.pos))
                }
            }
            _ => Err(self.unexpected(c, start)),
        }
    }

    /// Skip ordinary whitespace and FEEL comments (`// line` / `/* block */`), re-looping so the
    /// two interleave freely (whitespace, then a comment, then more whitespace, then another
    /// comment, …) before the next real token starts. An unterminated block comment is a lexer
    /// error rather than a silent swallow-to-EOF.
    fn skip_whitespace(&mut self) -> Result<(), FeelError> {
        loop {
            while self.pos < self.src.len() && self.src[self.pos].is_whitespace() {
                self.pos += 1;
            }
            if self.pos + 1 < self.src.len()
                && self.src[self.pos] == '/'
                && self.src[self.pos + 1] == '/'
            {
                self.pos += 2;
                while self.pos < self.src.len() && self.src[self.pos] != '\n' {
                    self.pos += 1;
                }
                continue;
            }
            if self.pos + 1 < self.src.len()
                && self.src[self.pos] == '/'
                && self.src[self.pos + 1] == '*'
            {
                let start = self.pos;
                self.pos += 2;
                let mut closed = false;
                while self.pos + 1 < self.src.len() {
                    if self.src[self.pos] == '*' && self.src[self.pos + 1] == '/' {
                        self.pos += 2;
                        closed = true;
                        break;
                    }
                    self.pos += 1;
                }
                if !closed {
                    return Err(FeelError::at(
                        codes::FEEL_SYNTAX_UNCLOSED_BRACKET,
                        "Unterminated block comment",
                        start,
                        Some(self.positions.range_for(start, self.src.len())),
                    ));
                }
                continue;
            }
            break;
        }
        Ok(())
    }

    fn unexpected(&self, c: char, at: usize) -> FeelError {
        FeelError::at(
            codes::FEEL_SYNTAX_UNEXPECTED_TOKEN,
            format!("Unexpected character '{c}'"),
            at,
            Some(self.positions.location_for(at)),
        )
    }
}

/// Keyword/identifier classification for one word of source text — shared between the main
/// scanner ([`FeelLexer::identifier_or_keyword`]) and the scope-gated hyphenated-name split
/// ([`split_hyphenated_names`]), so a split-off piece that spells a keyword re-tokenizes exactly
/// as the scanner would have.
fn word_token(text: String, start: usize, end: usize) -> Token {
    let (kind, value) = match text.as_str() {
        "and" => (TokenKind::And, FeelValue::Null),
        "or" => (TokenKind::Or, FeelValue::Null),
        "not" => (TokenKind::Not, FeelValue::Null),
        "if" => (TokenKind::If, FeelValue::Null),
        "then" => (TokenKind::Then, FeelValue::Null),
        "else" => (TokenKind::Else, FeelValue::Null),
        "some" => (TokenKind::Some, FeelValue::Null),
        "every" => (TokenKind::Every, FeelValue::Null),
        "for" => (TokenKind::For, FeelValue::Null),
        "in" => (TokenKind::In, FeelValue::Null),
        "satisfies" => (TokenKind::Satisfies, FeelValue::Null),
        "return" => (TokenKind::Return, FeelValue::Null),
        "true" => (TokenKind::Bool, FeelValue::Boolean(true)),
        "false" => (TokenKind::Bool, FeelValue::Boolean(false)),
        "null" => (TokenKind::Null, FeelValue::Null),
        _ => (TokenKind::Ident, FeelValue::String(text.clone())),
    };
    Token {
        kind,
        text,
        value,
        start,
        end,
    }
}

/// Whether `c` should be treated as a "Name" character purely because it's non-ASCII and not
/// whitespace — this widens identifier start/continuation beyond `is_alphanumeric()` (which
/// covers non-ASCII LETTERS/digits already, e.g. accented Latin, but not Unicode SYMBOL
/// characters like emoji) to also accept a bare, unquoted supplementary/astral character used as
/// a name — DMN-TCK 0083-feel-unicode `decision_006`/`decision_007`: `{🐎: "bar"}`, a context key
/// that is a single horse emoji (Unicode category "So", never alphanumeric). Safe to widen this
/// unconditionally: every FEEL operator/punctuation character this lexer recognizes (`operator`'s
/// own match) is plain ASCII, so no non-ASCII character can ever collide with a real operator —
/// this can only ever affect text that would otherwise be a hard "unexpected character" lex
/// error, never reinterpret an already-meaningful token.
fn is_extended_name_char(c: char) -> bool {
    (c as u32) >= 0x80 && !c.is_whitespace()
}

/// Collapse runs of adjacent `Ident`/`Number`/`In` tokens into a single `Ident` when the
/// space-joined run is a name known to the evaluation context — FEEL "names with spaces"
/// (§10.3.1.2), the dominant DMN-TCK level-3 construct (e.g. `Total Vacation Days`,
/// `decision C 2`). Because the lexer emits keywords (`and`/`or`/`if`/…) and every operator
/// as their own distinct kinds — never `Ident` — a run can only span the word/number pieces of
/// a single name, so this can never fuse across an operator or keyword boundary — EXCEPT `in`,
/// which a name may legitimately embed (`values in a list`, DMN-TCK 0016); `TokenKind::In` is
/// allowed mid-run for exactly that reason (a run must still START at an `Ident`, so a name can
/// never begin with the word "in", and the final merge is still gated on an exact `known` match,
/// so a run that happens to contain the quantifier's own real `in` keyword — with nothing in
/// scope named like it — simply falls through un-merged; see `feel_expressions.rs`'s
/// `spaced_name_containing_in_keyword` test for the full trace).
///
/// Greedy longest-match: at each `Ident`, the longest prefix of the following run whose
/// space-joined text is in `known` wins; a run matching nothing is left byte-for-byte
/// intact. Two adjacent operand tokens with no operator between them are not valid FEEL, so the
/// only expressions this changes are ones that were previously parse errors — single-word names
/// and every operator/keyword-separated expression are untouched.
///
/// Context-gated: only the `eval*` entry points (which carry a context) call this; the
/// context-free `parse` API never merges, preserving parse/eval separation for callers that
/// supply no names. The early-out makes it a no-op whenever no known name contains a space.
/// Cheap, allocation-free pre-check: does the stream contain two adjacent `Ident`/`Number`/`In`
/// tokens? Only such a run can ever be merged into a multi-word name, so when there is none the
/// caller skips building the known-name set and the merge entirely — the common case (operator-
/// separated expressions, single names). Restores the pre-merge fast path for every eval site.
/// A token kind that can appear as one WORD inside a multi-word DMN/FEEL name, when adjacent to
/// an `Ident` — not just another `Ident`/`Number` (DMN-TCK 0016's "values in a list") but also the
/// common English conjunction/negation keywords `and`/`or`/`not`, since a business-domain name is
/// free to contain any of those as an ordinary word (DMN-TCK 0036-dt-variable-input's own input
/// names "Another Date **and** Time" / "Another Days **and** Time Duration" / "Another Years
/// **and** Months Duration" — without this, the run broke at the keyword token, so the merge
/// either failed outright or (worse) matched a shorter, unrelated known name as a false-positive
/// prefix, corrupting the parse of the rest of the text).
fn continues_name_run(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Ident
            | TokenKind::Number
            | TokenKind::In
            | TokenKind::And
            | TokenKind::Or
            | TokenKind::Not
    )
}

pub(crate) fn has_adjacent_name_run(tokens: &[Token]) -> bool {
    tokens
        .windows(2)
        .any(|w| w[0].kind == TokenKind::Ident && continues_name_run(w[1].kind))
}

/// Cheap pre-check for [`resolve_names`]: does the stream contain anything scope-aware name
/// resolution could change — an adjacent name run (the spaces merge) or a hyphen-bearing `Ident`
/// (the hyphenated-name split)? When neither is present the caller skips building the known-name
/// set entirely (the common case: operator-separated expressions, single plain names).
pub(crate) fn needs_name_resolution(tokens: &[Token]) -> bool {
    has_adjacent_name_run(tokens)
        || tokens
            .iter()
            .any(|t| t.kind == TokenKind::Ident && t.text.contains('-'))
}

/// Scope-aware name resolution over a raw token stream: the hyphenated-name split (below), then
/// the "names with spaces" merge ([`merge_named_tokens`]). Split before merge: a split never
/// produces two adjacent `Ident`s (pieces are separated by the `-` they were split on), so the
/// passes cannot feed each other — the order only matters in that a hyphen-bearing token kept
/// whole (a known hyphenated name) must still be visible to the merge as one run element.
pub(crate) fn resolve_names(
    tokens: Vec<Token>,
    known: &std::collections::HashSet<String>,
) -> Vec<Token> {
    let tokens = split_hyphenated_names(tokens, known);
    if has_adjacent_name_run(&tokens) {
        merge_named_tokens(tokens, known)
    } else {
        tokens
    }
}

/// The scope-gated counterpart of the scanner's hyphen folding. The lexer folds
/// `identifier-identifier` adjacency into ONE hyphenated `Ident` (`Pre-bureauRiskCategory`,
/// `Date-Time2` — FEEL §10.3.1.2 permits `-` inside a name), but that same spelling is equally
/// valid FEEL for tight subtraction between two variables (`Rn-Kn`, DMN-TCK
/// 0035-test-structure-output's `1-Rn-Kn`). The grammar's ambiguity rule is scope-based —
/// the longest NAME KNOWN IN SCOPE wins, anything else is arithmetic — which a context-free
/// scanner cannot decide; this pass applies exactly that rule where scope exists (the `eval*`
/// entry points and `parse_with_known_names`). A folded token whose full text is a known name is
/// kept intact; otherwise it is re-split into `piece MINUS piece …`, with each maximal
/// hyphen-joined prefix that IS a known name kept as one `Ident` (longest-match, mirroring
/// [`merge_named_tokens`]), and each split-off word re-classified through the scanner's own
/// keyword table (`x-null` ⇒ `x - null`). The context-free `parse` API is untouched: with no
/// scope there is no evidence against the name reading, so the fold stands — exactly the
/// pre-existing behavior.
pub(crate) fn split_hyphenated_names(
    tokens: Vec<Token>,
    known: &std::collections::HashSet<String>,
) -> Vec<Token> {
    // A hyphen-bearing token must survive intact not only when it IS a known name, but also when
    // it appears as one space-separated WORD of a known name — the [`merge_named_tokens`] pass
    // that runs next needs the fold as its run element (DMN-TCK 0087-chapter-11-example:
    // `Post-bureau risk category` merges from the token run ["Post-bureau", "risk", "category"];
    // splitting "Post-bureau" first would make that name unreachable). A kept-but-unmatched fold
    // degrades exactly as before this pass existed (one unknown name ⇒ null/error), never worse.
    let keep_folded = |text: &str| {
        known.contains(text) || known.iter().any(|n| n.split(' ').any(|word| word == text))
    };
    if !tokens
        .iter()
        .any(|t| t.kind == TokenKind::Ident && t.text.contains('-') && !keep_folded(&t.text))
    {
        return tokens;
    }
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len());
    for t in tokens {
        if t.kind != TokenKind::Ident || !t.text.contains('-') || keep_folded(&t.text) {
            out.push(t);
            continue;
        }
        // The scanner only ever folds a `-` immediately followed by a name-start character, so
        // every piece is non-empty and the char offsets below are exact.
        let pieces: Vec<String> = t.text.split('-').map(String::from).collect();
        let mut piece_starts = Vec::with_capacity(pieces.len());
        let mut at = t.start;
        for p in &pieces {
            piece_starts.push(at);
            at += p.chars().count() + 1; // the piece plus its trailing '-'
        }
        let mut i = 0;
        while i < pieces.len() {
            // Longest hyphen-joined prefix that is a known name; a single piece otherwise.
            let mut k = pieces.len();
            while k > i + 1 && !known.contains(&pieces[i..k].join("-")) {
                k -= 1;
            }
            if i > 0 {
                out.push(Token {
                    kind: TokenKind::Minus,
                    text: "-".to_string(),
                    value: FeelValue::Null,
                    start: piece_starts[i] - 1,
                    end: piece_starts[i],
                });
            }
            let end = piece_starts[k - 1] + pieces[k - 1].chars().count();
            out.push(word_token(pieces[i..k].join("-"), piece_starts[i], end));
            i = k;
        }
    }
    out
}

pub(crate) fn merge_named_tokens(
    tokens: Vec<Token>,
    known: &std::collections::HashSet<String>,
) -> Vec<Token> {
    // A multi-token run always space-joins to a string containing a space, so it can only ever
    // match a space-bearing name. No such name in scope ⇒ the pass can do nothing.
    if !known.iter().any(|n| n.contains(' ')) {
        return tokens;
    }
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        if tokens[i].kind == TokenKind::Ident {
            // Maximal adjacent name-continuing run beginning at i (a name starts with a letter/
            // underscore ⇒ an Ident, and continues through word or digit pieces, an embedded `in`
            // — DMN-TCK 0016's `values in a list` — or an embedded `and`/`or`/`not` — DMN-TCK
            // 0036's "Another Date and Time" — see `continues_name_run`'s doc comment).
            let mut run_end = i + 1;
            while run_end < tokens.len() && continues_name_run(tokens[run_end].kind) {
                run_end += 1;
            }
            // Longest prefix (≥2 tokens) whose join is a known name wins; a 1-token "run" is
            // just the plain Ident and needs no merge.
            let mut merged = false;
            let mut k = run_end;
            while k >= i + 2 {
                let candidate = tokens[i..k]
                    .iter()
                    .map(|t| t.text.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
                if known.contains(&candidate) {
                    out.push(Token {
                        kind: TokenKind::Ident,
                        text: candidate.clone(),
                        value: FeelValue::String(candidate),
                        start: tokens[i].start,
                        end: tokens[k - 1].end,
                    });
                    i = k;
                    merged = true;
                    break;
                }
                k -= 1;
            }
            if merged {
                continue;
            }
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}
