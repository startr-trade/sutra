//! String- and depth-aware lexer for the `.srl` DSL.
//!
//! The lexer tokenises the **whole** source (structural keywords, string literals, integers,
//! identifiers, brackets, punctuation) so the parser can (a) locate the section keywords `then` /
//! `end` and the action-argument commas at paren/bracket **depth 0 outside string literals**, and
//! (b) slice the *raw* character substrings that are the embedded FEEL expressions. String
//! literals are atomic tokens — a `then` inside a quoted string is part of that one token and can
//! never be mistaken for the section keyword. Any character the DSL does not itself use (FEEL
//! operators `< > = ! + - * / .`, digits inside FEEL, …) becomes an [`TokenKind::Other`] token, so
//! the lexer never chokes on embedded FEEL; those tokens matter only for depth tracking and are
//! otherwise carried along inside the raw FEEL slices.
//!
//! Offsets are **character** offsets into the source (a `Vec<char>`, mirroring
//! `sutra_feel`), so slicing and offset composition stay consistent with FEEL's char-based
//! positions.

use sutra_feel::positions::FeelSourcePositions;

use crate::codes;
use crate::error::SrlError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenKind {
    // Section / structural keywords.
    Rule,
    When,
    Then,
    End,
    Salience,
    ActivationGroup,
    // Action verbs (closed set) + verbs reserved for a stateful engine.
    Report,
    Set,
    Insert,
    Retract,
    // Literals / names.
    Ident,
    Str,
    Int,
    // Grouping (tracked for depth).
    LParen,
    RParen,
    LBracket,
    RBracket,
    // Punctuation used by the DSL surface.
    Comma,
    Semicolon,
    /// Any other single character (FEEL operator / punctuation). Carried for depth-neutral
    /// pass-through; only its span is meaningful.
    Other,
    Eof,
}

#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub kind: TokenKind,
    /// Raw source text of the token.
    pub text: String,
    /// Unescaped payload for [`TokenKind::Str`] tokens; empty otherwise.
    pub str_value: String,
    /// The opening quote character for [`TokenKind::Str`] tokens (`"` or `'`); `'\0'` otherwise.
    pub quote: char,
    /// Character offset of the first character of the token (0-based).
    pub start: usize,
    /// Character offset one past the last character of the token (end-exclusive).
    pub end: usize,
}

pub(crate) struct SrlLexer<'a> {
    src: &'a [char],
    /// A copy of `src` with `//` line-comment characters blanked to spaces (offsets preserved).
    /// The parser slices embedded-FEEL substrings from this so a trailing `// …` comment inside a
    /// condition/action never reaches the FEEL parser. A `//` inside a string literal is *not*
    /// blanked (strings are lexed atomically, so `skip_trivia` never sees it).
    masked: Vec<char>,
    positions: &'a FeelSourcePositions,
    pos: usize,
}

impl<'a> SrlLexer<'a> {
    pub fn new(src: &'a [char], positions: &'a FeelSourcePositions) -> Self {
        SrlLexer {
            src,
            masked: src.to_vec(),
            positions,
            pos: 0,
        }
    }

    /// Tokenise, returning the token stream and the comment-masked character source (same length
    /// as the input — token offsets index both identically).
    pub fn tokenize(mut self) -> Result<(Vec<Token>, Vec<char>), SrlError> {
        let mut tokens = Vec::new();
        loop {
            self.skip_trivia();
            if self.pos >= self.src.len() {
                break;
            }
            let start = self.pos;
            let c = self.src[self.pos];
            if c == '"' || c == '\'' {
                tokens.push(self.string(start, c)?);
            } else if c.is_ascii_digit() {
                tokens.push(self.integer(start));
            } else if c.is_alphabetic() || c == '_' {
                tokens.push(self.identifier_or_keyword(start));
            } else {
                tokens.push(self.punct_or_other(start, c));
            }
        }
        tokens.push(Token {
            kind: TokenKind::Eof,
            text: String::new(),
            str_value: String::new(),
            quote: '\0',
            start: self.src.len(),
            end: self.src.len(),
        });
        Ok((tokens, self.masked))
    }

    /// Skip whitespace and `//` line comments (repeatedly). A `//` inside a string never reaches
    /// here because strings are lexed atomically. Comment characters are blanked to spaces in the
    /// `masked` buffer so they vanish from any embedded-FEEL slice.
    fn skip_trivia(&mut self) {
        loop {
            while self.pos < self.src.len() && self.src[self.pos].is_whitespace() {
                self.pos += 1;
            }
            if self.pos + 1 < self.src.len()
                && self.src[self.pos] == '/'
                && self.src[self.pos + 1] == '/'
            {
                while self.pos < self.src.len() && self.src[self.pos] != '\n' {
                    self.masked[self.pos] = ' ';
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
    }

    fn string(&mut self, start: usize, quote: char) -> Result<Token, SrlError> {
        self.pos += 1; // opening quote
        let mut value = String::new();
        while self.pos < self.src.len() && self.src[self.pos] != quote {
            let c = self.src[self.pos];
            if c == '\\' && self.pos + 1 < self.src.len() {
                self.pos += 1;
                let next = self.src[self.pos];
                value.push(match next {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    other => other, // \\ \" \' and any other escaped char → itself
                });
            } else {
                value.push(c);
            }
            self.pos += 1;
        }
        if self.pos >= self.src.len() {
            return Err(SrlError::at(
                codes::SRL_UNCLOSED_STRING,
                "unclosed string literal",
                start,
                self.positions,
            ));
        }
        self.pos += 1; // closing quote
        Ok(Token {
            kind: TokenKind::Str,
            text: self.slice(start, self.pos),
            str_value: value,
            quote,
            start,
            end: self.pos,
        })
    }

    fn integer(&mut self, start: usize) -> Token {
        while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        Token {
            kind: TokenKind::Int,
            text: self.slice(start, self.pos),
            str_value: String::new(),
            quote: '\0',
            start,
            end: self.pos,
        }
    }

    fn identifier_or_keyword(&mut self, start: usize) -> Token {
        while self.pos < self.src.len()
            && (self.src[self.pos].is_alphanumeric() || self.src[self.pos] == '_')
        {
            self.pos += 1;
        }
        let word = self.slice(start, self.pos);
        // `activation-group` is a single hyphenated keyword token: after reading `activation`,
        // greedily consume a following `-group`.
        if word == "activation" && self.matches_ahead("-group") {
            self.pos += "-group".chars().count();
            return Token {
                kind: TokenKind::ActivationGroup,
                text: self.slice(start, self.pos),
                str_value: String::new(),
                quote: '\0',
                start,
                end: self.pos,
            };
        }
        let kind = match word.as_str() {
            "rule" => TokenKind::Rule,
            "when" => TokenKind::When,
            "then" => TokenKind::Then,
            "end" => TokenKind::End,
            "salience" => TokenKind::Salience,
            "report" => TokenKind::Report,
            "set" => TokenKind::Set,
            "insert" => TokenKind::Insert,
            "retract" => TokenKind::Retract,
            _ => TokenKind::Ident,
        };
        Token {
            kind,
            text: word,
            str_value: String::new(),
            quote: '\0',
            start,
            end: self.pos,
        }
    }

    fn punct_or_other(&mut self, start: usize, c: char) -> Token {
        let kind = match c {
            '(' => TokenKind::LParen,
            ')' => TokenKind::RParen,
            '[' => TokenKind::LBracket,
            ']' => TokenKind::RBracket,
            ',' => TokenKind::Comma,
            ';' => TokenKind::Semicolon,
            _ => TokenKind::Other,
        };
        self.pos += 1;
        Token {
            kind,
            text: c.to_string(),
            str_value: String::new(),
            quote: '\0',
            start,
            end: self.pos,
        }
    }

    /// True when the characters immediately at `self.pos` equal `needle`.
    fn matches_ahead(&self, needle: &str) -> bool {
        for (i, nc) in (self.pos..).zip(needle.chars()) {
            if i >= self.src.len() || self.src[i] != nc {
                return false;
            }
        }
        true
    }

    fn slice(&self, start: usize, end: usize) -> String {
        self.src[start..end].iter().collect()
    }
}
