//! Translates a DMN `<inputEntry>` unary-test text into a full FEEL expression evaluable
//! against `{"input": value}`.
//!
//! DMN unary tests are an abbreviated grammar: the operand on the LHS is implicit (`> 10000`
//! means "the input is greater than 10000"). The FEEL evaluator has no unary-test mode — it
//! evaluates full expressions only — so this translator rewrites the unary test to its full
//! form with `input` as the synthesised LHS variable.
//!
//! Supported forms: comparisons (`> N`, `< N`, `>= N`, `<= N`, `= N`, `!= N`), string
//! equality (`= "x"`), bare literals (`5`, `"USD"` → equality), the wildcard `-` (→ `true`),
//! a comma-separated disjunction list of any of the above (`"Medium","Low"`, `<18,>=60` →
//! "matches any"), an interval/list literal (`[15..30)`, `]a..b]`, `[1,2,3]` → membership via
//! `in`), and pass-through full FEEL when the entry already looks like a complete expression.
//! Negation (`not(0)`) is out of scope, as in the canary.

/// Translate a unary-test text to a full FEEL expression with `input` as the synthetic LHS.
/// Returns the input unchanged when it looks like a full expression already.
///
/// # Panics
///
/// Panics on blank input — a programming error (callers guard blank entries as wildcard
/// matches).
pub fn to_full_expression(unary_test: &str) -> String {
    let trimmed = unary_test.trim();
    assert!(!trimmed.is_empty(), "unary test text is blank");

    // Wildcard: "-" means "matches any input" in DMN.
    if trimmed == "-" {
        return "true".to_string();
    }

    // A disjunction list ("Medium","Low" / <18,>=60): DMN Table 8.2's "list of simple
    // values" — the input matches when it matches ANY comma-separated segment. Split only on
    // TOP-LEVEL commas (never inside a quoted string or a bracket/paren pair), and translate
    // each segment with the very same rules (recursively), joined with `or`.
    if let Some(segments) = split_top_level_commas(trimmed) {
        return segments
            .iter()
            .map(|s| to_full_expression(s))
            .collect::<Vec<_>>()
            .join(" or ");
    }

    // Two-character comparison ops first (>= <=) so we don't accidentally split on '>' or '<'.
    if let Some(rest) = trimmed.strip_prefix(">=") {
        return format!("input >= {}", rest.trim());
    }
    if let Some(rest) = trimmed.strip_prefix("<=") {
        return format!("input <= {}", rest.trim());
    }
    if let Some(rest) = trimmed.strip_prefix("!=") {
        return format!("input != {}", rest.trim());
    }

    // One-character comparison ops.
    if let Some(rest) = trimmed.strip_prefix('>') {
        return format!("input > {}", rest.trim());
    }
    if let Some(rest) = trimmed.strip_prefix('<') {
        return format!("input < {}", rest.trim());
    }
    if let Some(rest) = trimmed.strip_prefix('=') {
        return format!("input = {}", rest.trim());
    }

    // An interval/list literal (`[15..30)`, `(a..b]`, `]a..b]`, `[1,2,3]`) is a unary test of
    // membership: "input is in this range/list" — FEEL's own `in` operator already handles
    // both a `Range` and a `List` right-hand side.
    if looks_like_range_or_list(trimmed) {
        return format!("input in {trimmed}");
    }

    // Pure literals are also valid unary tests: "5" means "input = 5", "\"USD\"" equality.
    if looks_like_literal(trimmed) {
        return format!("input = {trimmed}");
    }

    // A bare navigable name/path, or its negation — see `looks_like_bare_reference`'s doc
    // comment. Checked last, ahead only of the final pass-through, so every more-specific shape
    // above (comparisons, ranges/lists, literals) still wins first.
    if let Some(inner) = strip_full_not_wrapper(trimmed) {
        let inner = inner.trim();
        if looks_like_bare_reference(inner) {
            return format!("not(input in {inner})");
        }
    } else if looks_like_bare_reference(trimmed) {
        return format!("input in {trimmed}");
    }

    // Pass-through: the entry is already a full FEEL expression (heuristic — starts with a
    // letter or '(' / 'not(...)').
    trimmed.to_string()
}

/// `[`/`]` never open an arithmetic grouping in FEEL, so those always mean a list/range literal;
/// a leading `(` is genuinely ambiguous with a parenthesized boolean pass-through expression
/// (the existing pass-through heuristic below already anticipates that), so it's only treated as
/// a range open when the text also contains the `..` interval-endpoint separator — a bare
/// grouping paren never does.
fn looks_like_range_or_list(s: &str) -> bool {
    match s.chars().next() {
        Some('[') | Some(']') => true,
        Some('(') => s.contains(".."),
        _ => false,
    }
}

fn looks_like_literal(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    let second_is_digit = chars.next().is_some_and(|c| c.is_ascii_digit());
    // Numeric literal (incl. signed), string literal, or a bare boolean literal (DMN-TCK
    // 0004-lending/0087-chapter-11-example: an input-entry text of exactly `true`/`false` is a
    // unary-test LITERAL — "input = true" — exactly like `5` or `"USD"` two branches above, not
    // a standalone constant expression; without this, `to_full_expression` fell through to the
    // final pass-through branch and returned the bare word unchanged, so e.g. a rule's `true`
    // entry on a boolean column evaluated as the constant `true` and fired unconditionally,
    // completely ignoring the actual input value).
    first.is_ascii_digit()
        || first == '"'
        || ((first == '-' || first == '+') && second_is_digit)
        || s == "true"
        || s == "false"
}

/// A bare navigable name/path (`Complex.aBoolean`, `Flu Symtoms`) — DMN Table 8.2's "simple
/// value" grammar for a unary-test endpoint also permits a qualified name, not just a literal
/// constant (DMN-TCK 0036-dt-variable-input's `Compare Boolean`/`Compare String`/… columns,
/// 0039-dt-list-semantics' list-typed `Flu Symtoms` endpoint). FEEL's own `in` operator already
/// dispatches generically on the referenced value's runtime type (scalar -> equality, `List` ->
/// membership, `Range` -> interval test — see `sutra-feel`'s `FeelExpr::In` evaluator), so
/// translating a bare reference to `input in <name>` handles every case with zero new evaluator
/// work. Deliberately conservative: any operator/punctuation/quote character disqualifies it, so
/// a genuine full-FEEL-expression pass-through (`not(x > 3)`, `foo and bar`, `x + 1`) is never
/// mistaken for one (those keep falling through to the existing pass-through branch, unchanged).
fn looks_like_bare_reference(s: &str) -> bool {
    let trimmed = s.trim();
    let Some(first) = trimmed.chars().next() else {
        return false;
    };
    if !(first.is_alphabetic() || first == '_') {
        return false;
    }
    if !trimmed
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == ' ' || c == '\'' || c == '.')
    {
        return false;
    }
    if trimmed.contains("..") || trimmed.starts_with('.') || trimmed.ends_with('.') {
        return false;
    }
    // Reject a text containing any of these words ANYWHERE (not just a whole-string match): this
    // translator has no context/known-name awareness to tell a keyword-containing compound NAME
    // (only ever reached here as a rule's `<inputEntry>` text — a decision table's own
    // `<inputExpression>` side, where a genuine such name like DMN-TCK 0036's "Another Date and
    // Time" actually occurs, is evaluated through the context-aware FEEL lexer merge instead; see
    // `sutra_feel::lexer`'s `continues_name_run`) apart from an actual multi-variable boolean
    // expression (`foo and bar`, `not x`) — conservatively deferring to the existing pass-through
    // for anything ambiguous is always safe here (worst case, an as-yet-unseen keyword-containing
    // *rule-entry* bare name keeps its pre-cycle-6 behavior rather than gaining the new one).
    !trimmed.split([' ', '.']).any(|word| {
        matches!(
            word,
            "and" | "or" | "not" | "in" | "true" | "false" | "null"
        )
    })
}

/// `s` is exactly `not(` + inner text + a final `)`, with the inner text handed back untrimmed of
/// its own surrounding whitespace (callers trim). Used only to detect `not(<bare-reference>)`
/// (DMN-TCK 0036's `not(Complex.aBoolean)`/`not(Complex.aString)`) — a genuine boolean
/// pass-through like `not(x > 3)` has parens/operators in its inner text, which
/// `looks_like_bare_reference` already rejects, so it safely keeps falling through unchanged.
fn strip_full_not_wrapper(s: &str) -> Option<&str> {
    s.strip_prefix("not(")?.strip_suffix(')')
}

/// Split `s` on top-level commas — commas not nested inside a `"…"` string literal or a
/// `(...)`/`[...]` bracket pair (so a comma inside a range endpoint's own text, or inside a
/// quoted value, never splits). Returns `None` when there is no top-level comma at all (the
/// common single-test case), so callers can skip the recursive disjunction path entirely.
fn split_top_level_commas(s: &str) -> Option<Vec<&str>> {
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut boundaries = Vec::new();
    for (i, c) in s.char_indices() {
        if in_string {
            if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            ',' if depth <= 0 => boundaries.push(i),
            _ => {}
        }
    }
    if boundaries.is_empty() {
        return None;
    }
    let mut segments = Vec::with_capacity(boundaries.len() + 1);
    let mut last = 0;
    for b in boundaries {
        segments.push(s[last..b].trim());
        last = b + 1; // ',' is a single ASCII byte — b + 1 is still a char boundary.
    }
    segments.push(s[last..].trim());
    Some(segments)
}
