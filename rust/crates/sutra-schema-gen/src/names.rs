//! Identifier mangling — the deterministic name transforms the generated Rust
//! surface depends on. These are the contract for the emitted module/type/field/enum
//! identifiers; changing any of them changes the wire/API surface of the generated
//! crate, so they are pinned by unit tests (see the bottom of this file) and by the
//! byte-identical regeneration gate (the CLI `check` command).

use std::collections::HashSet;

/// Reserved Rust identifiers a mangled name must not collide with (suffixed with `_`).
pub const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false", "fn",
    "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
    "return", "self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use",
    "where", "while", "async", "await", "abstract", "become", "box", "do", "final", "macro",
    "override", "priv", "typeof", "unsized", "virtual", "yield", "try", "gen",
];

fn is_keyword(s: &str) -> bool {
    RUST_KEYWORDS.contains(&s)
}

/// camelCase / PascalCase / XML-name → snake_case Rust identifier.
pub fn snake_case(name: &str) -> String {
    let cs: Vec<char> = name.chars().collect();
    let mut sb = String::new();
    for (i, &c) in cs.iter().enumerate() {
        if !c.is_ascii_alphanumeric() {
            if !sb.is_empty() && !sb.ends_with('_') {
                sb.push('_');
            }
            continue;
        }
        if c.is_ascii_uppercase() && i > 0 {
            let prev = cs[i - 1];
            let prev_lower_or_digit = prev.is_ascii_lowercase() || prev.is_ascii_digit();
            let next_lower = i + 1 < cs.len() && cs[i + 1].is_ascii_lowercase();
            if (prev_lower_or_digit || (prev.is_ascii_uppercase() && next_lower))
                && !sb.is_empty()
                && !sb.ends_with('_')
            {
                sb.push('_');
            }
        }
        sb.push(c.to_ascii_lowercase());
    }
    let mut s = if sb.is_empty() {
        "field".to_string()
    } else {
        sb
    };
    if s.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        s = format!("f_{s}");
    }
    if is_keyword(&s) {
        s.push('_');
    }
    s
}

/// Escapes a string for a Rust `"…"` literal.
pub fn escape_rust(s: &str) -> String {
    let mut sb = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '\\' => sb.push_str("\\\\"),
            '"' => sb.push_str("\\\""),
            '\n' => sb.push_str("\\n"),
            '\r' => sb.push_str("\\r"),
            '\t' => sb.push_str("\\t"),
            c if (c as u32) < 0x20 => sb.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => sb.push(c),
        }
    }
    sb
}

/// XSD type/element name → PascalCase class identifier (strip a leading `_`, split on
/// non-alphanumeric runs, capitalize each segment's first char).
pub fn to_class_name(name: &str) -> String {
    if name.is_empty() {
        return name.to_string();
    }
    let name = name.strip_prefix('_').unwrap_or(name);
    let mut sb = String::new();
    for part in name.split(|c: char| !c.is_ascii_alphanumeric()) {
        if part.is_empty() {
            continue;
        }
        let mut chars = part.chars();
        let first = chars.next().unwrap();
        sb.push(first.to_ascii_uppercase());
        sb.push_str(chars.as_str());
    }
    sb
}

/// XSD enumeration value → canonical constant name (non-alnum runs → `_`, uppercased,
/// one leading/trailing `_` stripped). This is the constant the generated
/// `canonical_name()` accessor projects — the enum-name round-trip surface.
pub fn to_enum_constant(value: &str) -> String {
    let mut s = String::new();
    let mut prev_underscore = false;
    for c in value.chars() {
        if c.is_ascii_alphanumeric() {
            s.push(c.to_ascii_uppercase());
            prev_underscore = false;
        } else if !prev_underscore {
            s.push('_');
            prev_underscore = true;
        }
    }
    let s = s.strip_prefix('_').unwrap_or(&s);
    let s = s.strip_suffix('_').unwrap_or(s);
    s.to_string()
}

/// Sanitizes a constant name into a unique valid Rust enum-variant identifier,
/// recording it in `used` so later duplicates gain a numeric suffix.
pub fn sanitize_variant(canonical_name: &str, used: &mut HashSet<String>) -> String {
    let mut sb = String::new();
    for c in canonical_name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            sb.push(c);
        } else {
            sb.push('_');
        }
    }
    let mut v = if sb.is_empty() {
        "VALUE".to_string()
    } else {
        sb
    };
    if v.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        v = format!("V{v}");
    }
    let mut candidate = v.clone();
    let mut n = 2;
    while used.contains(&candidate) {
        candidate = format!("{v}_{n}");
        n += 1;
    }
    used.insert(candidate.clone());
    candidate
}

/// Message identifier → Rust module name:
/// `<family>.<number>.<variant>.<version>` → `<family><number>v<version>`
/// (e.g. `order.001.001.14` → `order001v14`). This is the registry idiom's whole naming
/// convention, so generation needs no per-schema configuration.
///
/// The variant segment must be `001` — the single-variant posture of the current corpus.
/// A future multi-variant schema would collide under this rule, so it fails loudly here
/// (extend the rule then) instead of silently overwriting a module.
pub fn module_from_message_type(message_type: &str) -> Result<String, String> {
    let segments: Vec<&str> = message_type.split('.').collect();
    let [fam, num, variant, ver] = segments.as_slice() else {
        return Err(format!(
            "message type '{message_type}' is not <family>.<number>.<variant>.<version>; \
             cannot derive a module name"
        ));
    };
    if fam.is_empty()
        || !fam.chars().all(|c| c.is_ascii_lowercase())
        || [num, variant, ver]
            .iter()
            .any(|s| s.is_empty() || !s.chars().all(|c| c.is_ascii_digit()))
    {
        return Err(format!(
            "message type '{message_type}' has non-canonical segments; cannot derive a module name"
        ));
    }
    if *variant != "001" {
        return Err(format!(
            "message type '{message_type}' has variant '{variant}' (≠ 001); the module-name \
             rule drops the variant and would collide — extend the rule for multi-variant corpora"
        ));
    }
    Ok(format!("{fam}{num}v{ver}"))
}

/// Module name → registry enum variant (first char uppercased).
pub fn variant_name(module_name: &str) -> String {
    let mut chars = module_name.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
        None => String::new(),
    }
}

/// Message family of a module: its leading alphabetic prefix (`order001v14` → `order`).
pub fn family(module_name: &str) -> &str {
    let end = module_name
        .char_indices()
        .find(|(_, c)| !c.is_ascii_alphabetic())
        .map(|(i, _)| i)
        .unwrap_or(module_name.len());
    if end == 0 {
        module_name
    } else {
        &module_name[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_case_handles_abbreviated_registry_names() {
        // The registry idiom abbreviates heavily and embeds acronyms mid-name, so the run of
        // capitals before a lowercase letter must break in the right place.
        assert_eq!(snake_case("HTTPToAMQPOrdrRte"), "http_to_amqp_ordr_rte");
        assert_eq!(snake_case("MsgId"), "msg_id");
        assert_eq!(snake_case("NbOfLines"), "nb_of_lines");
        assert_eq!(snake_case("XMLID"), "xmlid");
        assert_eq!(snake_case("URLAdr"), "url_adr");
        assert_eq!(snake_case("CreDtTm"), "cre_dt_tm");
        assert_eq!(snake_case("GroupHeader131"), "group_header131");
    }

    #[test]
    fn snake_case_escapes_rust_keywords() {
        assert_eq!(snake_case("Ref"), "ref_");
        assert_eq!(snake_case("Type"), "type_");
        assert_eq!(snake_case("Match"), "match_");
    }

    #[test]
    fn escape_rust_escapes_quotes_and_backslashes() {
        assert_eq!(escape_rust("a\\d\"x"), "a\\\\d\\\"x");
    }

    #[test]
    fn enum_constant_variant_and_family() {
        assert_eq!(to_enum_constant("DEBT"), "DEBT");
        assert_eq!(to_enum_constant("A-B"), "A_B");
        assert_eq!(variant_name("test002v03"), "Test002v03");
        assert_eq!(family("order001v14"), "order");
        assert_eq!(family("head001v04"), "head");
    }

    #[test]
    fn module_derivation_from_message_type() {
        assert_eq!(
            module_from_message_type("test.002.001.03").unwrap(),
            "test002v03"
        );
        assert_eq!(
            module_from_message_type("order.001.001.14").unwrap(),
            "order001v14"
        );
        assert_eq!(
            module_from_message_type("head.001.001.04").unwrap(),
            "head001v04"
        );
        // Non-canonical shapes and non-001 variants fail loudly (collision guard).
        assert!(module_from_message_type("order.001.14").is_err());
        assert!(module_from_message_type("order.001.002.14").is_err());
        assert!(module_from_message_type("ORDER.001.001.14").is_err());
    }
}
