//! Observability redaction — project a decoded payload into the masked view every read
//! surface (audit capture, the inspect API, dispatch diagnostics) must show. A
//! [`ContentRedactor`](sutra_redactor_spi::ContentRedactor) LOCATES sensitive spans; this
//! APPLIES them, replacing each located JSON-Pointer path's value with [`REDACTED_MARKER`] in
//! a COPY of the payload (the raw tree is never mutated — flow-to-flow logic still sees it).
//!
//! Fail-CLOSED: a crashed redactor ([`RedactionOutcome::Failed`]) over-masks the WHOLE payload
//! rather than risk a leak, so a broken redactor can never widen the exposure surface.

use sutra_feel::FeelValue;
use sutra_redactor_spi::tree::parse_pointer;
use sutra_redactor_spi::RedactionOutcome;

/// The irreversible mask substituted for a located sensitive span on observability surfaces.
pub const REDACTED_MARKER: &str = "[REDACTED]";

/// Build the masked observability projection of `payload` from the outcomes of the source's
/// redactor chain:
///
/// - Any [`RedactionOutcome::Failed`] (a redactor crash) fails CLOSED — the whole payload is
///   masked (returns the bare [`REDACTED_MARKER`]).
/// - Otherwise every located JSON-Pointer path from every [`RedactionOutcome::Located`] is
///   masked in a COPY of the payload; a `""` path masks the whole payload; a path that does
///   not resolve is skipped (the span is already absent).
pub fn mask_projection(payload: &FeelValue, outcomes: &[RedactionOutcome]) -> FeelValue {
    // A single crashed redactor over-masks everything — never leak past a failure.
    if outcomes
        .iter()
        .any(|o| matches!(o, RedactionOutcome::Failed { .. }))
    {
        return marker();
    }
    let paths: Vec<&str> = outcomes
        .iter()
        .filter_map(|o| match o {
            RedactionOutcome::Located(locators) => Some(locators),
            RedactionOutcome::Failed { .. } => None,
        })
        .flatten()
        .map(|l| l.path.as_str())
        .collect();
    mask_paths(payload, &paths)
}

fn marker() -> FeelValue {
    FeelValue::String(REDACTED_MARKER.to_string())
}

/// Mask the value at each JSON-Pointer `path` in a COPY of `payload`. A `""` (whole-document)
/// path masks everything; an unresolvable or malformed path is a no-op.
fn mask_paths(payload: &FeelValue, paths: &[&str]) -> FeelValue {
    if paths.iter().any(|p| p.is_empty()) {
        return marker();
    }
    let mut out = payload.clone();
    for p in paths {
        if let Some(tokens) = parse_pointer(p) {
            out = mask_one(&out, &tokens);
        }
    }
    out
}

/// Return a COPY of `value` with the node reached by `tokens` replaced by the marker. An empty
/// `tokens` slice masks `value` itself; a path that cannot be descended (a missing key, an
/// out-of-range or non-numeric list index, a scalar where a container was expected) leaves the
/// tree unchanged — the span simply is not present to leak.
fn mask_one(value: &FeelValue, tokens: &[String]) -> FeelValue {
    let Some((head, rest)) = tokens.split_first() else {
        return marker();
    };
    match value {
        FeelValue::Map(m) => {
            let mut m2 = m.clone();
            if let Some(child) = m2.get(head) {
                let masked = mask_one(child, rest);
                m2.insert(head.clone(), masked);
            }
            FeelValue::Map(m2)
        }
        FeelValue::List(items) => {
            if let Ok(idx) = head.parse::<usize>() {
                if idx < items.len() {
                    let mut items2 = items.clone();
                    items2[idx] = mask_one(&items2[idx], rest);
                    return FeelValue::List(items2);
                }
            }
            value.clone()
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sutra_redactor_spi::RedactionLocator;

    use super::*;

    fn s(v: &str) -> FeelValue {
        FeelValue::String(v.to_string())
    }

    fn map(pairs: &[(&str, FeelValue)]) -> FeelValue {
        FeelValue::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    fn located(paths: &[&str]) -> RedactionOutcome {
        RedactionOutcome::Located(
            paths
                .iter()
                .map(|p| RedactionLocator::new(*p, "SUTRA.TEST.REDACT"))
                .collect(),
        )
    }

    fn is_marker(v: &FeelValue) -> bool {
        matches!(v, FeelValue::String(x) if x == REDACTED_MARKER)
    }

    #[test]
    fn masks_a_top_level_key_only() {
        let payload = map(&[("a", s("1")), ("b", s("2"))]);
        let out = mask_projection(&payload, &[located(&["/a"])]);
        let FeelValue::Map(m) = out else { panic!() };
        assert!(is_marker(&m["a"]));
        assert_eq!(m["b"], s("2")); // sibling untouched
    }

    #[test]
    fn masks_a_nested_path() {
        let payload = map(&[("b", map(&[("c", s("secret")), ("d", s("keep"))]))]);
        let out = mask_projection(&payload, &[located(&["/b/c"])]);
        let FeelValue::Map(m) = out else { panic!() };
        let FeelValue::Map(b) = &m["b"] else { panic!() };
        assert!(is_marker(&b["c"]));
        assert_eq!(b["d"], s("keep"));
    }

    #[test]
    fn masks_a_list_index_leaving_siblings() {
        let payload = map(&[(
            "tx",
            FeelValue::List(vec![map(&[("pan", s("4111"))]), map(&[("pan", s("5555"))])]),
        )]);
        let out = mask_projection(&payload, &[located(&["/tx/0/pan"])]);
        let FeelValue::Map(m) = out else { panic!() };
        let FeelValue::List(tx) = &m["tx"] else {
            panic!()
        };
        let FeelValue::Map(t0) = &tx[0] else { panic!() };
        let FeelValue::Map(t1) = &tx[1] else { panic!() };
        assert!(is_marker(&t0["pan"]));
        assert_eq!(t1["pan"], s("5555"));
    }

    #[test]
    fn whole_document_path_masks_everything() {
        let payload = map(&[("a", s("1"))]);
        assert!(is_marker(&mask_projection(&payload, &[located(&[""])])));
    }

    #[test]
    fn failed_outcome_over_masks_the_whole_payload() {
        let payload = map(&[("a", s("1")), ("b", s("2"))]);
        let outcomes = vec![
            located(&["/a"]),
            RedactionOutcome::Failed {
                redactor: "boom".into(),
                message: "kaboom".into(),
            },
        ];
        assert!(is_marker(&mask_projection(&payload, &outcomes)));
    }

    #[test]
    fn unresolvable_path_is_a_no_op() {
        let payload = map(&[("a", s("1"))]);
        let out = mask_projection(&payload, &[located(&["/nope/x"])]);
        assert_eq!(out, payload); // nothing to mask, tree unchanged
    }

    #[test]
    fn no_outcomes_returns_the_payload_unchanged() {
        let payload = map(&[("a", s("1"))]);
        assert_eq!(mask_projection(&payload, &[]), payload);
    }
}
