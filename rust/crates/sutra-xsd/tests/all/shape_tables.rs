//! Navigation-shape emission: the compiled tables must carry what the T3 path checks need and
//! strictly supersede the first-cut structural scan (roots + numeric/boolean coercion by
//! element name).
//!
//! Driven by two fixture families — the AUTHORED schemas under `tests/data/schemas/` (see their
//! `provenance.md`) and the public example modules' own schemas, compiled in place.

use sutra_xsd::{FieldKind, Schema};

fn compile_repo(rel: &str) -> Schema {
    Schema::compile(&crate::support::repo_schema(rel)).unwrap()
}

#[test]
fn module_schema_shape_and_coercion() {
    let schema = compile_repo(
        "examples/money-transfer/deployments-src/default--money-transfer--1.0.0/schemas/transfer/transfer.xsd",
    );
    let mut roots: Vec<&str> = schema.root_names().collect();
    roots.sort_unstable();
    assert_eq!(
        roots,
        [
            "BalanceQuery",
            "CoverageQuery",
            "CoverageReset",
            "TransferRequest"
        ]
    );

    let shape = schema.navigation_shape("TransferRequest");
    assert_eq!(shape.paths.get("fromId"), Some(&FieldKind::String));
    assert_eq!(shape.paths.get("toId"), Some(&FieldKind::String));
    assert_eq!(shape.paths.get("amount"), Some(&FieldKind::Number));
    // A pure-sequence root is closed: an unknown sibling is a provable typo.
    assert!(!shape.open.contains(""));

    // The coercion index drives the multi-format decode's number/boolean coercion —
    // the structural first-cut equivalence.
    let coercion = schema.value_coercion();
    assert!(coercion.number_elements.contains("amount"));
    assert!(coercion.boolean_elements.is_empty());

    // Unknown root: fully open (warn-only), never a false error.
    let unknown = schema.navigation_shape("NoSuchRoot");
    assert!(unknown.paths.is_empty());
    assert!(unknown.open.contains(""));
}

/// The envelope case, over the authored `order.001.001.01` fixture: a `Document` root wrapping
/// one named envelope type, with every shape rule the flat model has to get right on a deep
/// message — nesting, the repeated-element cut-off, `simpleContent` openness, choice openness,
/// and "numeric text" staying text.
#[test]
fn envelope_schema_shape() {
    let schema = crate::support::fixture("order.001.001.01.xsd");
    assert_eq!(schema.root_names().collect::<Vec<_>>(), ["Document"]);

    let shape = schema.navigation_shape("Document");
    // The Document envelope: one child object.
    assert_eq!(shape.paths.get("OrderMessage"), Some(&FieldKind::Object));
    assert_eq!(
        shape.paths.get("OrderMessage.Header"),
        Some(&FieldKind::Object)
    );
    assert_eq!(
        shape.paths.get("OrderMessage.Header.MessageId"),
        Some(&FieldKind::String)
    );
    // Max15NumericText restricts xs:string — text, not number, however numeric it looks.
    assert_eq!(
        shape.paths.get("OrderMessage.Header.LineCount"),
        Some(&FieldKind::String)
    );
    // Repeated element: declared as an array, items not descended (flat model).
    assert_eq!(
        shape.paths.get("OrderMessage.Line"),
        Some(&FieldKind::Array)
    );
    assert_eq!(shape.paths.get("OrderMessage.Line.Sku"), None);
    // An amount (simpleContent + Ccy attribute) is an open object.
    assert_eq!(
        shape.paths.get("OrderMessage.Header.TotalAmount"),
        Some(&FieldKind::Object)
    );
    assert!(shape.open.contains("OrderMessage.Header.TotalAmount"));
    // A choice-typed container is open (a missing branch sibling is not a typo) while its
    // members stay declared.
    assert_eq!(
        shape.paths.get("OrderMessage.Header.Buyer.Id"),
        Some(&FieldKind::Object)
    );
    assert!(shape.open.contains("OrderMessage.Header.Buyer.Id"));
    assert_eq!(
        shape.paths.get("OrderMessage.Header.Buyer.Id.RegistryId"),
        Some(&FieldKind::String)
    );
    // The envelope itself stays CLOSED — openness is local to the containers that earn it.
    assert!(!shape.open.contains(""));
    assert!(!shape.open.contains("OrderMessage.Header"));

    // Coercion supersession: the amount element types via a *named* simpleContent chain
    // (invisible to a builtin-attribute scan) still resolve to Number.
    let coercion = schema.value_coercion();
    assert!(coercion.number_elements.contains("TotalAmount"));
    assert!(coercion.number_elements.contains("UnitPrice"));
    assert!(coercion.number_elements.contains("Quantity"));
    assert!(!coercion.number_elements.contains("MessageId"));
    assert!(!coercion.number_elements.contains("LineCount"));
    // The one boolean in the schema is found by name.
    assert!(coercion.boolean_elements.contains("Batched"));
}

/// The second fixture is arranged differently on purpose (inline root type, choice directly
/// under a named type, bounded repeat, decimal facets without `simpleContent`, a two-hop
/// restriction chain) — so the same rules are proved on a shape the first one does not have.
#[test]
fn inline_root_schema_shape() {
    let schema = crate::support::fixture("invoice.002.001.01.xsd");
    assert_eq!(schema.root_names().collect::<Vec<_>>(), ["Document"]);

    let shape = schema.navigation_shape("Document");
    assert_eq!(
        shape.paths.get("InvoiceSettlement"),
        Some(&FieldKind::Object)
    );
    assert_eq!(
        shape.paths.get("InvoiceSettlement.InvoiceId"),
        Some(&FieldKind::String)
    );
    assert_eq!(
        shape.paths.get("InvoiceSettlement.Net"),
        Some(&FieldKind::Number)
    );
    assert_eq!(
        shape.paths.get("InvoiceSettlement.Disputed"),
        Some(&FieldKind::Boolean)
    );
    // A BOUNDED repeat (maxOccurs="9") is an array too — "repeated" is the property, not
    // "unbounded".
    assert_eq!(
        shape.paths.get("InvoiceSettlement.Attachment"),
        Some(&FieldKind::Array)
    );
    assert_eq!(shape.paths.get("InvoiceSettlement.Attachment.Name"), None);
    // The choice opens ITS container, not the envelope above it.
    assert!(shape.open.contains("InvoiceSettlement.Reason"));
    assert!(!shape.open.contains("InvoiceSettlement"));
    assert_eq!(
        shape.paths.get("InvoiceSettlement.Reason.Code"),
        Some(&FieldKind::String)
    );
    assert_eq!(
        shape.paths.get("InvoiceSettlement.Reason.Narrative"),
        Some(&FieldKind::String)
    );

    let coercion = schema.value_coercion();
    assert!(coercion.number_elements.contains("Net"));
    assert!(coercion.boolean_elements.contains("Disputed"));
    // The two-hop restriction chain bottoms out in xs:string, so it is NOT a number.
    assert!(!coercion.number_elements.contains("Narrative"));
}

/// Sweeps every authored fixture: each root emits a non-empty path table. The equivalent sweep
/// over a large registered corpus runs in the repository that owns one.
#[test]
fn every_fixture_emits_a_shape_for_its_roots() {
    let mut checked = 0usize;
    for path in crate::support::fixtures() {
        let schema = Schema::compile(&std::fs::read(&path).unwrap()).unwrap();
        let roots: Vec<String> = schema.root_names().map(str::to_string).collect();
        assert!(!roots.is_empty(), "{} declares a root", path.display());
        for root in roots {
            let shape = schema.navigation_shape(&root);
            assert!(
                !shape.paths.is_empty(),
                "{} root {root} declares paths",
                path.display()
            );
        }
        checked += 1;
    }
    assert_eq!(checked, 2, "the authored schema fixture set");
}
