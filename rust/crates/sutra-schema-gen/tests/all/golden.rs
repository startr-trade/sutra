//! Golden regeneration: rebuild one schema in-test and require byte equality with a committed
//! reference emission — the fine-grained sibling of the CLI `check` gate (which runs over a
//! whole corpus).
//!
//! Both inputs are AUTHORED and live under `tests/data/` (see `tests/data/schemas/provenance.md`),
//! so this crate's suite is self-contained AND carries no third-party licensing surface: the
//! generator is a neutral tool, and its gate must not depend on the location of any particular
//! corpus, on the generated crate that consumes it, or on anyone's published message content.
//!
//! `test.002.001.03.xsd` is written in the Standards-Editor idiom the generator parses, and is
//! deliberately the SMALLEST schema that still drives every emission path — nested types, a
//! choice type, an unbounded repeat, an optional element, `xs:any` (`has_any`), an enumeration
//! value table, string facets (min/max length + pattern), decimal facets (total/fraction digits,
//! minInclusive), and `simpleContent` + a required attribute (the value/attribute slot pair).
//!
//! The `.golden` file is generator OUTPUT, not hand-written: refresh it with
//! `cargo run -p sutra-cli -- schemagen generate <this dir>/tests/data/schemas <tmp>` and copy
//! `test002v03.rs` over it, in the same commit as whatever generator change moved it.

use std::path::Path;

use sutra_schema_gen::{emit::Generator, parse, rustfmt};

#[test]
fn test002v03_regenerates_byte_identical() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let xsd_path = data.join("schemas/test.002.001.03.xsd");
    let committed_path = data.join("golden/test002v03.rs.golden");

    let xsd = std::fs::read(&xsd_path).expect("fixture schema readable");
    let result = parse::parse_xsd(&xsd).expect("schema parses");

    let mut generator = Generator::new();
    let raw = generator.generate(&result, "test002v03");
    let formatted = rustfmt(&raw).expect("rustfmt succeeds");

    let committed = std::fs::read_to_string(&committed_path).expect("golden readable");
    assert_eq!(
        formatted, committed,
        "regenerated test002v03.rs must be byte-identical to the committed golden"
    );
}

/// The golden must keep DRIVING every emission path — a fixture edit that quietly drops a
/// construct would still pass the byte-equality gate above (both sides regenerate together),
/// which is exactly the failure this catches.
#[test]
fn the_golden_exercises_every_emission_path() {
    let committed = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/golden/test002v03.rs.golden"),
    )
    .expect("golden readable");
    for needle in [
        "is_choice: true",              // choice type
        "has_any: true",                // xs:any
        "repeated: true",               // unbounded repeat
        "required: false",              // optional element
        "support::Content::Nested(",    // nested complex type
        "support::Content::Enum(",      // enumeration value table
        "support::Slot::Value",         // simpleContent value slot
        "support::Slot::Attribute",     // required attribute slot
        "support::ScalarKind::Decimal", // decimal scalar
        "min_len: Some(",               // string facets
        "patterns: &[",                 // pattern facet
        "total_digits: Some(",          // decimal facets
        "fraction_digits: Some(",       //
        "pub static SHAPES:",           // shape metadata
        "pub fn shape_of(",             //
    ] {
        assert!(
            committed.contains(needle),
            "the golden emission no longer contains `{needle}` — the fixture stopped \
             exercising that path"
        );
    }
}
