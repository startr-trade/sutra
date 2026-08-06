//! `sutra-xsd` — XSD-subset schema compiler with two back-ends over one parsed model:
//!
//! 1. **Streaming instance validation** ([`Schema::validate`]): one forward pass,
//!    collect-ALL violations with line:col positions, soft-error posture. Consumers
//!    map violations onto their stable diagnostic codes via [`DiagnosticProfile`] —
//!    [`DiagnosticProfile::MODULE_CODEC`] (`SUTRA.PARSE.XSD.*`) for module
//!    `schemaKind: xsd` codecs; an extension codec hands in its own published pair, so
//!    this crate names no message standard.
//! 2. **Navigation-shape emission** ([`Schema::navigation_shape`],
//!    [`Schema::value_coercion`]): the per-message-type path tables the deploy-time
//!    navigation checks and the multi-format value coercion consume — the compiled
//!    `schemas/**` archive artifact of the packaging contract.
//! 3. **Declared-field enumeration** ([`Schema::fields_of`]): one type's own children with the
//!    precision column typing needs — effective facets down the restriction chain, occurrence
//!    bounds, attribute-ness, `choice` membership. Where (2) is deliberately coarse and whole-
//!    message, this is exact and one type deep.
//!
//! Scope is exactly the Tier-1 profile: the runtime-validated schema
//! surface — the Standards-Editor idiom message definitions are published in, plus the
//! module-codec authoring surface. Everything outside it is rejected at *schema-compile*
//! time with a collected "not in the supported subset" finding — the module-codec
//! authoring contract. Schemas are single-file and self-contained by design; external
//! resolution does not exist in this crate (no import/include, no DTD, no network),
//! which is the XXE stance by construction.
//!
//! Behaviour is pinned at unit granularity against recorded expectations
//! (`tests/all/validate_behavior.rs`: presence + severity + location parity, never
//! message prose) and structurally over authored schema fixtures
//! (`tests/all/{compile_subset,shape_tables}.rs`). The corpus-scale differential —
//! real instance traffic plus systematically generated mutants, compared against
//! checked-in goldens — runs in whichever repository owns such a corpus, against
//! this same crate.

mod compile;
mod datatype;
mod diag;
mod facet;
mod fields;
mod model;
mod shape;
mod validate;

pub use datatype::Builtin;
pub use diag::{
    codes, schema_not_found, CompileError, CompileFinding, Diagnostic, DiagnosticProfile,
    DocumentError, Severity, SourcePos, Violation,
};
pub use fields::{FieldDecl, FieldFacets, FieldShape, TEXT_CONTENT_FIELD, WILDCARD_FIELD};
pub use model::Schema;
pub use shape::{FieldKind, NavigationShape, ValueCoercion};

/// A set of independently compiled schemas validating as one codec surface (a module
/// codec folder with several `.xsd` files). Each member stays self-contained; a
/// document is validated by the member that declares its root element.
#[derive(Debug, Default)]
pub struct SchemaSet {
    schemas: Vec<Schema>,
}

impl SchemaSet {
    /// Compile every document; findings from all of them are combined.
    pub fn compile(sources: &[&[u8]]) -> Result<SchemaSet, CompileError> {
        let mut schemas = Vec::new();
        let mut findings = Vec::new();
        for source in sources {
            match Schema::compile(source) {
                Ok(schema) => schemas.push(schema),
                Err(mut error) => findings.append(&mut error.findings),
            }
        }
        if findings.is_empty() {
            Ok(SchemaSet { schemas })
        } else {
            Err(CompileError { findings })
        }
    }

    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }

    /// All root element names across members (the codec's message types).
    pub fn root_names(&self) -> impl Iterator<Item = &str> {
        self.schemas.iter().flat_map(Schema::root_names)
    }

    /// The member schema declaring `root`, if any.
    pub fn schema_for_root(&self, root: &str) -> Option<&Schema> {
        self.schemas
            .iter()
            .find(|s| s.root_names().any(|r| r == root))
    }
}
