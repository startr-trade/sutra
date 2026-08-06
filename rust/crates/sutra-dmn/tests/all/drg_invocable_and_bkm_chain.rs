//! Focused unit tests for cycle-6 mechanisms that the full TCK conformance run (`tests/tck.rs`)
//! exercises indirectly but doesn't pin down in isolation:
//!
//! - the BKM callable-prelude's "names with spaces" merge at PARSE time (a BKM body calling
//!   ANOTHER multi-word-named BKM — DMN-TCK 0034-drg-scopes's "BKM I" → "BKM II" → "BKM III"/
//!   "BKM IV" chain);
//! - the native `Invocable` mechanism for indirect decision-service invocation (DMN-TCK
//!   0085-decision-services), including its STRICT arity gating;
//! - a boxed `<relation>`'s text translation to a list-of-contexts literal (DMN-TCK
//!   0016-some-every's `priceTable1`);
//! - a non-literal-bodied (boxed context/table) BKM's own native `Invocable`, callable bare BY
//!   NAME from an ordinary literal expression (DMN-TCK 0014-loan-comparison's `FinancialMetrics`);
//! - an `<import>`ed model's own BKM exposed (alongside its decisions) on the import alias map
//!   (DMN-TCK 0086-import's `myimport.Say Hello(A Person)`).

use sutra_dmn::load_drg;
use sutra_feel::FeelValue;

fn value_of(model: &[u8], decision_name: &str) -> FeelValue {
    let drg = load_drg(model).expect("model should load");
    let out = drg.evaluate(&Default::default());
    out.get(decision_name)
        .unwrap_or_else(|| panic!("no result for decision '{decision_name}' — got {out:?}"))
        .clone()
        .unwrap_or_else(|e| panic!("decision '{decision_name}' errored: {e}"))
}

/// A BKM ("Outer Helper") whose own literal-expression body calls ANOTHER multi-word-named BKM
/// ("Inner Helper") by bare name. Before cycle 6's fix, `build_bkm_function`'s parse of "Outer
/// Helper"'s body used `sutra_feel::expressions::parse` (context-free — no "names with spaces"
/// merge), so the lexer never fused "Inner"+"Helper" into one call-name token, the parse failed,
/// and "Outer Helper" was silently dropped from the callable prelude entirely.
const BKM_CHAIN_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:bkm-chain" name="bkm_chain_test">
    <businessKnowledgeModel name="Inner Helper" id="_inner">
        <encapsulatedLogic>
            <formalParameter name="x" typeRef="number"/>
            <literalExpression><text>x + 1</text></literalExpression>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <businessKnowledgeModel name="Outer Helper" id="_outer">
        <encapsulatedLogic>
            <formalParameter name="x" typeRef="number"/>
            <literalExpression><text>Inner Helper(x) * 2</text></literalExpression>
        </encapsulatedLogic>
        <knowledgeRequirement><requiredKnowledge href="#_inner"/></knowledgeRequirement>
    </businessKnowledgeModel>
    <decision name="Result" id="_result">
        <variable name="Result"/>
        <literalExpression><text>Outer Helper(5)</text></literalExpression>
    </decision>
</definitions>"##;

#[test]
fn bkm_chain_calling_another_multi_word_named_bkm_resolves() {
    // Inner Helper(5) = 6; Outer Helper(5) = 6 * 2 = 12.
    assert_eq!(
        value_of(BKM_CHAIN_MODEL.as_bytes(), "Result"),
        FeelValue::num("12")
    );
}

/// A `<decisionService>` ("Doubler") plus a decision ("CallsDoubler") that invokes it
/// INDIRECTLY — a bare call from an ordinary literal expression, not the structured
/// `<invocation>` element and not the TCK harness's own direct `invocableName` dispatch.
const DECISION_SERVICE_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:ds-invocable" name="ds_invocable_test">
    <inputData name="n" id="_n">
        <variable name="n" typeRef="number"/>
    </inputData>
    <decision name="doubled" id="_doubled">
        <variable name="doubled"/>
        <informationRequirement><requiredInput href="#_n"/></informationRequirement>
        <literalExpression><text>n * 2</text></literalExpression>
    </decision>
    <decisionService name="Doubler" id="_ds">
        <variable name="Doubler"/>
        <outputDecision href="#_doubled"/>
        <inputData href="#_n"/>
    </decisionService>
    <decision name="CallsDoubler" id="_calls">
        <variable name="CallsDoubler"/>
        <knowledgeRequirement><requiredKnowledge href="#_ds"/></knowledgeRequirement>
        <literalExpression><text>Doubler(21)</text></literalExpression>
    </decision>
    <decision name="CallsDoublerBadArity" id="_calls_bad">
        <variable name="CallsDoublerBadArity"/>
        <knowledgeRequirement><requiredKnowledge href="#_ds"/></knowledgeRequirement>
        <literalExpression><text>Doubler(21, 99)</text></literalExpression>
    </decision>
</definitions>"##;

#[test]
fn indirect_decision_service_call_computes_the_correct_value() {
    assert_eq!(
        value_of(DECISION_SERVICE_MODEL.as_bytes(), "CallsDoubler"),
        FeelValue::num("42")
    );
}

#[test]
fn indirect_decision_service_call_with_wrong_arity_is_never_invoked() {
    // DMN's "the service is never invoked on a bad call" semantics (DMN-TCK
    // 0085-decision-services#005/#007/#008): a wrong-arity positional call is `null`, not a
    // partial/best-effort invocation.
    assert_eq!(
        value_of(DECISION_SERVICE_MODEL.as_bytes(), "CallsDoublerBadArity"),
        FeelValue::Null
    );
}

/// A `<relation>` boxed expression — DMN-TCK 0016-some-every's `priceTable1`: a table of rows,
/// each becoming a context keyed by the declared column names.
const RELATION_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:relation" name="relation_test">
    <decision name="priceTable" id="_price_table">
        <variable name="priceTable"/>
        <relation>
            <column name="itemName" id="_col_name"/>
            <column name="price" id="_col_price"/>
            <row id="_row1">
                <literalExpression><text>"widget"</text></literalExpression>
                <literalExpression><text>25</text></literalExpression>
            </row>
            <row id="_row2">
                <literalExpression><text>"trinket"</text></literalExpression>
                <literalExpression><text>1.5</text></literalExpression>
            </row>
        </relation>
    </decision>
    <decision name="everyGtTen" id="_every_gt_ten">
        <variable name="everyGtTen"/>
        <informationRequirement><requiredDecision href="#_price_table"/></informationRequirement>
        <literalExpression><text>every i in priceTable satisfies i.price &gt; 10</text></literalExpression>
    </decision>
</definitions>"##;

#[test]
fn boxed_relation_becomes_a_list_of_contexts() {
    assert_eq!(
        value_of(RELATION_MODEL.as_bytes(), "priceTable"),
        FeelValue::List(vec![
            FeelValue::Map(
                [
                    ("itemName".to_string(), FeelValue::from("widget")),
                    ("price".to_string(), FeelValue::num("25")),
                ]
                .into_iter()
                .collect()
            ),
            FeelValue::Map(
                [
                    ("itemName".to_string(), FeelValue::from("trinket")),
                    ("price".to_string(), FeelValue::num("1.5")),
                ]
                .into_iter()
                .collect()
            ),
        ])
    );
    // A row's price (1.5) fails `> 10`, so `every` must be false — not silently true because the
    // relation's own value was previously invisible to the quantifier.
    assert_eq!(
        value_of(RELATION_MODEL.as_bytes(), "everyGtTen"),
        FeelValue::Boolean(false)
    );
}

/// A BKM whose `<encapsulatedLogic>` body is a boxed CONTEXT (not a plain literal expression) —
/// DMN-TCK 0014-loan-comparison's `FinancialMetrics`, bare-called from an ordinary literal
/// expression. `build_bkm_function`'s load-time literal prelude can't represent this (there's no
/// FEEL AST for a boxed context), so it needs the native `Invocable` path instead.
const CONTEXT_BODIED_BKM_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:context-bkm" name="context_bkm_test">
    <businessKnowledgeModel name="Metrics" id="_metrics">
        <variable name="Metrics"/>
        <encapsulatedLogic>
            <formalParameter typeRef="number" name="amount"/>
            <context>
                <contextEntry>
                    <variable name="doubled"/>
                    <literalExpression><text>amount * 2</text></literalExpression>
                </contextEntry>
                <contextEntry>
                    <variable name="tripled"/>
                    <literalExpression><text>amount * 3</text></literalExpression>
                </contextEntry>
            </context>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <decision name="Result" id="_result">
        <variable name="Result"/>
        <knowledgeRequirement><requiredKnowledge href="#_metrics"/></knowledgeRequirement>
        <literalExpression><text>Metrics(10).tripled</text></literalExpression>
    </decision>
</definitions>"##;

#[test]
fn context_bodied_bkm_is_callable_bare_by_name() {
    assert_eq!(
        value_of(CONTEXT_BODIED_BKM_MODEL.as_bytes(), "Result"),
        FeelValue::num("30")
    );
}

/// DMN-TCK 0086-import: the MAIN model imports a sibling model under alias "lib" and calls the
/// sibling's OWN BKM ("Greet") via `lib.Greet(...)` — a field access on the import-alias map,
/// then a call. `Greet` must be exposed on that map alongside the sibling's decisions.
const IMPORTED_LIB_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:import-lib" name="lib">
    <businessKnowledgeModel name="Greet" id="_greet">
        <variable name="Greet"/>
        <encapsulatedLogic>
            <formalParameter typeRef="string" name="name"/>
            <literalExpression><text>"Hello " + name</text></literalExpression>
        </encapsulatedLogic>
    </businessKnowledgeModel>
</definitions>"##;

const IMPORTING_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:importing" name="importing_test">
    <import namespace="urn:test:import-lib" name="lib" importType="https://www.omg.org/spec/DMN/20230324/MODEL/"/>
    <decision name="Result" id="_result">
        <variable name="Result"/>
        <literalExpression><text>lib.Greet("Sam")</text></literalExpression>
    </decision>
</definitions>"##;

/// DMN-TCK 0037-dt-on-bkm-implicit-params: a BKM invoked via a structured `<invocation>` whose
/// formal parameters are declared with DOTTED names (`Person.Gender`, `Person.Name`) — meant to
/// be re-exposed, inside the BKM's own decision table, via ordinary FEEL navigation
/// (`Person.Gender`) rather than a flat context key literally named "Person.Gender" (which a bare
/// `.` in the table's `<inputExpression>` text always tokenizes as navigation, never one
/// identifier — there is no dot-aware "names with spaces"-style merge pass for it).
const DOTTED_PARAM_BKM_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:dotted-param" name="dotted_param_test">
    <businessKnowledgeModel name="Description" id="_description">
        <variable name="Description"/>
        <encapsulatedLogic>
            <formalParameter typeRef="string" name="Person.Gender"/>
            <decisionTable hitPolicy="UNIQUE">
                <input id="_in1"><inputExpression typeRef="string"><text>Person.Gender</text></inputExpression></input>
                <output id="_out1"/>
                <rule id="_r1"><inputEntry><text>"Male"</text></inputEntry><outputEntry><text>"He"</text></outputEntry></rule>
                <rule id="_r2"><inputEntry><text>"Female"</text></inputEntry><outputEntry><text>"She"</text></outputEntry></rule>
            </decisionTable>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <decision name="Result" id="_result">
        <variable name="Result"/>
        <knowledgeRequirement><requiredKnowledge href="#_description"/></knowledgeRequirement>
        <invocation>
            <literalExpression><text>Description</text></literalExpression>
            <binding>
                <parameter name="Person.Gender"/>
                <literalExpression><text>"Female"</text></literalExpression>
            </binding>
        </invocation>
    </decision>
</definitions>"##;

#[test]
fn invocation_binding_a_dotted_formal_parameter_name_is_navigable_inside_the_bkm() {
    assert_eq!(
        value_of(DOTTED_PARAM_BKM_MODEL.as_bytes(), "Result"),
        FeelValue::from("She")
    );
}

#[test]
fn imported_models_own_bkm_is_exposed_on_the_import_alias_map() {
    let resolve = |namespace: &str| -> Option<Vec<u8>> {
        if namespace == "urn:test:import-lib" {
            Some(IMPORTED_LIB_MODEL.as_bytes().to_vec())
        } else {
            None
        }
    };
    let drg = sutra_dmn::load_drg_with_imports(IMPORTING_MODEL.as_bytes(), &resolve)
        .expect("model should load");
    let out = drg.evaluate(&Default::default());
    assert_eq!(
        out.get("Result")
            .unwrap_or_else(|| panic!("no result for 'Result' — got {out:?}"))
            .clone()
            .unwrap_or_else(|e| panic!("'Result' errored: {e}")),
        FeelValue::from("Hello Sam")
    );
}
