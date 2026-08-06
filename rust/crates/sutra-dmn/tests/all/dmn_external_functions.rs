//! DMN external function definitions (`kind="Java"`/`"PMML"` — DMN 1.4 §10.3.2.13.3, DMN-TCK
//! 0076-feel-external-java): a model carrying one must LOAD (external functions are an optional
//! feature; the definition itself is valid), and every invocation path — bare call of a BKM,
//! structured `<invocation>`, a boxed `<functionDefinition kind="Java">` context entry — must be
//! rejected with the deliberate semantic code `SUTRA.FEEL.EVAL.EXTERNAL_UNSUPPORTED` (never a
//! `SYNTAX.*` code; see the TCK harness's errorResult crediting).

use sutra_dmn::load_drg;
use sutra_feel::FeelValue;

fn error_of(model: &[u8], decision_name: &str) -> String {
    let drg = load_drg(model).expect("model with an external function definition must load");
    let out = drg.evaluate(&Default::default());
    out.get(decision_name)
        .unwrap_or_else(|| panic!("no result for decision '{decision_name}' — got {out:?}"))
        .clone()
        .expect_err("invoking an external function must error")
}

/// A BKM whose `<encapsulatedLogic kind="Java">` body is the boxed java binding context, bare-
/// called from a decision's literal expression.
const JAVA_BKM_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:external-bkm" name="external_bkm_test">
    <businessKnowledgeModel name="maxDouble" id="_maxDouble">
        <encapsulatedLogic kind="Java">
            <formalParameter name="d1" typeRef="number"/>
            <formalParameter name="d2" typeRef="number"/>
            <context>
                <contextEntry>
                    <variable name="class"/>
                    <literalExpression><text>"java.lang.Math"</text></literalExpression>
                </contextEntry>
                <contextEntry>
                    <variable name="method signature"/>
                    <literalExpression><text>"max(double, double)"</text></literalExpression>
                </contextEntry>
            </context>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <decision name="callsExternal" id="_callsExternal">
        <variable name="callsExternal"/>
        <knowledgeRequirement><requiredKnowledge href="#_maxDouble"/></knowledgeRequirement>
        <literalExpression><text>maxDouble(123, 456)</text></literalExpression>
    </decision>
    <decision name="invokesExternal" id="_invokesExternal">
        <variable name="invokesExternal"/>
        <knowledgeRequirement><requiredKnowledge href="#_maxDouble"/></knowledgeRequirement>
        <invocation>
            <literalExpression><text>maxDouble</text></literalExpression>
            <binding>
                <parameter name="d1"/>
                <literalExpression><text>123</text></literalExpression>
            </binding>
            <binding>
                <parameter name="d2"/>
                <literalExpression><text>456</text></literalExpression>
            </binding>
        </invocation>
    </decision>
</definitions>"##;

#[test]
fn java_bkm_loads_and_bare_call_is_rejected_at_invocation() {
    let err = error_of(JAVA_BKM_MODEL.as_bytes(), "callsExternal");
    assert!(
        err.contains("EXTERNAL_UNSUPPORTED")
            && err.contains("java.lang.Math")
            && err.contains("max(double, double)"),
        "unexpected error: {err}"
    );
    assert!(!err.contains("SYNTAX"), "must not be syntax-classed: {err}");
}

#[test]
fn java_bkm_structured_invocation_is_rejected_too() {
    let err = error_of(JAVA_BKM_MODEL.as_bytes(), "invokesExternal");
    assert!(
        err.contains("EXTERNAL_UNSUPPORTED") && err.contains("java.lang.Math"),
        "unexpected error: {err}"
    );
}

/// DMN-TCK 0076 `boxed_001`'s shape: a decision's boxed context defines the external function in
/// one entry (`<functionDefinition kind="Java">` with a boxed context body) and calls it in the
/// result entry.
const BOXED_JAVA_DECISION_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:external-boxed" name="external_boxed_test">
    <decision name="boxedExternal" id="_boxedExternal">
        <variable name="boxedExternal"/>
        <context>
            <contextEntry>
                <variable name="maxDouble"/>
                <functionDefinition kind="Java">
                    <formalParameter typeRef="number" name="d1"/>
                    <formalParameter typeRef="number" name="d2"/>
                    <context>
                        <contextEntry>
                            <variable name="class"/>
                            <literalExpression><text>"java.lang.Math"</text></literalExpression>
                        </contextEntry>
                        <contextEntry>
                            <variable name="method signature"/>
                            <literalExpression><text>"max(double, double)"</text></literalExpression>
                        </contextEntry>
                    </context>
                </functionDefinition>
            </contextEntry>
            <contextEntry>
                <literalExpression><text>maxDouble(123, 456)</text></literalExpression>
            </contextEntry>
        </context>
    </decision>
</definitions>"##;

#[test]
fn boxed_function_definition_kind_java_loads_and_is_rejected_at_invocation() {
    let err = error_of(BOXED_JAVA_DECISION_MODEL.as_bytes(), "boxedExternal");
    assert!(
        err.contains("EXTERNAL_UNSUPPORTED")
            && err.contains("java.lang.Math")
            && err.contains("max(double, double)"),
        "unexpected error: {err}"
    );
    assert!(!err.contains("SYNTAX"), "must not be syntax-classed: {err}");
}

/// `kind="PMML"` takes the same path with the pmml binding shape; defining is loadable, invoking
/// is rejected, and the diagnostic names the document.
const PMML_BKM_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:external-pmml" name="external_pmml_test">
    <businessKnowledgeModel name="scorer" id="_scorer">
        <encapsulatedLogic kind="PMML">
            <formalParameter name="x" typeRef="number"/>
            <context>
                <contextEntry>
                    <variable name="document"/>
                    <literalExpression><text>"models/scorecard.pmml"</text></literalExpression>
                </contextEntry>
                <contextEntry>
                    <variable name="model"/>
                    <literalExpression><text>"score"</text></literalExpression>
                </contextEntry>
            </context>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <decision name="callsPmml" id="_callsPmml">
        <variable name="callsPmml"/>
        <knowledgeRequirement><requiredKnowledge href="#_scorer"/></knowledgeRequirement>
        <literalExpression><text>scorer(5)</text></literalExpression>
    </decision>
</definitions>"##;

#[test]
fn pmml_bkm_loads_and_is_rejected_at_invocation() {
    let err = error_of(PMML_BKM_MODEL.as_bytes(), "callsPmml");
    assert!(
        err.contains("EXTERNAL_UNSUPPORTED") && err.contains("models/scorecard.pmml"),
        "unexpected error: {err}"
    );
}

/// The BKM value itself (not invoked) is still a perfectly good function VALUE — a decision can
/// reference it without erroring (only invocation is rejected).
#[test]
fn external_bkm_as_a_bare_value_does_not_error() {
    let model = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:external-value" name="external_value_test">
    <businessKnowledgeModel name="maxDouble" id="_maxDouble">
        <encapsulatedLogic kind="Java">
            <formalParameter name="d1" typeRef="number"/>
            <context>
                <contextEntry>
                    <variable name="class"/>
                    <literalExpression><text>"java.lang.Math"</text></literalExpression>
                </contextEntry>
                <contextEntry>
                    <variable name="method signature"/>
                    <literalExpression><text>"max(double, double)"</text></literalExpression>
                </contextEntry>
            </context>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <decision name="isFunction" id="_isFunction">
        <variable name="isFunction"/>
        <knowledgeRequirement><requiredKnowledge href="#_maxDouble"/></knowledgeRequirement>
        <literalExpression><text>maxDouble instance of function</text></literalExpression>
    </decision>
</definitions>"##;
    let drg = load_drg(model.as_bytes()).expect("model must load");
    let out = drg.evaluate(&Default::default());
    let result = out
        .get("isFunction")
        .expect("decision result present")
        .clone()
        .expect("referencing the external BKM as a value must not error");
    assert_eq!(result, FeelValue::Boolean(true));
}
