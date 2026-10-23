//! BPMN's own data vocabulary as a front door onto the declared-variable model:
//! `<bpmn:property>`, `<bpmn:dataObject>` / `<bpmn:dataObjectReference>` named by a data
//! association, and `<bpmn:itemDefinition>` as the type source. `<q:variables>` stays
//! authoritative — it is the only spelling that can say `transient` / `sensitive` / `subjectKey` /
//! `source` — so it wins a name collision outright, and the loser is recorded for `sutra lint`.

use sutra_bpmn::model::BpmnDataSpelling;
use sutra_bpmn::{BpmnModelLoader, DeclaredVariable, FieldType, ProcessDefinition};

fn load(bpmn: &str) -> ProcessDefinition {
    let module = BpmnModelLoader::new().load(bpmn.as_bytes()).unwrap();
    module.processes()[0].clone()
}

fn var<'a>(p: &'a ProcessDefinition, name: &str) -> &'a DeclaredVariable {
    p.declared_variables
        .iter()
        .find(|v| v.name == name)
        .unwrap_or_else(|| {
            panic!(
                "no declared variable '{name}'; got {:?}",
                p.declared_variables
                    .iter()
                    .map(|v| &v.name)
                    .collect::<Vec<_>>()
            )
        })
}

/// A process whose `<property>` / data-object declarations are spelled in BPMN, with `extra`
/// spliced in for the `<q:variables>` and `<itemDefinition>` variants.
fn with_data(items: &str, process_head: &str, task_body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0"
                          xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          {items}
          <bpmn:process id="p1">
            {process_head}
            <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="x.hbs">
              <bpmn:incoming>f1</bpmn:incoming>
              <bpmn:outgoing>f2</bpmn:outgoing>
              {task_body}
            </bpmn:serviceTask>
            <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

#[test]
fn bpmn_property_becomes_a_declared_variable() {
    let p = load(&with_data(
        "",
        r#"<bpmn:property id="prop1" name="orderRef"/>"#,
        "",
    ));
    assert_eq!(
        var(&p, "orderRef"),
        &DeclaredVariable::scalar("orderRef", FieldType::Any),
        "a <property> with no resolvable itemDefinition declares an untyped variable, exactly as \
         a <q:variable> with no @type does"
    );
    assert!(p.shadowed_bpmn_variables.is_empty());
}

#[test]
fn item_definition_structure_ref_types_the_property() {
    let p = load(&with_data(
        r#"<bpmn:itemDefinition id="amountItem" structureRef="xsd:decimal"/>
           <bpmn:itemDefinition id="refItem" structureRef="xsd:string"/>"#,
        r#"<bpmn:property id="prop1" name="amount" itemSubjectRef="amountItem"/>
           <bpmn:property id="prop2" name="orderRef" itemSubjectRef="tns:refItem"/>
           <bpmn:property id="prop3" name="payload" itemSubjectRef="tns:someComplexType"/>"#,
        "",
    ));
    assert_eq!(var(&p, "amount").ty, FieldType::Number);
    assert_eq!(
        var(&p, "orderRef").ty,
        FieldType::String,
        "an @itemSubjectRef is a QName; the prefix is whatever the tool bound, so only the local \
         part is matched against the itemDefinition id"
    );
    assert_eq!(
        var(&p, "payload").ty,
        FieldType::Any,
        "an unresolvable or non-scalar structure leaves the type unsaid rather than guessing"
    );
}

#[test]
fn associated_data_object_reference_declares_a_variable_and_carries_no_token() {
    let p = load(&with_data(
        r#"<bpmn:itemDefinition id="flagItem" structureRef="xsd:boolean"/>"#,
        r#"<bpmn:dataObject id="do1" name="approved" itemSubjectRef="flagItem"/>
           <bpmn:dataObjectReference id="doRef1" dataObjectRef="do1"/>
           <bpmn:dataObject id="unused" name="neverReferenced"/>"#,
        r#"<bpmn:dataOutputAssociation><bpmn:targetRef>doRef1</bpmn:targetRef></bpmn:dataOutputAssociation>"#,
    ));
    assert_eq!(
        var(&p, "approved"),
        &DeclaredVariable::scalar("approved", FieldType::Boolean),
        "a reference with no name/type of its own stands for its <dataObject>, so both come from \
         the target"
    );
    assert!(
        !p.declared_variables
            .iter()
            .any(|v| v.name == "neverReferenced"),
        "a data object no association names is inert; declaring it would invent state nothing \
         reads or writes"
    );
    for node in p.nodes() {
        assert!(
            !matches!(node.id(), "do1" | "doRef1" | "unused"),
            "a data object must stay non-token-carrying: this is data DECLARATION, not flow"
        );
    }
}

#[test]
fn q_variable_wins_a_name_collision_and_the_loser_is_recorded() {
    let p = load(&with_data(
        r#"<bpmn:itemDefinition id="numItem" structureRef="xsd:int"/>"#,
        r#"<bpmn:extensionElements>
             <q:variables>
               <q:variable name="amount" transient="true" sensitive="true" subjectKey="true"/>
             </q:variables>
           </bpmn:extensionElements>
           <bpmn:property id="prop1" name="amount" itemSubjectRef="numItem"/>"#,
        "",
    ));
    assert_eq!(
        p.declared_variables.len(),
        1,
        "one name, one declaration: {:?}",
        p.declared_variables
    );
    let v = var(&p, "amount");
    assert!(
        v.transient && v.sensitive && v.subject_key,
        "the <q:variable> must survive intact — it says what BPMN cannot"
    );
    assert_eq!(
        v.ty,
        FieldType::Any,
        "a <q:variable> with no @type means 'any' deliberately; the BPMN type does not fill it in"
    );
    assert_eq!(p.shadowed_bpmn_variables.len(), 1);
    let shadowed = &p.shadowed_bpmn_variables[0];
    assert_eq!(shadowed.name, "amount");
    assert_eq!(shadowed.element_id, "p1");
    assert_eq!(shadowed.spelling, BpmnDataSpelling::Property);
}

#[test]
fn a_bpmn_spelling_contributes_name_and_type_only() {
    // The engine attributes are not in BPMN's vocabulary at all, so the only way to assert they
    // cannot be set this way is to spell them where a BPMN author would reach for them — as
    // attributes on the element — and check they are ignored rather than honoured.
    let p = load(&with_data(
        "",
        r#"<bpmn:property id="prop1" name="secret" transient="true" sensitive="true"
                          subjectKey="true" source="orders.in"/>
           <bpmn:dataObject id="do1" name="held" sensitive="true"/>
           <bpmn:dataObjectReference id="doRef1" dataObjectRef="do1"/>"#,
        r#"<bpmn:dataInputAssociation><bpmn:sourceRef>doRef1</bpmn:sourceRef></bpmn:dataInputAssociation>"#,
    ));
    for name in ["secret", "held"] {
        let v = var(&p, name);
        assert_eq!(
            v,
            &DeclaredVariable::scalar(name, FieldType::Any),
            "a BPMN spelling may contribute a name and a type and nothing else; persistence, \
             redaction, subject indexing and intake binding are sayable only in <q:variables>"
        );
    }
}
