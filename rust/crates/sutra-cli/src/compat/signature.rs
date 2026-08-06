//! Structural signature of a BPMN file: process ids, start/end event ids, named task ids
//! and start-event message refs — exactly the element set whose REMOVAL is a breaking
//! change. Extraction is a small streaming XML walk, not an engine parse: compatibility
//! checks need structural ids only, no semantic resolution.

use std::path::Path;

use crate::bpmn_walk::{attr, local_name, walk_bpmn, WalkEvent};

/// Signature of one BPMN file, keyed by (relative) file path when scanned from a tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BpmnSignature {
    pub file_path: String,
    pub processes: Vec<ProcessSignature>,
}

/// Per-process structural contents. Id lists preserve document order and are duplicate-free.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessSignature {
    pub id: String,
    pub name: Option<String>,
    pub start_event_ids: Vec<String>,
    pub end_event_ids: Vec<String>,
    pub user_task_ids: Vec<String>,
    pub service_task_ids: Vec<String>,
    pub script_task_ids: Vec<String>,
    /// `messageRef` attributes of `messageEventDefinition` elements INSIDE start events
    /// only — the dispatch-relevant subscriptions.
    pub referenced_message_ids: Vec<String>,
}

impl BpmnSignature {
    /// Extract a signature from a BPMN file on disk.
    pub fn extract(file: &Path) -> Result<BpmnSignature, String> {
        let xml = std::fs::read_to_string(file)
            .map_err(|e| format!("cannot read {}: {e}", file.display()))?;
        Self::extract_from_str(&file.display().to_string(), &xml)
    }

    /// Extract a signature from BPMN text; `file_path` is carried for reporting only.
    pub fn extract_from_str(file_path: &str, xml: &str) -> Result<BpmnSignature, String> {
        let mut processes: Vec<ProcessSignature> = Vec::new();
        let mut current: Option<ProcessSignature> = None;
        let mut in_start_event = false;

        walk_bpmn(xml, |event| {
            let closes_immediately = matches!(event, WalkEvent::Empty(_));
            match event {
                WalkEvent::Start(e) | WalkEvent::Empty(e) => match local_name(e).as_str() {
                    "process" => {
                        let sig = ProcessSignature {
                            id: attr(e, "id").unwrap_or_default(),
                            name: attr(e, "name"),
                            ..ProcessSignature::default()
                        };
                        if closes_immediately {
                            if !sig.id.is_empty() {
                                processes.push(sig);
                            }
                        } else {
                            current = Some(sig);
                        }
                    }
                    "startEvent" => {
                        if let Some(p) = current.as_mut() {
                            if let Some(id) = attr(e, "id") {
                                push_unique(&mut p.start_event_ids, id);
                            }
                            if !closes_immediately {
                                in_start_event = true;
                            }
                        }
                    }
                    "endEvent" => {
                        if let (Some(p), Some(id)) = (current.as_mut(), attr(e, "id")) {
                            push_unique(&mut p.end_event_ids, id);
                        }
                    }
                    "userTask" => {
                        if let (Some(p), Some(id)) = (current.as_mut(), attr(e, "id")) {
                            push_unique(&mut p.user_task_ids, id);
                        }
                    }
                    "serviceTask" => {
                        if let (Some(p), Some(id)) = (current.as_mut(), attr(e, "id")) {
                            push_unique(&mut p.service_task_ids, id);
                        }
                    }
                    "scriptTask" => {
                        if let (Some(p), Some(id)) = (current.as_mut(), attr(e, "id")) {
                            push_unique(&mut p.script_task_ids, id);
                        }
                    }
                    "messageEventDefinition" if in_start_event => {
                        if let (Some(p), Some(m)) = (current.as_mut(), attr(e, "messageRef")) {
                            push_unique(&mut p.referenced_message_ids, m);
                        }
                    }
                    _ => {}
                },
                WalkEvent::End(name) => match name.as_str() {
                    "process" => {
                        if let Some(p) = current.take() {
                            if !p.id.is_empty() {
                                processes.push(p);
                            }
                        }
                    }
                    "startEvent" => in_start_event = false,
                    _ => {}
                },
            }
        })
        .map_err(|e| format!("failed to parse BPMN file {file_path}: {e}"))?;

        Ok(BpmnSignature {
            file_path: file_path.to_owned(),
            processes,
        })
    }
}

fn push_unique(list: &mut Vec<String>, value: String) {
    if !list.contains(&value) {
        list.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fixture carried over from the reference baseline's test suite.
    const SAMPLE_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  targetNamespace="http://example/">
  <bpmn:message id="msgA" name="A"/>
  <bpmn:process id="p1" name="ProcessOne">
    <bpmn:startEvent id="s1">
      <bpmn:messageEventDefinition messageRef="msgA"/>
    </bpmn:startEvent>
    <bpmn:userTask id="t1"/>
    <bpmn:serviceTask id="t2"/>
    <bpmn:scriptTask id="t3"/>
    <bpmn:endEvent id="e1"/>
  </bpmn:process>
  <bpmn:process id="p2">
    <bpmn:startEvent id="s2"/>
    <bpmn:endEvent id="e2"/>
  </bpmn:process>
</bpmn:definitions>
"#;

    #[test]
    fn extracts_process_ids_starts_ends_and_tasks() {
        let sig = BpmnSignature::extract_from_str("sample.bpmn", SAMPLE_BPMN).unwrap();
        assert_eq!(sig.file_path, "sample.bpmn");
        assert_eq!(sig.processes.len(), 2);

        let p1 = &sig.processes[0];
        assert_eq!(p1.id, "p1");
        assert_eq!(p1.name.as_deref(), Some("ProcessOne"));
        assert_eq!(p1.start_event_ids, vec!["s1"]);
        assert_eq!(p1.end_event_ids, vec!["e1"]);
        assert_eq!(p1.user_task_ids, vec!["t1"]);
        assert_eq!(p1.service_task_ids, vec!["t2"]);
        assert_eq!(p1.script_task_ids, vec!["t3"]);
        assert_eq!(p1.referenced_message_ids, vec!["msgA"]);

        let p2 = &sig.processes[1];
        assert_eq!(p2.id, "p2");
        assert_eq!(p2.start_event_ids, vec!["s2"]);
        assert_eq!(p2.end_event_ids, vec!["e2"]);
        assert!(p2.user_task_ids.is_empty());
        assert!(p2.referenced_message_ids.is_empty());
    }

    #[test]
    fn message_ref_only_captured_from_start_events() {
        let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  targetNamespace="http://example/">
  <bpmn:message id="msgB" name="B"/>
  <bpmn:process id="p1">
    <bpmn:startEvent id="s1"/>
    <bpmn:endEvent id="e1">
      <bpmn:messageEventDefinition messageRef="msgB"/>
    </bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#;
        let sig = BpmnSignature::extract_from_str("x.bpmn", bpmn).unwrap();
        assert!(sig.processes[0].referenced_message_ids.is_empty());
    }

    #[test]
    fn malformed_xml_is_an_error() {
        let err =
            BpmnSignature::extract_from_str("bad.bpmn", "<not><closed-properly>").unwrap_err();
        assert!(err.contains("failed to parse BPMN file bad.bpmn"), "{err}");
    }
}
