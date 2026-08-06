//! Test-support stand-in for the structural codec (the module codec compiled from
//! the example XSDs — the full schema-bound codec family lives elsewhere). Enough of the real
//! contract to drive the shipped channels end-to-end:
//!
//! - accepts xml/json/yaml (the codec-manifest `formats`),
//! - the message type is the document root (xml) / the single top-level field (json/yaml),
//! - the payload is the root's children as a FEEL-walkable map,
//! - leaf values are coerced per the XSD's declared leaf types (`xs:decimal`/`xs:integer`
//!   → number, `xs:boolean` → boolean) so FEEL arithmetic is numeric — the
//!   `StructuralCodec.applySchemaTypes` behaviour.
#![allow(dead_code)]

use std::collections::HashSet;

use quick_xml::events::Event;
use quick_xml::reader::Reader;
use sutra_codec_spi::{CodecValue, DecodeResult, PayloadCodec};
use sutra_formats::{JsonCodec, XmlCodec, YamlCodec};

/// A process-wide tokio runtime for the sync `#[test]`s: [`drive`] block_ons engine
/// futures on it, mirroring how a shard lane's actor loop awaits each request to
/// completion (the engine surface is fully async since Phase 3 — `ChannelEngine::builder`
/// takes no runtime handle any more). The runtime is kept alive for the whole test
/// binary; the sync tests run on non-runtime threads, so the `block_on` is never nested.
pub fn test_runtime() -> tokio::runtime::Handle {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime")
    })
    .handle()
    .clone()
}

/// Drive one engine future to completion from a sync `#[test]` — the test-side stand-in
/// for a shard lane's actor loop (one request awaited to completion at a time, on one
/// thread). Must NOT be called from inside an async test (`Handle::block_on` panics in a
/// runtime context); async tests go through `EngineHandle` and `.await` instead.
pub fn drive<T>(fut: impl std::future::Future<Output = T>) -> T {
    test_runtime().block_on(fut)
}

pub struct StructuralStandInCodec {
    urn: String,
    roots: HashSet<String>,
    number_fields: HashSet<String>,
    boolean_fields: HashSet<String>,
}

impl StructuralStandInCodec {
    /// Scan the XSD for its global roots and typed leaf elements.
    // `Attribute::unescape_value` is deprecated in quick-xml 0.41 (its replacement
    // `normalized_value` collapses in-value whitespace); this test stand-in keeps the
    // exact 0.37 entity-only semantics, so the deprecation is allowed deliberately.
    #[allow(deprecated)]
    pub fn compile(urn: &str, xsd: &[u8]) -> StructuralStandInCodec {
        let mut reader = Reader::from_reader(xsd);
        reader.config_mut().expand_empty_elements = true;
        let mut depth = 0usize;
        let mut roots = HashSet::new();
        let mut number_fields = HashSet::new();
        let mut boolean_fields = HashSet::new();
        loop {
            match reader.read_event() {
                Ok(Event::Start(e)) => {
                    depth += 1;
                    let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                    if local == "element" {
                        let mut name = None;
                        let mut type_ref = None;
                        for a in e.attributes().flatten() {
                            let key = String::from_utf8_lossy(a.key.as_ref()).into_owned();
                            let value = a.unescape_value().unwrap_or_default().into_owned();
                            match key.as_str() {
                                "name" => name = Some(value),
                                "type" => type_ref = Some(value),
                                _ => {}
                            }
                        }
                        if let Some(name) = name {
                            // Depth 2 = a direct child of <xs:schema> — a global root.
                            if depth == 2 {
                                roots.insert(name.clone());
                            }
                            match type_ref.as_deref().map(local_type) {
                                Some("decimal") | Some("integer") | Some("int") | Some("long") => {
                                    number_fields.insert(name);
                                }
                                Some("boolean") => {
                                    boolean_fields.insert(name);
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Ok(Event::End(_)) => depth = depth.saturating_sub(1),
                Ok(Event::Eof) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        StructuralStandInCodec {
            urn: urn.to_string(),
            roots,
            number_fields,
            boolean_fields,
        }
    }

    fn coerce(&self, value: serde_json::Value, key: Option<&str>) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(k, v)| {
                        let coerced = self.coerce(v, Some(&k));
                        (k, coerced)
                    })
                    .collect(),
            ),
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(|v| self.coerce(v, key)).collect())
            }
            serde_json::Value::String(s) => {
                if let Some(k) = key {
                    if self.number_fields.contains(k) {
                        if let Ok(n) = s.trim().parse::<serde_json::Number>() {
                            return serde_json::Value::Number(n);
                        }
                    }
                    if self.boolean_fields.contains(k) {
                        match s.trim() {
                            t if t.eq_ignore_ascii_case("true") => {
                                return serde_json::Value::Bool(true)
                            }
                            t if t.eq_ignore_ascii_case("false") => {
                                return serde_json::Value::Bool(false)
                            }
                            _ => {}
                        }
                    }
                }
                serde_json::Value::String(s)
            }
            other => other,
        }
    }
}

enum Kind {
    Xml,
    Json,
    Yaml,
}

/// The structural codec's `detect`: content-type first, else sniff the first byte.
fn detect(body: &[u8], content_type: Option<&str>) -> Kind {
    let ct = content_type.unwrap_or("").to_lowercase();
    if ct.contains("xml") {
        return Kind::Xml;
    }
    if ct.contains("json") {
        return Kind::Json;
    }
    if ct.contains("yaml") || ct.contains("yml") {
        return Kind::Yaml;
    }
    for b in body {
        match b {
            b' ' | b'\n' | b'\r' | b'\t' => continue,
            b'<' => return Kind::Xml,
            b'{' | b'[' => return Kind::Json,
            _ => break,
        }
    }
    Kind::Yaml
}

fn local_type(type_ref: &str) -> &str {
    type_ref
        .rsplit_once(':')
        .map(|(_, l)| l)
        .unwrap_or(type_ref)
}

impl PayloadCodec for StructuralStandInCodec {
    fn name(&self) -> &str {
        &self.urn
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec![
            "application/xml".to_string(),
            "text/xml".to_string(),
            "application/*+xml".to_string(),
            "application/json".to_string(),
            "application/*+json".to_string(),
            "application/x-yaml".to_string(),
            "application/yaml".to_string(),
            "text/yaml".to_string(),
        ]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let inner = match detect(body, content_type) {
            Kind::Xml => XmlCodec.decode(body, content_type),
            Kind::Json => JsonCodec.decode(body, content_type),
            Kind::Yaml => YamlCodec.decode(body, content_type),
        };
        if inner.payload.is_none() {
            return inner; // FATAL rides through untouched
        }
        // XML already carries messageType (root) + root-children payload; json/yaml wrap
        // the message in a single top-level field naming the root.
        let (payload, message_type) = match (&inner.payload, &inner.message_type) {
            (Some(CodecValue::Json(v)), Some(mt)) => (v.clone(), Some(mt.clone())),
            (Some(CodecValue::Json(serde_json::Value::Object(map))), None) if map.len() == 1 => {
                let (root, value) = map.iter().next().expect("len checked");
                (value.clone(), Some(root.clone()))
            }
            (Some(CodecValue::Json(v)), None) => (v.clone(), None),
            _ => {
                return inner;
            }
        };
        let coerced = self.coerce(payload, None);
        let mut out = inner;
        out.payload = Some(CodecValue::Json(coerced));
        out.message_type = message_type;
        out
    }

    fn encode(
        &self,
        _payload: &CodecValue,
        _content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        Err("the structural stand-in is decode-only (replies are template renders)".to_string())
    }
}
