//! The inbound chain:
//! channel-scoped decode, the payload-view projection, the tier-1 + tier-2 validator
//! chain, the FROZEN `validation.*` summary variables, and the
//! `<q:onValidation>` policy.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use sutra_bpmn::qbindings::OnValidationMode;
use sutra_bpmn::ProcessDefinition;
use sutra_codec_spi::{
    CodecValue, DecodeOutcome, DecodeResult, IssueSeverity, PayloadCodec, ShapeClass,
    ValidationIssue,
};
use sutra_executor::telemetry;
use sutra_executor::DeploymentId;
use sutra_executor::Variables;
use sutra_feel::FeelValue;

use crate::codes;
use crate::config::ChannelBinding;
use crate::content_type;
use crate::diag::Diagnostic;
use crate::registry::{codec_not_found, CodecRegistry, FormatRegistry};
use crate::validators::{run_validator, ValidatorRegistry, ValidatorTier};
use crate::{mask_projection, run_redactor, ContentRedactor, RedactorRegistry};

/// Outcome of the inbound chain — `Proceed` starts the instance; the other two halt the
/// dispatch with the carried diagnostic.
#[derive(Debug, Clone)]
pub enum IntakeOutcome {
    /// Start the instance from the routed start event (`None` = the sole start event).
    Proceed { start_node_id: Option<String> },
    /// Transport-level reject (`<q:onValidation mode="reject">`).
    Reject(Diagnostic),
    /// BPMN-error posture (`mode="error"`) with the configured `errorCode`.
    Error {
        diagnostic: Diagnostic,
        error_code: String,
    },
}

/// Stateless inbound-chain executor — one instance per dispatcher.
///
/// The three read-only registries are held behind `Arc` (execution scale-out §2 row 10): they
/// are immutable once an activation has built them, so ONE set is built per activation and every
/// actor lane's chain points at it. The redactor registry is the exception and stays per-lane —
/// a template redactor carries a lazily-populated compiled-template cache (interior mutability),
/// which is per-lane state, not a shared immutable artifact.
pub struct InboundChain {
    codecs: Arc<CodecRegistry>,
    formats: Arc<FormatRegistry>,
    validators: Arc<ValidatorRegistry>,
    /// Built-in (`urn:sutra:redactor:<name>`) + deployment-scoped user redactors. Seeded via
    /// [`with_redactors`](InboundChain::with_redactors) at engine assembly; empty by default (the
    /// many lightweight test constructions).
    redactors: RedactorRegistry,
}

impl InboundChain {
    /// Each registry is taken as anything that converts into its `Arc` — a plain owned registry
    /// (the many test constructions) or an already-shared one (the engine assembly, which hands
    /// every lane the SAME activation-built set).
    pub fn new(
        codecs: impl Into<Arc<CodecRegistry>>,
        formats: impl Into<Arc<FormatRegistry>>,
        validators: impl Into<Arc<ValidatorRegistry>>,
    ) -> InboundChain {
        InboundChain {
            codecs: codecs.into(),
            formats: formats.into(),
            validators: validators.into(),
            redactors: RedactorRegistry::new(),
        }
    }

    /// Attach the redactor registry (built-ins + deployment-scoped user redactors). Builder-style so
    /// the many lightweight `InboundChain::new` construction sites need no change.
    pub fn with_redactors(mut self, redactors: RedactorRegistry) -> InboundChain {
        self.redactors = redactors;
        self
    }

    pub fn redactors(&self) -> &RedactorRegistry {
        &self.redactors
    }

    pub fn codecs(&self) -> &CodecRegistry {
        &self.codecs
    }

    /// Channel-scoped decode (the inverted hot path's first step). Three routes:
    ///
    /// - **no codec** → content-type-driven format decode (case b): the parser is chosen purely
    ///   by the inbound `Content-Type`, fail-open to opaque raw passthrough. Never rejects; the raw
    ///   wire bytes stay available at `event.body`.
    /// - **a bare format** (`json`/`xml`/`yaml`/`csv`/`raw-*`) → a SHAPE CONTRACT: the parser is
    ///   chosen by content-type within the contract's class (json/xml/yaml interchangeable).
    /// - **a schema-backed codec** (a business standard / user `urn:` schema codec) → the codec
    ///   takes full control: its own `accepted_content_types` gates, its own `decode` runs.
    ///
    /// The capability gate runs BEFORE decode (the auditable-drop case): only a genuine media-type
    /// mismatch rejects.
    pub fn decode(
        &self,
        binding: &ChannelBinding,
        body: &[u8],
        content_type: Option<&str>,
    ) -> Result<Option<DecodeResult>, Diagnostic> {
        // The `sutra.decode` waterfall span (telemetry facade).
        let _span = tracing::info_span!(
            telemetry::SPAN_DECODE,
            channel = %binding.channel_name,
            codec = %binding.codec,
        )
        .entered();

        // (b) No declared codec — negotiate a format from the content-type (fail-open to raw).
        if binding.codec.is_empty() {
            return Ok(Some(self.formats.open_decode(body, content_type)));
        }

        // A bare format reference — a shape contract; negotiate the parser by content-type.
        if let Some(contract) = self.formats.contract(&binding.codec) {
            let codec = self
                .formats
                .select(contract, content_type, &binding.codec)
                .ok_or_else(|| {
                    Diagnostic::error(
                        codes::INBOUND_CAPABILITY_MISMATCH,
                        format!(
                            "Inbound on channel '{}' arrived as content-type '{}' but the '{}' \
                             format contract accepts only {:?}; the flow cannot understand this \
                             encoding.",
                            binding.channel_name,
                            content_type.unwrap_or(""),
                            binding.codec,
                            self.formats.admissible_content_types(contract)
                        ),
                    )
                    .with_attribute("channel", &binding.channel_name)
                    .with_attribute("codec", &binding.codec)
                    .with_attribute("contentType", content_type.unwrap_or(""))
                })?;
            let result = codec.decode(body, content_type);
            // Flat-map cross-accept: a nested format (json/xml/yaml) admitted under a flat-map (csv)
            // contract must decode to a FLAT (row-shaped) tree; a NESTED body is rejected — the
            // discriminator is flatness, not syntax. csv is flat by construction, so it always passes.
            if contract == ShapeClass::FlatMap && result.outcome != DecodeOutcome::Fatal {
                if let Some(CodecValue::Json(tree)) = &result.payload {
                    if !crate::registry::is_flat_map_tree(tree) {
                        return Err(Diagnostic::error(
                            codes::INBOUND_CAPABILITY_MISMATCH,
                            format!(
                                "Inbound on channel '{}' bound the flat '{}' format but the '{}' \
                                 body is NESTED; a flat-map channel accepts only flat, row-shaped \
                                 data (name→scalar).",
                                binding.channel_name,
                                binding.codec,
                                content_type.unwrap_or("")
                            ),
                        )
                        .with_attribute("channel", &binding.channel_name)
                        .with_attribute("codec", &binding.codec)
                        .with_attribute("contentType", content_type.unwrap_or("")));
                    }
                }
            }
            return Ok(Some(result));
        }

        // A schema-backed codec — the codec is in full control. Resolved within the binding's
        // deployment, in the same tri-tier resolution order the codec registry documents:
        // archive codec → built-in `urn:sutra:codec:<name>:internal` → explicit URN.
        let codec = self
            .codecs
            .resolve(&binding.codec, &binding.deployment_id())
            .ok_or_else(|| {
                codec_not_found(&binding.channel_name, &binding.codec, &self.codecs.names())
            })?;
        if !content_type::accepts(&codec.accepted_content_types(), content_type) {
            return Err(Diagnostic::error(
                codes::INBOUND_CAPABILITY_MISMATCH,
                format!(
                    "Inbound on channel '{}' arrived as content-type '{}' but codec '{}' \
                     accepts only {:?}; the flow cannot understand this encoding.",
                    binding.channel_name,
                    content_type.unwrap_or(""),
                    binding.codec,
                    codec.accepted_content_types()
                ),
            )
            .with_attribute("channel", &binding.channel_name)
            .with_attribute("codec", &binding.codec)
            .with_attribute("contentType", content_type.unwrap_or("")));
        }
        Ok(Some(codec.decode(body, content_type)))
    }

    /// Reply encode — the outbound counterpart of [`Self::decode`] ("native reply" continuity). For
    /// a format contract (or no codec) the reply is encoded in the NEGOTIATED content-type (echo the
    /// inbound); for a schema-backed codec it is the same codec + inbound content-type.
    pub fn encode(
        &self,
        binding: &ChannelBinding,
        reply: &CodecValue,
        content_type: Option<&str>,
    ) -> Result<Vec<u8>, Diagnostic> {
        let codec = self.outbound_codec(binding, content_type)?;
        codec.encode(reply, content_type).map_err(|e| {
            Diagnostic::error(
                codes::OUTBOUND_ENCODE_FAILED,
                format!(
                    "Channel '{}' codec '{}' could not encode the reply object: {e}",
                    binding.channel_name, binding.codec
                ),
            )
        })
    }

    /// Validate bytes the flow produced against the contract of the channel they are leaving
    /// by — the outbound counterpart of [`Self::decode`] + the validator chain.
    ///
    /// The codec's own decode step is the validator: it is the only direction in the SPI that
    /// validates, and for a schema-bound codec it is what checks the message body against the
    /// schema. So the question asked here is literally "read back what we are about to send" —
    /// which needs no new SPI surface, no second contract, and no knowledge of any format or
    /// standard. It covers both ways a flow produces bytes: a codec-encoded object and a
    /// template render, which reaches the wire without touching a codec at all.
    ///
    /// Returns `Ok(())` when the egress is schema-less (a bare format or no codec: fail-open by
    /// construction — it never rejects, so there is no contract to fail) or when the read-back
    /// is clean. Otherwise the fail-closed [`codes::OUTBOUND_VALIDATION_REJECT`], carrying the
    /// issue count and the first few issues so the operator sees the offending field rather than
    /// a bare refusal.
    #[allow(clippy::too_many_arguments)]
    pub fn validate_outbound(
        &self,
        deployment: &DeploymentId,
        channel_label: &str,
        declared_codec: Option<&str>,
        channel_codec: &str,
        body: &[u8],
        content_type: Option<&str>,
        site: &str,
    ) -> Result<(), Diagnostic> {
        let declared = declared_codec.map(str::trim).filter(|c| !c.is_empty());
        let name = declared.unwrap_or_else(|| channel_codec.trim());
        // Schema-less egress: no contract to fail. Mirrors the inbound R5 carve-out exactly.
        if name.is_empty() || self.formats.contract(name).is_some() {
            return Ok(());
        }
        let codec = match declared {
            // An author who NAMED the governing codec gets a hard answer: a name that resolves
            // to nothing is a wiring fault, and silently skipping the check would turn the one
            // declaration that asked for validation into the one case that gets none. Lint
            // catches it at package time; this is the runtime backstop.
            Some(name) => self
                .codecs
                .resolve(name, deployment)
                .or_else(|| {
                    sutra_codec_spi::builtin_codecs()
                        .into_iter()
                        .find(|c| c.name() == name)
                })
                .ok_or_else(|| codec_not_found(channel_label, name, &self.codecs.names()))?,
            // Inferred from the channel: an unresolvable codec here is reported by the send
            // path itself, with the name and the known set, so a second differently-worded
            // refusal from this check would only misdirect.
            None => match self.codecs.resolve(name, deployment) {
                Some(codec) => codec,
                None => return Ok(()),
            },
        };
        let decoded = codec.decode(body, content_type);
        let errors: Vec<&ValidationIssue> = decoded
            .issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Error)
            .collect();
        if errors.is_empty() {
            return Ok(());
        }
        let shown = errors
            .iter()
            .take(5)
            .map(|i| format!("{} at {}: {}", i.code, i.path, i.message))
            .collect::<Vec<_>>()
            .join("; ");
        // Name the codec, and say WHERE it came from: a stated codec is the author's own
        // declaration, while an inferred one is the channel's, and an operator reading this needs
        // to know which of the two to go and change.
        let provenance = match declared {
            Some(_) => "declared on the output".to_string(),
            None => format!("bound to channel '{channel_label}'"),
        };
        Err(Diagnostic::error(
            codes::OUTBOUND_VALIDATION_REJECT,
            format!(
                "{site} produced a message that codec '{name}' ({provenance}) rejects: {} \
                 validation error(s) — {shown}. The message was NOT sent.",
                errors.len()
            ),
        )
        .with_attribute("channel", channel_label)
        .with_attribute("codec", name)
        .with_attribute(
            "codecSource",
            if declared.is_some() {
                "declared"
            } else {
                "channel"
            },
        )
        .with_attribute("issueCount", errors.len().to_string()))
    }

    /// The codec that carries bytes OUT over `binding` — the one resolution shared by the
    /// encode step and the outbound validation step, so a message is validated by exactly the
    /// codec that will put it on the wire.
    fn outbound_codec(
        &self,
        binding: &ChannelBinding,
        content_type: Option<&str>,
    ) -> Result<Arc<dyn PayloadCodec>, Diagnostic> {
        let codec: Arc<dyn PayloadCodec> = if binding.codec.is_empty() {
            // No codec: mirror the inbound format (fail-open to raw) — same choice as open_decode.
            self.formats.open_select(content_type).ok_or_else(|| {
                Diagnostic::error(
                    codes::OUTBOUND_ENCODE_FAILED,
                    format!(
                        "Channel '{}' has no codec and no format matches content-type '{}' for \
                         the reply.",
                        binding.channel_name,
                        content_type.unwrap_or("")
                    ),
                )
            })?
        } else if let Some(contract) = self.formats.contract(&binding.codec) {
            self.formats
                .select(contract, content_type, &binding.codec)
                .ok_or_else(|| {
                    Diagnostic::error(
                        codes::OUTBOUND_ENCODE_FAILED,
                        format!(
                            "Channel '{}' format '{}' cannot encode the reply for content-type '{}'.",
                            binding.channel_name,
                            binding.codec,
                            content_type.unwrap_or("")
                        ),
                    )
                })?
        } else {
            self.codecs
                .resolve(&binding.codec, &binding.deployment_id())
                .ok_or_else(|| {
                    codec_not_found(&binding.channel_name, &binding.codec, &self.codecs.names())
                })?
        };
        Ok(codec)
    }

    /// Process-scoped post-decode — pipeline steps 3-7: project the decoded payload under
    /// the intake node's `<q:source name>`, run the validator chain, build the frozen
    /// `validation.*` summary, and honour `<q:onValidation>`. Mutates `variables`.
    pub fn apply_decoded(
        &self,
        process: &ProcessDefinition,
        node_id: &str,
        binding: &ChannelBinding,
        decoded: &DecodeResult,
        variables: &mut Variables,
    ) -> Result<IntakeOutcome, Diagnostic> {
        let bindings = process.bindings_for(node_id);
        let Some(source) = bindings.source() else {
            // No <q:source> on the selected node — leave variables alone, continue.
            return Ok(IntakeOutcome::Proceed {
                start_node_id: None,
            });
        };
        // The `sutra.validate` waterfall span (tier-1 + tier-2 + summary).
        let _span = tracing::info_span!(
            telemetry::SPAN_VALIDATE,
            process.id = %process.id,
            node.id = %node_id,
        )
        .entered();

        // Project the FEEL-walkable payload view under `<source.name>` (default "payload").
        let typed_payload = decoded
            .payload
            .as_ref()
            .map(payload_view)
            .unwrap_or(FeelValue::Null);
        variables.insert(source.name.clone(), typed_payload.clone());

        // Project the codec's reported message attributes under the SAME root an intake
        // predicate reads them under, so `attr.direction` means one thing in a
        // `<q:source filterExpression>` and inside the process. Without this, a flow that needs
        // the value a start event was selected on would have to make the codec duplicate it into
        // the payload.
        variables.insert(
            SELECTOR_ATTRIBUTE_ROOT.to_string(),
            attribute_map(Some(decoded)),
        );

        // Redaction (DLP): run the `<q:redactors>` chain over the raw payload to LOCATE
        // sensitive spans, then store the masked projection under `<source.name>.redacted` —
        // the view every observability surface (audit / inspect / diagnostics) shows. The raw
        // `<source.name>` stays available for flow-to-flow logic (encrypted at rest).
        // Fail-closed: a crashed redactor over-masks the whole payload (see `mask_projection`).
        if !source.redactors.is_empty() {
            let mut outcomes = Vec::with_capacity(source.redactors.len());
            for reference in &source.redactors {
                let redactor = self.resolve_redactor(reference, binding, &process.id, node_id)?;
                outcomes.push(run_redactor(redactor.as_ref(), &typed_payload, variables));
            }
            variables.insert(
                format!("{}{}", source.name, sutra_bpmn::REDACTION_COMPANION_SUFFIX),
                mask_projection(&typed_payload, &outcomes),
            );
        }

        // Tier-1: codec-side issues are the first entries (STRUCTURAL by definition).
        let mut accumulated: Vec<ValidationIssue> = decoded.issues.clone();
        let mut has_structural = !decoded.issues.is_empty();

        // FATAL decode short-circuits the validator chain — nothing to validate; the
        // <q:onValidation> policy below still decides the response posture.
        let fatal_decode = decoded.outcome == DecodeOutcome::Fatal;

        if !fatal_decode {
            // Complex chain — each <q:complexValidator source=…> validates the whole
            // payload, resolved under the binding's version-bearing module key (VM-7b).
            for name in &source.complex_validators {
                let validator = self.resolve_validator(name, binding, &process.id, node_id)?;
                let issues = run_validator(validator.as_ref(), &typed_payload, variables);
                if !issues.is_empty() && validator.tier() == ValidatorTier::Structural {
                    has_structural = true;
                }
                accumulated.extend(issues);
            }
            // Simple chain — a field content validator at the FEEL path (unresolvable
            // path ⇒ null ⇒ the validator decides). Reuses the q:alias path resolver.
            for sv in &source.simple_validators {
                let validator =
                    self.resolve_validator(&sv.reference, binding, &process.id, node_id)?;
                let value = sutra_feel::expressions::eval(&sv.path, &variables.to_feel_context())
                    .unwrap_or(FeelValue::Null);
                let issues = run_validator(validator.as_ref(), &value, variables);
                if !issues.is_empty() && validator.tier() == ValidatorTier::Structural {
                    has_structural = true;
                }
                accumulated.extend(issues);
            }
        }

        variables.insert(
            format!("{}.validation", source.name),
            issues_as_feel(&accumulated),
        );

        // ---- the FROZEN validation.* summary -------------------------------------------
        let had_errors = accumulated
            .iter()
            .any(|i| i.severity == IssueSeverity::Error);
        variables.insert(
            "validation",
            build_validation_summary(&accumulated, had_errors, has_structural),
        );

        // ---- <q:onValidation> policy -----------------------------------------------------
        //
        // A clean payload always proceeds. A payload with errors is decided by the flow's declared
        // posture — and, when the flow declared none, by the fail-CLOSED default below.
        let on_validation = bindings.on_validation.as_ref();
        if !had_errors {
            return Ok(IntakeOutcome::Proceed {
                start_node_id: Some(node_id.to_string()),
            });
        }
        // R5 applies only where there is a CONTRACT to fail. A channel binding a bare format (or
        // no codec at all) is schema-less ingress: the format layer is fail-open by construction —
        // it never rejects, and the raw bytes stay available at `event.body` — so an issue there is
        // an observation, not a verdict, and turning it into a refusal would contradict the layer's
        // own documented posture. A schema-backed codec or a declared validator chain IS a
        // contract, and that is where the default below bites.
        let schemaless = binding.codec.trim().is_empty()
            || self.formats.contract(binding.codec.trim()).is_some();
        let has_contract = !schemaless
            || !source.complex_validators.is_empty()
            || !source.simple_validators.is_empty();
        if !has_contract && on_validation.is_none() {
            return Ok(IntakeOutcome::Proceed {
                start_node_id: Some(node_id.to_string()),
            });
        }
        let Some(policy) = on_validation else {
            // No <q:onValidation> means the flow said
            // NOTHING about handling a validation failure, so the failure must not enter it — the
            // codec answers the caller instead. This is `reject`, deliberately not `error`: an
            // `error` raises a BPMN error into a process that, by definition, declared no handler
            // for it, reporting "uncaught BPMN error" instead of naming the offending field.
            //
            // Authors who want the previous pass-through get it by declaring
            // <q:onValidation mode="route"/>, which also makes the intent visible in the diagram.
            return Ok(IntakeOutcome::Reject(
                Diagnostic::error(
                    codes::INBOUND_VALIDATION_REJECT,
                    format!(
                        "Inbound rejected on intake node {node_id} of process {}: {} validation                          issue(s) and no <q:onValidation> policy to handle them. Declare                          <q:onValidation mode=\"route\"/> to let the flow triage                          payload.validation.issues itself.",
                        process.id,
                        accumulated.len()
                    ),
                )
                .with_attribute("processId", &process.id)
                .with_attribute("nodeId", node_id)
                .with_attribute("issueCount", accumulated.len().to_string())
                .with_attribute("defaultPosture", "reject"),
            ));
        };
        match policy.mode {
            OnValidationMode::Route => Ok(IntakeOutcome::Proceed {
                start_node_id: Some(node_id.to_string()),
            }),
            OnValidationMode::Reject => Ok(IntakeOutcome::Reject(
                Diagnostic::error(
                    codes::INBOUND_VALIDATION_REJECT,
                    format!(
                        "Inbound rejected on intake node {node_id} of process {}: {} \
                         validation issue(s); mode=reject.",
                        process.id,
                        accumulated.len()
                    ),
                )
                .with_attribute("processId", &process.id)
                .with_attribute("nodeId", node_id)
                .with_attribute("issueCount", accumulated.len().to_string()),
            )),
            OnValidationMode::Error => {
                let error_code = policy.error_code.clone().unwrap_or_default();
                Ok(IntakeOutcome::Error {
                    diagnostic: Diagnostic::error(
                        codes::INBOUND_VALIDATION_ERROR,
                        format!(
                            "Inbound raised BPMN error '{error_code}' on Start Event \
                             {node_id} of process {}: payload failed validator chain ({} \
                             issue(s)).",
                            process.id,
                            accumulated.len()
                        ),
                    )
                    .with_attribute("processId", &process.id)
                    .with_attribute("nodeId", node_id)
                    .with_attribute("issueCount", accumulated.len().to_string())
                    .with_attribute("errorCode", &error_code),
                    error_code,
                })
            }
        }
    }

    /// Resolve a named validator: version-qualified module rule → global SPI validator →
    /// fail closed (`SUTRA.VALIDATE.VALIDATOR_NOT_FOUND`).
    fn resolve_validator(
        &self,
        name: &str,
        binding: &ChannelBinding,
        process_id: &str,
        node_id: &str,
    ) -> Result<Arc<dyn crate::validators::ContentValidator>, Diagnostic> {
        self.validators
            .resolve(name, &binding.deployment_id())
            .ok_or_else(|| {
                Diagnostic::error(
                    codes::VALIDATE_VALIDATOR_NOT_FOUND,
                    format!(
                        "Start Event {node_id} of process {process_id} requested validator \
                         '{name}' but no ContentValidator is registered for tenant '{}'. \
                         Known: {:?}",
                        binding.tenant(),
                        self.validators.names()
                    ),
                )
                .with_attribute("processId", process_id)
                .with_attribute("nodeId", node_id)
                .with_attribute("validator", name)
            })
    }

    /// Resolve a `<q:redactor ref=…>` within the binding's deployment (archive redactor →
    /// built-in `urn:sutra:redactor:<name>:internal` → explicit URN — the same tri-tier
    /// resolution order). Fail closed
    /// (`SUTRA.VALIDATE.REDACTOR_NOT_FOUND`) on an unknown ref: a flow that can't redact must
    /// not run rather than leak.
    fn resolve_redactor(
        &self,
        reference: &str,
        binding: &ChannelBinding,
        process_id: &str,
        node_id: &str,
    ) -> Result<Rc<dyn ContentRedactor>, Diagnostic> {
        self.redactors
            .resolve(reference, &binding.deployment_id())
            .ok_or_else(|| {
                Diagnostic::error(
                    codes::VALIDATE_REDACTOR_NOT_FOUND,
                    format!(
                        "Start Event {node_id} of process {process_id} requested redactor \
                         '{reference}' but no ContentRedactor is registered for tenant '{}'. \
                         Known: {:?}",
                        binding.tenant(),
                        self.redactors.names()
                    ),
                )
                .with_attribute("processId", process_id)
                .with_attribute("nodeId", node_id)
                .with_attribute("redactor", reference)
            })
    }
}

/// The FEEL-walkable view of a decoded payload — ONE projection, used by aliases, gateway
/// conditions and validators alike. Map-shaped codec output walks directly;
/// scalars pass through; raw bytes surface as a (lossy) UTF-8 string — `FeelValue` has no
/// bytes variant, and validators needing wire bytes read `event.body`.
pub fn payload_view(value: &CodecValue) -> FeelValue {
    match value {
        CodecValue::Text(s) => FeelValue::String(s.clone()),
        CodecValue::Bytes(b) => FeelValue::String(String::from_utf8_lossy(b).into_owned()),
        CodecValue::Json(v) => json_to_feel(v),
    }
}

/// JSON tree → FEEL value (arbitrary-precision numbers survive as `BigDecimal`).
pub fn json_to_feel(v: &serde_json::Value) -> FeelValue {
    sutra_executor::variables::json_to_feel(v)
}

/// The issue list as a FEEL-walkable value (`payload.validation` / `validation.issues`) —
/// each issue a map of `code` / `severity` / `path` / `message` / `value`.
fn issues_as_feel(issues: &[ValidationIssue]) -> FeelValue {
    FeelValue::List(issues.iter().map(issue_as_feel).collect())
}

fn issue_as_feel(issue: &ValidationIssue) -> FeelValue {
    let mut m = BTreeMap::new();
    m.insert("code".to_string(), FeelValue::String(issue.code.clone()));
    m.insert(
        "severity".to_string(),
        FeelValue::String(issue.severity.as_str().to_string()),
    );
    m.insert("path".to_string(), FeelValue::String(issue.path.clone()));
    m.insert(
        "message".to_string(),
        FeelValue::String(issue.message.clone()),
    );
    m.insert(
        "value".to_string(),
        issue
            .value
            .clone()
            .map(FeelValue::String)
            .unwrap_or(FeelValue::Null),
    );
    FeelValue::Map(m)
}

/// The frozen `validation` summary map (the names are part of the contract):
/// `outcome` (`OK|SOFT_ERRORS|FATAL`), `tier` (`n/a|structural|content`),
/// `firstReasonCode` (first ERROR issue's `value` slot, else `""`), `firstIssue`,
/// `issues` (the raw list).
fn build_validation_summary(
    issues: &[ValidationIssue],
    had_errors: bool,
    has_structural: bool,
) -> FeelValue {
    let mut out = BTreeMap::new();
    if issues.is_empty() {
        out.insert("outcome".to_string(), FeelValue::String("OK".to_string()));
        out.insert("tier".to_string(), FeelValue::String("n/a".to_string()));
        out.insert(
            "firstReasonCode".to_string(),
            FeelValue::String(String::new()),
        );
        out.insert("firstIssue".to_string(), FeelValue::String(String::new()));
        out.insert("issues".to_string(), FeelValue::List(Vec::new()));
        return FeelValue::Map(out);
    }
    // The canonical "first" issue — the first ERROR-severity one (soft issues must not
    // shadow a hard one), else the first issue.
    let first = issues
        .iter()
        .find(|i| i.severity == IssueSeverity::Error)
        .unwrap_or(&issues[0]);
    out.insert(
        "outcome".to_string(),
        FeelValue::String(if had_errors { "FATAL" } else { "SOFT_ERRORS" }.to_string()),
    );
    out.insert(
        "tier".to_string(),
        FeelValue::String(
            if has_structural {
                "structural"
            } else {
                "content"
            }
            .to_string(),
        ),
    );
    out.insert(
        "firstReasonCode".to_string(),
        FeelValue::String(first.value.clone().unwrap_or_default()),
    );
    out.insert(
        "firstIssue".to_string(),
        FeelValue::String(first.message.clone()),
    );
    out.insert("issues".to_string(), issues_as_feel(issues));
    FeelValue::Map(out)
}

// These three live in this ALWAYS-COMPILED module rather than beside the dispatcher: the
// dispatcher is behind the `transport` feature, while the selector table in `registry` and the
// projection below are not — and `sutra-loader` (hence the wasm lint core) depends on this
// crate with default features off.

/// The root a message's codec-reported attributes are read under, in an intake predicate and
/// inside the process alike — so `attr.direction` means the same thing in both places. Fixed
/// deliberately: a per-codec root could not be analysed at package time.
pub(crate) const SELECTOR_ATTRIBUTE_ROOT: &str = "attr";

/// The attributes a codec reported for this message, as a FEEL map.
///
/// One source for both the intake predicate and the in-process projection, so the value a filter
/// matched on is exactly the value the process reads.
pub(crate) fn attribute_map(
    decoded: Option<&sutra_codec_spi::DecodeResult>,
) -> sutra_feel::FeelValue {
    sutra_feel::FeelValue::Map(decoded.map(|d| d.attributes.clone()).unwrap_or_default())
}

/// The context a `<q:source filterExpression>` is evaluated against: `attr.*` plus `messageType`.
///
/// Deliberately NOT headers and NOT the payload. Headers are an open map and a payload an
/// arbitrary tree, so admitting either would make the deploy-time proof that two subscribers
/// cannot both match impossible, and would let one business message route differently depending
/// on the transport it arrived over. Both stay readable inside the process once a start event has
/// been chosen.
// Only the dispatch path builds a selector context, and `dispatch` is transport-gated — the
// WASM lint core compiles this crate without it.
#[cfg(feature = "transport")]
pub(crate) fn selector_context(
    decoded: Option<&sutra_codec_spi::DecodeResult>,
    message_type: Option<&str>,
) -> sutra_feel::FeelContext {
    let mut ctx = sutra_feel::FeelContext::new();
    ctx.insert(SELECTOR_ATTRIBUTE_ROOT.to_string(), attribute_map(decoded));
    ctx.insert(
        "messageType".to_string(),
        message_type
            .map(|mt| sutra_feel::FeelValue::String(mt.to_string()))
            .unwrap_or(sutra_feel::FeelValue::Null),
    );
    ctx
}
