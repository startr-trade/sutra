//! OMG DMN-TCK conformance harness. Walks a DMN-TCK checkout, runs each `*-test-NN.xml`
//! case's inputs through the level-3 DRG evaluator ([`sutra_dmn::drg`]), compares the decision
//! results against the expected `resultNode`s, and reports a per-compliance-level pass rate.
//!
//! Gated + external: the corpus is NOT vendored (it is OMG ASL-2.0 licensed; this crate ships
//! under MIT OR Apache-2.0). Clone it once — the default location is
//! `<target-dir>/dmn-tck` (git-ignored, never packaged) — and run explicitly:
//!
//! ```text
//! git clone --depth 1 https://github.com/dmn-tck/tck.git rust/target/dmn-tck
//! cargo test -p sutra-dmn --test tck -- --ignored --nocapture
//! ```
//!
//! `SUTRA_DMN_TCK_DIR=/path/to/tck` overrides that location for an existing checkout. With no
//! corpus present the test SKIPS (printing the clone command) rather than failing — absence of
//! an unvendorable corpus is not a conformance regression.
//!
//! Classification per `resultNode`: PASS (result equals expected), FAIL (engine produced a
//! different value — a real conformance gap), UNSUPPORTED (the engine could not evaluate the
//! construct — recorded, not failed: the allowlist is *automatic* here, every `Err` from the
//! evaluator or unparseable expected value counts as unsupported rather than a failure).
//!
//! Side outputs, both env-gated: `SUTRA_DMN_TCK_DUMP=<file>` writes every non-pass outcome
//! (assertion-level, for gap analysis); `SUTRA_DMN_TCK_RESULTS_DIR=<dir>` writes the official
//! vendor-submission pair `tck_results.csv` + `tck_results.properties` (TCK `TestResults/`
//! format: one row per test CASE, statuses SUCCESS/IGNORED/ERROR — see `write_tck_results` for
//! the assertion→case roll-up rule). `SUTRA_DMN_TCK_PRODUCT_VERSION` / `SUTRA_DMN_TCK_LAST_UPDATE`
//! override the two non-constant properties (default: crate version / today UTC).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use bigdecimal::BigDecimal;
use sutra_feel::{FeelContext, FeelValue};

// ---- a minimal XML DOM (quick-xml; no namespace resolution — attributes kept by raw key) ------

#[derive(Default)]
struct El {
    name: String,
    attrs: Vec<(String, String)>,
    children: Vec<El>,
    text: String,
}

impl El {
    fn attr(&self, local: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(k, _)| k == local || k.rsplit(':').next() == Some(local))
            .map(|(_, v)| v.as_str())
    }
    fn child(&self, name: &str) -> Option<&El> {
        self.children.iter().find(|c| c.name == name)
    }
    fn children_named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a El> {
        self.children.iter().filter(move |c| c.name == name)
    }
}

fn parse_xml(bytes: &[u8]) -> Result<El, String> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().expand_empty_elements = true;
    let mut stack: Vec<El> = Vec::new();
    let mut root: Option<El> = None;
    loop {
        match reader.read_event().map_err(|e| e.to_string())? {
            Event::Start(e) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                let mut attrs = Vec::new();
                for a in e.attributes().flatten() {
                    let key = String::from_utf8_lossy(a.key.as_ref()).into_owned();
                    let val = String::from_utf8_lossy(&a.value).into_owned();
                    attrs.push((key, val));
                }
                stack.push(El {
                    name,
                    attrs,
                    ..El::default()
                });
            }
            Event::End(_) => {
                let el = stack.pop().ok_or("unbalanced end tag")?;
                match stack.last_mut() {
                    Some(p) => p.children.push(el),
                    None => root = Some(el),
                }
            }
            Event::Text(t) => {
                if let Some(top) = stack.last_mut() {
                    let decoded = t.decode().map_err(|e| e.to_string())?;
                    top.text.push_str(&decoded);
                }
            }
            Event::CData(c) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&String::from_utf8_lossy(c.as_ref()));
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    root.ok_or_else(|| "no document element".to_string())
}

// ---- TCK value codec: <value xsi:type>/<list>/<component>/nil -> FeelValue --------------------

/// Decode a TCK value element (`<value>`, `<item>`, `<component>`, or an `<expected>` wrapper's
/// child). `None` = a value shape the harness can't represent (→ unsupported), NOT a mismatch.
fn decode_value(el: &El) -> Option<FeelValue> {
    if el.attr("nil") == Some("true") {
        return Some(FeelValue::Null);
    }
    // A <component>/<item> wraps its scalar/nil payload in a nested <value> element (xsi:type or
    // xsi:nil lives on that nested <value>, never inlined on <component>/<item> itself — verified
    // corpus-wide). A *structured* payload (component-in-component, list/component-in-component,
    // item-in-list) nests directly instead, with no <value> wrapper, and falls through to the
    // checks below unaffected: the corpus never nests a <value> alongside a <component>/<list>/
    // <item> sibling on the same element, nor a <value> inside another <value>.
    if let Some(v) = el.child("value") {
        return decode_value(v);
    }
    // Structured value: <component name="x">…</component> children -> a context/map.
    let components: Vec<&El> = el.children_named("component").collect();
    if !components.is_empty() {
        let mut map = BTreeMap::new();
        for c in components {
            let name = c.attr("name")?.to_string();
            map.insert(name, decode_value(c)?);
        }
        return Some(FeelValue::Map(map));
    }
    // List value: <list><item>…</item>…</list> (or direct <item> children).
    if let Some(list) = el.child("list") {
        let mut items = Vec::new();
        for it in list.children_named("item") {
            items.push(decode_value(it)?);
        }
        return Some(FeelValue::List(items));
    }
    let items: Vec<&El> = el.children_named("item").collect();
    if !items.is_empty() {
        let mut out = Vec::new();
        for it in items {
            out.push(decode_value(it)?);
        }
        return Some(FeelValue::List(out));
    }
    // Scalar: keyed by xsi:type (local "type").
    let ty = el.attr("type").unwrap_or("");
    let ty = ty.rsplit(':').next().unwrap_or(ty); // strip xsd: prefix
                                                  // `.trim()` is correct for numeric/boolean/temporal text (incidental XML
                                                  // indentation/newlines around a `<value>` are insignificant there), but wrong for
                                                  // `"string"` specifically, where leading/trailing whitespace is part of the data
                                                  // (DMN-TCK 1103-feel-substring-function#008 / 1105-feel-upper-case-function#007 /
                                                  // 1106-feel-lower-case-function#008 — each expects a real trailing space preserved).
    let text = el.text.trim();
    match ty {
        "decimal" | "double" | "float" | "integer" | "int" | "long" | "short" | "byte"
        | "nonNegativeInteger" | "positiveInteger" => {
            Some(FeelValue::Number(BigDecimal::from_str(text).ok()?))
        }
        "boolean" => Some(FeelValue::Boolean(text == "true" || text == "1")),
        "string" => Some(FeelValue::String(trim_xml_padding(&el.text))),
        // Temporal values decode through the same ISO parser the engine uses for `@"…"` literals,
        // so an expected date/time/duration compares structurally against the engine's result.
        "date" | "time" | "duration" | "dateTime" => sutra_feel::temporal::parse_at_literal(text),
        // Any other (untyped) value: not represented by the harness -> unsupported (allowlisted).
        _ => None,
    }
}

/// Strip only XML pretty-printing padding from a string-typed `<value>`'s raw text: a
/// leading/trailing whitespace run is discarded ONLY when it itself contains a newline (i.e. it's
/// indentation around the element, not same-line data); a same-line trailing/leading space with
/// no newline in its run is significant string content and is preserved verbatim.
fn trim_xml_padding(text: &str) -> String {
    let edge_len = |chars: &[char]| -> usize {
        let run_end = chars.iter().take_while(|c| c.is_whitespace()).count();
        if chars[..run_end].contains(&'\n') {
            run_end
        } else {
            0
        }
    };
    let chars: Vec<char> = text.chars().collect();
    let start = edge_len(&chars);
    let mut rev: Vec<char> = chars.clone();
    rev.reverse();
    let end_trim = edge_len(&rev);
    let end = chars.len().saturating_sub(end_trim);
    if start >= end {
        String::new()
    } else {
        chars[start..end].iter().collect()
    }
}

// ---- TCK test-case model ----------------------------------------------------------------------

struct TckCase {
    id: String,
    inputs: Vec<(String, Option<FeelValue>)>,
    /// decision name -> (expected value [None = undecodable], errorResult). A resultNode marked
    /// `errorResult="true"` asserts the decision must FAIL/produce null on the given inputs; a
    /// conformant engine erroring (or yielding null) is a PASS for that node.
    results: Vec<(String, Option<FeelValue>, bool)>,
    /// The testCase's own `invocableName` attribute — set only for a DIRECT decision-service
    /// invocation (`type="decisionService"`, DMN-TCK 0085/0082's `decisionService_NNN` cases):
    /// `inputs` then supplies the service's declared parameters BY NAME (not the model's raw
    /// inputData), dispatched via `Drg::evaluate_decision_service` rather than `Drg::evaluate`.
    invocable_name: Option<String>,
}

fn parse_test_cases(doc: &El) -> (String, Vec<TckCase>) {
    let model = doc
        .child("modelName")
        .map(|m| m.text.trim().to_string())
        .unwrap_or_default();
    let mut cases = Vec::new();
    for tc in doc.children_named("testCase") {
        let id = tc.attr("id").unwrap_or("").to_string();
        let inputs = tc
            .children_named("inputNode")
            .filter_map(|n| n.attr("name").map(|name| (name.to_string(), value_of(n))))
            .collect();
        let results = tc
            .children_named("resultNode")
            .map(|n| {
                let name = n.attr("name").unwrap_or("").to_string();
                // The expected value lives under <expected>; fall back to a direct <value>.
                let expected = n
                    .child("expected")
                    .and_then(value_of)
                    .or_else(|| value_of(n));
                let error_result = n.attr("errorResult") == Some("true");
                (name, expected, error_result)
            })
            .collect();
        let invocable_name = tc
            .attr("invocableName")
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string);
        cases.push(TckCase {
            id,
            inputs,
            results,
            invocable_name,
        });
    }
    (model, cases)
}

/// The decoded value carried by an inputNode / expected wrapper — its `<value>`/`<list>`/
/// `<component>` child (or itself when the value markup is inline).
fn value_of(node: &El) -> Option<FeelValue> {
    if let Some(v) = node.child("value") {
        return decode_value(v);
    }
    if node.child("list").is_some() || node.child("component").is_some() {
        return decode_value(node);
    }
    None
}

// ---- FEEL value equality (DMN numeric equality: value, not scale) -----------------------------

fn feel_eq(a: &FeelValue, b: &FeelValue) -> bool {
    match (a, b) {
        // Numeric tolerance matches the official TCK reference runner's value comparison:
        // numbers compare equal when
        // |expected − actual| < 0.00000001 (absolute tolerance), not exact BigDecimal equality.
        (FeelValue::Number(x), FeelValue::Number(y)) => {
            (x - y).abs() < BigDecimal::from_str("0.00000001").unwrap()
        }
        (FeelValue::List(xs), FeelValue::List(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(x, y)| feel_eq(x, y))
        }
        (FeelValue::Map(xs), FeelValue::Map(ys)) => {
            xs.len() == ys.len()
                && xs
                    .iter()
                    .all(|(k, x)| ys.get(k).map(|y| feel_eq(x, y)).unwrap_or(false))
        }
        _ => a == b,
    }
}

// ---- runner -----------------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Tally {
    pass: usize,
    fail: usize,
    unsupported: usize,
    /// Unsupported broken down by reason (model_unloadable / expected_undecodable /
    /// input_undecodable / evaluator_error / decision_missing) — drives gap prioritization.
    reasons: BTreeMap<&'static str, usize>,
    fail_examples: Vec<String>,
    err_examples: Vec<String>,
    /// Evaluator errors bucketed by a normalized signature (quoted tokens collapsed) so the
    /// dominant FEEL/DMN gaps are countable, not just sampled.
    err_hist: BTreeMap<String, usize>,
    /// Every non-pass outcome, uncapped (`FAIL …` / `ERR …` / `UNSUP(reason) …`) — written to
    /// `SUTRA_DMN_TCK_DUMP` when set, so gap analysis works from the full list, not samples.
    dump: Vec<String>,
    /// One row per test CASE (the TCK submission granularity — a case's resultNode assertions
    /// roll up per `write_tck_results`), in corpus walk order.
    rows: Vec<CaseRow>,
}

/// One `tck_results.csv` row: the five columns of the official `TestResults/` format.
#[derive(Clone)]
struct CaseRow {
    /// `compliance-level-N/<model-dir>` — the test's directory relative to `TestCases/`.
    suite: String,
    /// The test-definition file stem, e.g. `0001-input-data-string-test-01`.
    file_stem: String,
    /// The `<testCase id>` attribute.
    case_id: String,
    /// `SUCCESS` | `IGNORED` | `ERROR` (the full vocabulary observed across vendor files).
    status: &'static str,
    /// Empty for SUCCESS; the first offending assertion's dump line otherwise.
    comment: String,
}

impl Tally {
    fn unsupported(&mut self, reason: &'static str) {
        self.unsupported += 1;
        *self.reasons.entry(reason).or_default() += 1;
    }
}

/// Collapse an error string to a stable bucket key: single-quoted literals (the specific
/// offending token/name) become `'…'`, so `got 'days'` / `got 'III'` count as one gap, while
/// distinct builtin names (`max()` vs `abs()`) stay separate.
fn err_signature(e: &str) -> String {
    // Split on single-quote and rebuild: odd segments are the quoted literals. Replace each
    // with '…' UNLESS it is a single non-alphanumeric char (the offending lexer character —
    // '[', '{', ':' — which is the actionable detail worth keeping distinct).
    let mut s = String::new();
    for (i, seg) in e.split('\'').enumerate() {
        if i % 2 == 0 {
            s.push_str(seg);
        } else {
            let keep = seg.chars().count() == 1
                && seg
                    .chars()
                    .next()
                    .map(|c| !c.is_alphanumeric())
                    .unwrap_or(false);
            if keep {
                s.push('\'');
                s.push_str(seg);
                s.push('\'');
            } else {
                s.push_str("'…'");
            }
        }
    }
    s
}

fn run_level(root: &Path, level: &str, tally: &mut Tally) {
    let dir = root.join("TestCases").join(level);
    let mut test_files = Vec::new();
    collect_test_xml(&dir, &mut test_files);
    test_files.sort();
    for test_xml in test_files {
        let Ok(bytes) = std::fs::read(&test_xml) else {
            continue;
        };
        let Ok(doc) = parse_xml(&bytes) else {
            continue;
        };
        let (model_name, cases) = parse_test_cases(&doc);
        if model_name.is_empty() {
            continue;
        }
        let model_path = test_xml.parent().unwrap().join(&model_name);
        let Ok(model_bytes) = std::fs::read(&model_path) else {
            continue;
        };
        let model_dir = model_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        // CSV row identity: directory relative to TestCases/ ('/'-joined even off-unix) + the
        // test-definition file stem.
        let suite = test_xml
            .parent()
            .and_then(|p| p.strip_prefix(root.join("TestCases")).ok())
            .map(|p| {
                p.components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .unwrap_or_default();
        let file_stem = test_xml
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let resolve_import = import_resolver(model_dir);
        let drg = sutra_dmn::load_drg_with_imports(&model_bytes, &resolve_import);
        // Cache the full DRG evaluation by (invocable name, input) signature: a model with N
        // independent decisions and N one-per-decision cases (0100-arithmetic: 1087×1088)
        // otherwise re-evaluates every decision for every case (~1.2M evals). Cases that share
        // both share one evaluation.
        let mut eval_cache: HashMap<String, BTreeMap<String, Result<FeelValue, String>>> =
            HashMap::new();
        for case in &cases {
            let mut ctx = FeelContext::new();
            let mut input_undecodable = false;
            for (name, value) in &case.inputs {
                match value {
                    Some(v) => {
                        ctx.insert(name.clone(), v.clone());
                    }
                    None => input_undecodable = true,
                }
            }
            let cache_key = format!("{:?}|{ctx:?}", case.invocable_name);
            let results = eval_cache.entry(cache_key).or_insert_with(|| match &drg {
                Ok(drg) => match &case.invocable_name {
                    Some(name) => drg.evaluate_decision_service(name, &ctx),
                    None => drg.evaluate(&ctx),
                },
                Err(_) => BTreeMap::new(), // model didn't load (unsupported construct)
            });
            // Snapshot the assertion tallies so the case's outcome rolls up from the deltas —
            // the assertion arms below stay untouched. Any FAIL assertion ⇒ ERROR; else any
            // unsupported ⇒ IGNORED; else SUCCESS. The comment is the first offending
            // assertion's dump line (dump is pushed by every non-pass arm, so it is always
            // non-empty for a non-SUCCESS case).
            let (fail0, unsup0, dump0) = (tally.fail, tally.unsupported, tally.dump.len());
            for (decision, expected, error_result) in &case.results {
                let label = format!("{}#{} [{}]", model_name, case.id, decision);
                if drg.is_err() {
                    tally.unsupported("model_unloadable");
                    tally.dump.push(format!("UNSUP(model_unloadable) {label}"));
                    continue;
                }
                if input_undecodable {
                    tally.unsupported("input_undecodable");
                    tally.dump.push(format!("UNSUP(input_undecodable) {label}"));
                    continue;
                }
                // An `errorResult` node asserts the decision must reject the inputs: the engine
                // erroring — or returning null — is the conformant outcome; a concrete value is a
                // real failure (the engine accepted what it should have rejected).
                if *error_result {
                    match results.get(decision) {
                        // A semantic rejection (type mismatch, div-by-zero, …) or an explicit null
                        // is the conformant outcome. A SYNTAX error means the construct is
                        // unsupported — the engine didn't reject it, it couldn't parse it — so that
                        // stays on the unsupported allowlist, not counted as a pass.
                        Some(Ok(FeelValue::Null)) => tally.pass += 1,
                        Some(Err(e)) if !e.contains("SYNTAX") => tally.pass += 1,
                        Some(Err(e)) => {
                            tally.unsupported("evaluator_error");
                            *tally.err_hist.entry(err_signature(e)).or_default() += 1;
                            tally.dump.push(format!("ERR {label}: {e} [errorResult]"));
                        }
                        Some(Ok(actual)) => {
                            tally.fail += 1;
                            tally
                                .dump
                                .push(format!("FAIL {label}: expected error/null, got {actual:?}"));
                            if tally.fail_examples.len() < 25 {
                                tally
                                    .fail_examples
                                    .push(format!("{label}: expected error/null, got {actual:?}"));
                            }
                        }
                        None => {
                            tally.unsupported("decision_missing");
                            tally.dump.push(format!("UNSUP(decision_missing) {label}"));
                        }
                    }
                    continue;
                }
                let Some(expected) = expected else {
                    tally.unsupported("expected_undecodable");
                    tally
                        .dump
                        .push(format!("UNSUP(expected_undecodable) {label}"));
                    continue;
                };
                match results.get(decision) {
                    Some(Ok(actual)) => {
                        if feel_eq(actual, expected) {
                            tally.pass += 1;
                        } else {
                            tally.fail += 1;
                            tally.dump.push(format!(
                                "FAIL {label}: expected {expected:?}, got {actual:?}"
                            ));
                            if tally.fail_examples.len() < 25 {
                                tally.fail_examples.push(format!(
                                    "{label}: expected {expected:?}, got {actual:?}"
                                ));
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tally.unsupported("evaluator_error");
                        *tally.err_hist.entry(err_signature(e)).or_default() += 1;
                        tally.dump.push(format!("ERR {label}: {e}"));
                        if tally.err_examples.len() < 20 {
                            tally.err_examples.push(format!("{label}: {e}"));
                        }
                    }
                    None => {
                        tally.unsupported("decision_missing");
                        tally.dump.push(format!("UNSUP(decision_missing) {label}"));
                    }
                }
            }
            if case.results.is_empty() {
                continue; // no resultNodes — nothing asserted, no submission row
            }
            let (status, comment) = if tally.fail > fail0 {
                ("ERROR", first_dump_line(&tally.dump, dump0, "FAIL "))
            } else if tally.unsupported > unsup0 {
                ("IGNORED", first_dump_line(&tally.dump, dump0, ""))
            } else {
                ("SUCCESS", String::new())
            };
            tally.rows.push(CaseRow {
                suite: suite.clone(),
                file_stem: file_stem.clone(),
                case_id: case.id.clone(),
                status,
                comment,
            });
        }
    }
}

/// First dump line pushed since `from` that starts with `prefix` (the case's own offending
/// assertion — dump grows only within the current case between the snapshot and here), single-
/// lined and capped so the CSV stays one record per row.
fn first_dump_line(dump: &[String], from: usize, prefix: &str) -> String {
    let line = dump[from..]
        .iter()
        .find(|l| l.starts_with(prefix))
        .cloned()
        .unwrap_or_default();
    let mut flat: String = line.replace('\n', " ").chars().take(400).collect();
    if line.chars().count() > 400 {
        flat.push('…');
    }
    flat
}

/// Build a `sutra_dmn::load_drg_with_imports` resolver that scans `dir` (the model's own
/// directory — DMN-TCK colocates every `<import>`ed sibling file with the importing model) for a
/// `.dmn` file whose own `<definitions namespace="…">` matches. The scan only runs when the
/// resolver is actually CALLED, which `load_drg_with_imports` only does for a model that has
/// `<import>` elements at all — the overwhelming majority of the corpus never triggers it.
fn import_resolver(dir: PathBuf) -> impl Fn(&str) -> Option<Vec<u8>> {
    move |namespace: &str| -> Option<Vec<u8>> {
        let entries = std::fs::read_dir(&dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("dmn") {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(doc) = parse_xml(&bytes) else {
                continue;
            };
            if doc.attr("namespace") == Some(namespace) {
                return Some(bytes);
            }
        }
        None
    }
}

fn collect_test_xml(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_test_xml(&path, out);
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.contains("-test-") && n.ends_with(".xml"))
            .unwrap_or(false)
        {
            out.push(path);
        }
    }
}

/// Write `tck_results.csv` + `tck_results.properties` in the dmn-tck `TestResults/` submission
/// format. CSV: one row per test case, five double-quoted columns (suite path, test-file stem,
/// case id, status, comment); internal quotes doubled per RFC 4180. The per-case status is the
/// roll-up computed in `run_level` (FAIL ⇒ ERROR, else unsupported ⇒ IGNORED, else SUCCESS —
/// matching how the reference TCK runners report partial-case outcomes). Self-checks (hard asserts):
/// row totals match the per-level case counts and every non-SUCCESS row carries a comment.
fn write_tck_results(dir: &Path, levels: &[&Tally]) {
    std::fs::create_dir_all(dir).expect("create SUTRA_DMN_TCK_RESULTS_DIR");
    let mut csv = String::new();
    let mut totals: BTreeMap<&'static str, usize> = BTreeMap::new();
    for t in levels {
        for r in &t.rows {
            *totals.entry(r.status).or_default() += 1;
            assert!(
                r.status == "SUCCESS" || !r.comment.is_empty(),
                "non-SUCCESS row without a comment: {}/{}#{}",
                r.suite,
                r.file_stem,
                r.case_id
            );
            let esc = |s: &str| s.replace('"', "\"\"");
            csv.push_str(&format!(
                "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n",
                esc(&r.suite),
                esc(&r.file_stem),
                esc(&r.case_id),
                r.status,
                esc(&r.comment)
            ));
        }
    }
    let rows: usize = levels.iter().map(|t| t.rows.len()).sum();
    assert_eq!(rows, totals.values().sum::<usize>());
    std::fs::write(dir.join("tck_results.csv"), csv).expect("write tck_results.csv");

    // `.properties` value-side escaping: backslash first, then the two delimiters.
    let esc = |s: &str| {
        s.replace('\\', "\\\\")
            .replace(':', "\\:")
            .replace('=', "\\=")
    };
    let version = std::env::var("SUTRA_DMN_TCK_PRODUCT_VERSION")
        .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());
    let today = std::env::var("SUTRA_DMN_TCK_LAST_UPDATE").unwrap_or_else(|_| {
        let d = time::OffsetDateTime::now_utc().date();
        format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
    });
    let props = [
        ("instructions.url", "https://sutra.startr.trade/"),
        ("last.update", today.as_str()),
        (
            "product.comment",
            "Sutra is a Rust-native BPMN + DMN engine; full compliance level 3 execution.",
        ),
        ("product.name", "Sutra"),
        ("product.url", "https://sutra.startr.trade/"),
        ("product.version", version.as_str()),
        ("vendor.name", "Startr Trading Technologies Private Limited"),
        ("vendor.url", "https://startr.trade/"),
    ];
    let mut out = format!("#generated by the sutra-dmn TCK harness on {today}\n");
    for (k, v) in props {
        out.push_str(&format!("{k}={}\n", esc(v)));
    }
    std::fs::write(dir.join("tck_results.properties"), out).expect("write tck_results.properties");

    let breakdown: Vec<String> = totals.iter().map(|(k, n)| format!("{k}={n}")).collect();
    println!(
        "  TCK submission files written to {} ({rows} case rows: {})",
        dir.display(),
        breakdown.join(", ")
    );
}

fn report(level: &str, t: &Tally) {
    let attempted = t.pass + t.fail;
    let total = attempted + t.unsupported;
    let rate = if attempted == 0 {
        0.0
    } else {
        100.0 * t.pass as f64 / attempted as f64
    };
    println!(
        "  {level}: {total} result assertions | PASS {} | FAIL {} | UNSUPPORTED {} | \
         pass-rate over attempted = {rate:.1}%",
        t.pass, t.fail, t.unsupported
    );
    if !t.reasons.is_empty() {
        let mut reasons: Vec<_> = t.reasons.iter().collect();
        reasons.sort_by(|a, b| b.1.cmp(a.1));
        let breakdown: Vec<String> = reasons.iter().map(|(k, n)| format!("{k}={n}")).collect();
        println!("      unsupported by reason: {}", breakdown.join(", "));
    }
}

/// Canonical upstream corpus. Not vendored (OMG ASL-2.0) — cloned on demand by whoever runs the
/// sweep, so this URL is the one default that matters: it is what the skip message hands you.
const TCK_CLONE_URL: &str = "https://github.com/dmn-tck/tck.git";

/// Where the harness looks when `SUTRA_DMN_TCK_DIR` is unset: `<target-dir>/dmn-tck`.
///
/// Deliberately NOT a path inside the crate or the source tree. `cargo test` runs an integration
/// test with the PACKAGE ROOT as its working directory, so a relative `./dmn-tck` would clone a
/// ~100 MB ASL-2.0 corpus into `crates/sutra-dmn/` — inside a crate that publishes to crates.io
/// under MIT OR Apache-2.0. `target/` is already git-ignored and is never packaged, and unlike a
/// temp dir it persists, so the corpus is cloned once rather than per run.
///
/// Derived from the test binary's own location (`<target>/<profile>/deps/<bin>`), so it follows
/// `CARGO_TARGET_DIR` and custom profiles without being told about them.
fn default_tck_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| {
            exe.parent() // deps/
                .and_then(Path::parent) // <profile>/
                .and_then(Path::parent) // <target>/
                .map(Path::to_path_buf)
        })
        .unwrap_or_else(|| PathBuf::from("target"))
        .join("dmn-tck")
}

#[ignore = "dmn-tck: clone https://github.com/dmn-tck/tck.git (or set SUTRA_DMN_TCK_DIR)"]
#[test]
fn dmn_tck_conformance() {
    // The corpus is deliberately NOT vendored (OMG ASL-2.0). Absence is therefore a SKIP, not a
    // failure — otherwise every environment without a local checkout (CI, a fresh contributor
    // clone) reports a red conformance test it was never able to run. An explicitly-set
    // `SUTRA_DMN_TCK_DIR` that is wrong IS a hard error: that is a misconfiguration, not an
    // absent corpus.
    let explicit = std::env::var("SUTRA_DMN_TCK_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let root = explicit
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(default_tck_dir);
    if explicit.is_none() && !root.join("TestCases").is_dir() {
        println!(
            "SKIP dmn_tck_conformance: no DMN-TCK corpus at {}.\n  \
             Get it:  git clone --depth 1 {TCK_CLONE_URL} {}\n  \
             Or point at an existing checkout:  SUTRA_DMN_TCK_DIR=/path/to/tck cargo test \
             -p sutra-dmn --test tck -- --ignored --nocapture",
            root.display(),
            root.display()
        );
        return;
    }
    assert!(
        root.join("TestCases").is_dir(),
        "SUTRA_DMN_TCK_DIR={} has no TestCases/ dir",
        root.display()
    );

    println!("\nDMN-TCK conformance (engine: sutra-dmn DRG evaluator)");
    let mut l2 = Tally::default();
    run_level(&root, "compliance-level-2", &mut l2);
    report("compliance-level-2", &l2);

    let mut l3 = Tally::default();
    run_level(&root, "compliance-level-3", &mut l3);
    report("compliance-level-3", &l3);

    if !l2.fail_examples.is_empty() || !l3.fail_examples.is_empty() {
        println!("\n  Sample FAILs (engine produced a different value — real gaps):");
        for ex in l2.fail_examples.iter().chain(l3.fail_examples.iter()) {
            println!("    - {ex}");
        }
    }
    if !l3.err_examples.is_empty() {
        println!("\n  Sample evaluator errors (FEEL/DMN features to add):");
        for ex in l3.err_examples.iter() {
            println!("    - {ex}");
        }
    }
    if !l3.err_hist.is_empty() {
        println!("\n  L3 evaluator-error histogram (count × normalized signature):");
        let mut hist: Vec<_> = l3.err_hist.iter().collect();
        hist.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        for (sig, n) in hist.iter().take(30) {
            println!("    {n:>5} × {sig}");
        }
    }

    // Official vendor-submission pair, written in the DMN-TCK `TestResults/<Vendor>/<version>/`
    // layout.
    if let Ok(dir) = std::env::var("SUTRA_DMN_TCK_RESULTS_DIR") {
        write_tck_results(Path::new(&dir), &[&l2, &l3]);
    }

    // Full per-case dump (every non-pass outcome, uncapped) for offline gap analysis.
    if let Ok(path) = std::env::var("SUTRA_DMN_TCK_DUMP") {
        let mut all = String::new();
        for (lvl, t) in [("L2", &l2), ("L3", &l3)] {
            for line in &t.dump {
                all.push_str(lvl);
                all.push(' ');
                all.push_str(line);
                all.push('\n');
            }
        }
        std::fs::write(&path, all).expect("write SUTRA_DMN_TCK_DUMP file");
        println!("  Full non-pass dump written to {path}");
    }

    // The harness never hard-fails on UNSUPPORTED (those are the allowlist). A supported
    // construct producing the WRONG value IS a conformance gap; surface the count but let the
    // run report rather than abort, so progress is visible while the engine matures toward L3.
    println!(
        "\n  Totals: L2 pass={}/{}, L3 pass={}/{}\n",
        l2.pass,
        l2.pass + l2.fail,
        l3.pass,
        l3.pass + l3.fail
    );
}
