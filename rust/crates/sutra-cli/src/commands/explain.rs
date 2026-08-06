//! `sutra explain '<feel>'` — evaluate a FEEL expression against an optional flat
//! context, one-shot or as a line-by-line REPL on stdin. Built directly on `sutra-feel`
//! (the engine's expression language); made for "why did this gateway take the wrong
//! path" debugging.

use std::path::PathBuf;

use sutra_feel::value::canonical_string_of;
use sutra_feel::{expressions, FeelContext, FeelError, FeelValue};

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct ExplainArgs {
    /// FEEL expression. If omitted, enters REPL mode on stdin.
    pub expression: Option<String>,

    /// Context file with flat `key: value` (or `key=value`) pairs, one per line.
    /// Values coerce to boolean/number when they parse as such, else string.
    #[arg(long, value_name = "FILE")]
    pub context: Option<PathBuf>,
}

pub fn execute(args: ExplainArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "explain: {msg}");
            return exit::USAGE;
        }
    };
    let context = match load_context(args.context.as_deref()) {
        Ok(ctx) => ctx,
        Err(msg) => {
            let _ = writeln!(io.err, "explain: {msg}");
            return exit::USAGE;
        }
    };
    match args.expression.as_deref().map(str::trim) {
        Some(expr) if !expr.is_empty() => eval_once(expr, &context, format, io),
        _ => repl(&context, format, io),
    }
}

fn eval_once(expr: &str, context: &FeelContext, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    match expressions::eval(expr, context) {
        Ok(value) => {
            match format {
                ReportFormat::Text => {
                    let _ = writeln!(io.out, "{expr}  =>  {}", canonical_string_of(&value));
                }
                ReportFormat::Json => {
                    let payload = serde_json::json!({
                        "expression": expr,
                        "result": feel_to_json(&value),
                    });
                    let _ = writeln!(io.out, "{payload}");
                }
            }
            exit::OK
        }
        Err(e) => {
            let _ = writeln!(io.err, "{}", feel_diagnostic(&e).render_text());
            exit::FINDINGS
        }
    }
}

fn repl(context: &FeelContext, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let _ = writeln!(
        io.out,
        "sutra explain — FEEL REPL (:quit, :q, :exit or Ctrl-D to exit)"
    );
    loop {
        let mut line = String::new();
        match io.input.read_line(&mut line) {
            Ok(0) => return exit::OK, // EOF
            Ok(_) => {}
            Err(e) => {
                let _ = writeln!(io.err, "explain: REPL aborted: {e}");
                return exit::USAGE;
            }
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == ":quit" || line == ":q" || line == ":exit" {
            return exit::OK;
        }
        // Errors are printed and the session continues — a REPL never fails the process.
        eval_once(line, context, format, io);
    }
}

fn load_context(file: Option<&std::path::Path>) -> Result<FeelContext, String> {
    let Some(file) = file else {
        return Ok(FeelContext::new());
    };
    if !file.is_file() {
        return Err(format!("context file not found: {}", file.display()));
    }
    let body = std::fs::read_to_string(file).map_err(|e| format!("failed to read context: {e}"))?;
    let mut context = FeelContext::new();
    // Deliberately tiny flat-pair parser: full structured context
    // files can be pre-flattened with jq/yq into key=value lines.
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(sep) = line.find(':').or_else(|| line.find('=')) else {
            continue;
        };
        let key = line[..sep].trim().replace('"', "");
        let value = line[sep + 1..].trim().replace('"', "");
        context.insert(key, coerce(&value));
    }
    Ok(context)
}

fn coerce(value: &str) -> FeelValue {
    if value.eq_ignore_ascii_case("true") {
        return FeelValue::Boolean(true);
    }
    if value.eq_ignore_ascii_case("false") {
        return FeelValue::Boolean(false);
    }
    if let Ok(n) = value.parse::<i64>() {
        return FeelValue::from(n);
    }
    if let Ok(f) = value.parse::<f64>() {
        if f.is_finite() {
            return FeelValue::from(f);
        }
    }
    FeelValue::from(value)
}

fn feel_diagnostic(e: &FeelError) -> Diagnostic {
    let d = Diagnostic::error(&e.code, e.message.clone());
    match &e.location {
        Some(loc) => d.at(format!("{}:{}:{}", loc.uri, loc.line, loc.column)),
        None => d,
    }
}

fn feel_to_json(v: &FeelValue) -> serde_json::Value {
    match v {
        FeelValue::Null => serde_json::Value::Null,
        FeelValue::Boolean(b) => serde_json::Value::Bool(*b),
        FeelValue::Number(n) => serde_json::from_str::<serde_json::Number>(&n.to_string())
            .map(serde_json::Value::Number)
            .unwrap_or_else(|_| serde_json::Value::String(n.to_string())),
        FeelValue::String(s) => serde_json::Value::String(s.clone()),
        FeelValue::Instant(..)
        | FeelValue::Date(_)
        | FeelValue::Time(..)
        | FeelValue::Duration(_)
        | FeelValue::Function(_)
        | FeelValue::Invocable(_)
        | FeelValue::Range(_) => serde_json::Value::String(canonical_string_of(v)),
        FeelValue::List(items) => {
            serde_json::Value::Array(items.iter().map(feel_to_json).collect())
        }
        FeelValue::Map(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), feel_to_json(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    //! Behavior carried over from the reference baseline's test suite.

    use super::*;
    use crate::output::run_captured;

    fn run(args: ExplainArgs, format: Option<&str>, input: &str) -> (i32, String, String) {
        let global = GlobalArgs {
            format: format.map(str::to_owned),
            verbose: 0,
        };
        run_captured(input, |io| execute(args, &global, io))
    }

    #[test]
    fn one_shot_evaluates_a_feel_literal() {
        let (code, out, _) = run(
            ExplainArgs {
                expression: Some("1 + 2".into()),
                context: None,
            },
            None,
            "",
        );
        assert_eq!(code, crate::exit::OK);
        assert_eq!(out, "1 + 2  =>  3\n");
    }

    #[test]
    fn context_file_loads_key_value_pairs() {
        let dir = std::env::temp_dir().join(format!("sutra-explain-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ctx = dir.join("ctx.txt");
        std::fs::write(&ctx, "amount: 100\n").unwrap();
        let (code, out, _) = run(
            ExplainArgs {
                expression: Some("amount".into()),
                context: Some(ctx.clone()),
            },
            None,
            "",
        );
        std::fs::remove_file(&ctx).ok();
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("100"), "{out}");
    }

    #[test]
    fn repl_mode_exits_cleanly_on_colon_quit() {
        let (code, out, _) = run(
            ExplainArgs {
                expression: None,
                context: None,
            },
            None,
            "1 + 2\n:quit\n",
        );
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("FEEL REPL"), "{out}");
        assert!(out.contains("1 + 2  =>  3"), "{out}");
    }

    #[test]
    fn repl_exits_cleanly_on_eof_and_survives_errors() {
        let (code, _, err) = run(
            ExplainArgs {
                expression: None,
                context: None,
            },
            None,
            "1 +\n",
        );
        assert_eq!(code, crate::exit::OK);
        assert!(err.contains("SUTRA.FEEL."), "{err}");
    }

    #[test]
    fn json_format_emits_json_object() {
        let (code, out, _) = run(
            ExplainArgs {
                expression: Some("true".into()),
                context: None,
            },
            Some("json"),
            "",
        );
        assert_eq!(code, crate::exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["expression"], "true");
        assert_eq!(v["result"], true);
    }

    #[test]
    fn missing_context_file_is_a_usage_error() {
        let (code, _, err) = run(
            ExplainArgs {
                expression: Some("1".into()),
                context: Some(PathBuf::from("/does/not/exist.yaml")),
            },
            None,
            "",
        );
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("context file not found"), "{err}");
    }

    #[test]
    fn eval_error_is_a_finding_with_a_diagnostic_line() {
        let (code, _, err) = run(
            ExplainArgs {
                expression: Some("1 +".into()),
                context: None,
            },
            None,
            "",
        );
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(err.starts_with("[ERROR] SUTRA.FEEL."), "{err}");
    }

    #[test]
    fn numbers_stay_numbers_in_json() {
        let (_, out, _) = run(
            ExplainArgs {
                expression: Some("1.5 * 2".into()),
                context: None,
            },
            Some("json"),
            "",
        );
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"].to_string(), "3.0");
    }
}
