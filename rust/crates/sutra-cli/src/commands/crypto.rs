//! `sutra crypto provision-dek` — mint (or wrap a supplied) per-tenant data-encryption key and
//! store it, sealed under the KEK, in the `data_key` table (the envelope KeyProvider's boot-load
//! source).
//!
//! Provisioning is an out-of-band onboarding step, like `sutra migrate`: mint the wrapped DEK for a
//! tenant `key_id` BEFORE the engine boots with `sutra.crypto.envelope.enabled`, which then loads
//! the `data_key` map and unwraps DEKs under the same KEK. The KEK reference resolves through the
//! envref registry's builtin schemes (`env:` / `secret:`) — the same resolution the engine's
//! `sutra.crypto.envelope.kek` performs for those schemes. (Vendor KMS schemes — `vault:`,
//! `aws-secrets:`, … — need their resolver crates linked, a follow-up; resolve to an `env:`/`secret:`
//! reference for provisioning until then.)
//!
//! Only ciphertext leaves the tool: it prints the `key_id` + the wrapped byte length, never the raw
//! DEK material or the KEK.

use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sutra_crypto::Kek;
use sutra_persistence::stores::PgDataKeyStore;

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

/// Diagnostic codes owned by `sutra crypto` (the `SUTRA.CRYPTO.*` family).
pub mod codes {
    pub const KEK_UNRESOLVED: &str = "SUTRA.CRYPTO.KEK_UNRESOLVED";
    pub const BAD_DEK: &str = "SUTRA.CRYPTO.BAD_DEK";
    pub const CONNECT_FAILED: &str = "SUTRA.CRYPTO.CONNECT_FAILED";
    pub const WRAP_FAILED: &str = "SUTRA.CRYPTO.WRAP_FAILED";
    pub const STORE_FAILED: &str = "SUTRA.CRYPTO.STORE_FAILED";
    pub const KEY_EXISTS: &str = "SUTRA.CRYPTO.KEY_EXISTS";
}

#[derive(Debug, clap::Args)]
pub struct CryptoArgs {
    #[command(subcommand)]
    pub action: CryptoAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum CryptoAction {
    /// Mint (or wrap a supplied) DEK for a tenant key-id and store it wrapped in `data_key`.
    ProvisionDek(ProvisionDekArgs),
}

#[derive(Debug, clap::Args)]
pub struct ProvisionDekArgs {
    /// Database URL (postgres://…).
    #[arg(long, env = "SUTRA_DB_URL", value_name = "URL")]
    pub url: Option<String>,

    /// The crypto identity to provision — the migration-stable keyId (typically the tenant label)
    /// the envelope KeyProvider will look up at unwrap time.
    #[arg(long, value_name = "KEY_ID")]
    pub key_id: String,

    /// The key-encryption-key reference (`env:NAME` / `secret:…`), resolved whole-value at run time.
    #[arg(long, env = "SUTRA_CRYPTO_ENVELOPE_KEK", value_name = "REF")]
    pub kek: String,

    /// Optional explicit 32-byte DEK as 64 hex chars (onboarding a KNOWN key). Omit to mint a fresh
    /// random DEK.
    #[arg(long, value_name = "HEX")]
    pub dek_hex: Option<String>,

    /// Replace an existing wrapped DEK for this key-id (rotation). Without it, an existing key-id is
    /// left untouched and the command fails closed.
    #[arg(long)]
    pub force: bool,
}

pub fn execute(args: CryptoArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "crypto: {msg}");
            return exit::USAGE;
        }
    };
    match args.action {
        CryptoAction::ProvisionDek(args) => provision_dek(args, format, io),
    }
}

fn provision_dek(args: ProvisionDekArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    // Resolve the KEK whole-value through the envref registry's builtins (env:/secret:).
    let secret = match sutra_envref_spi::resolve_value(&args.kek) {
        Ok(secret) => secret,
        Err(e) => {
            let d = Diagnostic::error(
                codes::KEK_UNRESOLVED,
                format!("cannot resolve --kek '{}': {e}", args.kek),
            );
            let _ = writeln!(io.err, "{}", d.render_text());
            return exit::USAGE;
        }
    };
    let kek = Kek::from_secret(secret.as_bytes());

    // Mint a fresh random DEK, or wrap the supplied one. Only the wrapped (ciphertext) form is kept.
    let wrapped = match &args.dek_hex {
        Some(hex) => match parse_dek_hex(hex) {
            Ok(material) => kek.wrap_fresh_dek(&args.key_id, material),
            Err(msg) => {
                let d = Diagnostic::error(codes::BAD_DEK, msg);
                let _ = writeln!(io.err, "{}", d.render_text());
                return exit::USAGE;
            }
        },
        None => kek.wrap_random_dek(&args.key_id),
    };
    let wrapped = match wrapped {
        Ok(wrapped) => wrapped,
        Err(e) => {
            let d = Diagnostic::error(codes::WRAP_FAILED, format!("wrap DEK: {e}"));
            let _ = writeln!(io.err, "{}", d.render_text());
            return exit::FINDINGS;
        }
    };

    let Some(url) = args.url.as_deref() else {
        let _ = writeln!(
            io.err,
            "crypto provision-dek: --url (or SUTRA_DB_URL) is required"
        );
        return exit::USAGE;
    };

    block_on(async {
        let pool = match connect_pool(url).await {
            Ok(pool) => pool,
            Err(msg) => {
                let d = Diagnostic::error(codes::CONNECT_FAILED, msg);
                let _ = writeln!(io.err, "{}", d.render_text());
                return exit::USAGE;
            }
        };
        let store = PgDataKeyStore::new(pool);

        // Fail closed on an existing key-id unless --force (rotation must be explicit).
        if !args.force {
            match store.list_all().await {
                Ok(existing) if existing.iter().any(|w| w.key_id() == args.key_id) => {
                    let d = Diagnostic::error(
                        codes::KEY_EXISTS,
                        format!(
                            "a wrapped DEK for key-id '{}' already exists; pass --force to rotate it",
                            args.key_id
                        ),
                    );
                    let _ = writeln!(io.err, "{}", d.render_text());
                    return exit::FINDINGS;
                }
                Ok(_) => {}
                Err(e) => {
                    let d = Diagnostic::error(codes::STORE_FAILED, format!("read data_key: {e}"));
                    let _ = writeln!(io.err, "{}", d.render_text());
                    return exit::FINDINGS;
                }
            }
        }

        if let Err(e) = store.upsert(&wrapped).await {
            let d = Diagnostic::error(codes::STORE_FAILED, format!("upsert data_key: {e}"));
            let _ = writeln!(io.err, "{}", d.render_text());
            return exit::FINDINGS;
        }

        match format {
            ReportFormat::Text => {
                let _ = writeln!(
                    io.out,
                    "Provisioned a wrapped DEK for key-id '{}' ({} ciphertext bytes) into data_key{}.",
                    args.key_id,
                    wrapped.as_bytes().len(),
                    if args.dek_hex.is_some() { " (supplied)" } else { " (freshly minted)" }
                );
            }
            ReportFormat::Json => {
                let payload = serde_json::json!({
                    "keyId": args.key_id,
                    "wrappedBytes": wrapped.as_bytes().len(),
                    "generated": args.dek_hex.is_none(),
                });
                let _ = writeln!(io.out, "{payload}");
            }
        }
        exit::OK
    })
}

/// Parse a 64-hex-char (32-byte) DEK. Rejects any other length or a non-hex character.
fn parse_dek_hex(hex: &str) -> Result<[u8; 32], String> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return Err(format!(
            "--dek-hex must be exactly 64 hex chars (32 bytes), got {}",
            hex.len()
        ));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|_| "invalid hex".to_string())?;
        out[i] = u8::from_str_radix(pair, 16).map_err(|_| format!("invalid hex byte '{pair}'"))?;
    }
    Ok(out)
}

async fn connect_pool(url: &str) -> Result<sqlx::PgPool, String> {
    let options =
        PgConnectOptions::from_str(url).map_err(|e| format!("invalid database URL: {e}"))?;
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .map_err(|e| format!("cannot connect to the database: {e}"))
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dek_hex_parses_exactly_32_bytes() {
        let hex = "00".repeat(32); // 64 chars
        assert_eq!(parse_dek_hex(&hex).unwrap(), [0u8; 32]);
        let ff = "ff".repeat(32);
        assert_eq!(parse_dek_hex(&ff).unwrap(), [0xffu8; 32]);
    }

    #[test]
    fn dek_hex_rejects_wrong_length_and_non_hex() {
        assert!(parse_dek_hex("00").is_err(), "too short");
        assert!(parse_dek_hex(&"00".repeat(33)).is_err(), "too long");
        assert!(
            parse_dek_hex(&format!("zz{}", "00".repeat(31))).is_err(),
            "non-hex"
        );
    }
}
