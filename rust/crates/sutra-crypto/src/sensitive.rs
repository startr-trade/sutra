//! `Sensitive<T>` — a defense-in-depth wrapper that turns the "never log the raw body" CONVENTION
//! into a compile-time GUARANTEE.
//!
//! Its [`fmt::Debug`] NEVER reveals the wrapped value — it prints `Sensitive(***)` — so a stray
//! `tracing::debug!("{:?}", emission)` (or a `#[derive(Debug)]` on a struct that carries one) can
//! no longer leak the payload. Reads stay ergonomic: it [`Deref`]s to the inner value, so
//! `&sensitive` coerces to `&T` and slice/len/iter calls work unchanged; [`into_inner`] moves the
//! value out at a boundary that legitimately needs the bytes (e.g. encoding onto the wire).
//!
//! It lives in `sutra-crypto` (the data-protection-primitives crate — encryption + this masking
//! backstop) so every layer that carries a raw message body can wrap it cheaply: the executor
//! (`Emission.body`), the channels (`InboundMessage`/… bodies), and the persistence tier
//! (`OutboxEntry.body`) all depend on this leaf without pulling a heavier graph. `sutra-executor`
//! re-exports it as `sutra_executor::Sensitive` for backward compatibility.
//!
//! This is a BACKSTOP, not the enforcement itself: redaction (masked projection) and encryption at
//! rest are the load-bearing controls; `Sensitive<T>` just makes an accidental debug-leak of a
//! wrapped field a compile-time non-event.

use std::fmt;
use std::ops::Deref;

/// A value whose [`Debug`](fmt::Debug) is masked. See the module docs.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Sensitive<T>(T);

impl<T> Sensitive<T> {
    /// Wrap a value as sensitive.
    pub fn new(inner: T) -> Self {
        Sensitive(inner)
    }

    /// Borrow the inner value (also available via [`Deref`]).
    pub fn get(&self) -> &T {
        &self.0
    }

    /// Move the inner value out — at a boundary that legitimately needs the raw value (e.g. wire
    /// encoding). Every such call is an explicit, greppable acknowledgement that the value leaves
    /// its masked wrapper.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for Sensitive<T> {
    fn from(inner: T) -> Self {
        Sensitive(inner)
    }
}

impl<T> Deref for Sensitive<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// The whole point: the value is NEVER rendered — not its content, not even its length.
impl<T> fmt::Debug for Sensitive<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Sensitive(***)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_reveals_the_value() {
        let s = Sensitive::new(b"4111111111111111".to_vec());
        assert_eq!(format!("{s:?}"), "Sensitive(***)");
        // A struct that derives Debug and carries a Sensitive field is masked too.
        #[derive(Debug)]
        #[allow(dead_code)] // fields are exercised only through the derived Debug
        struct Msg {
            id: u32,
            body: Sensitive<Vec<u8>>,
        }
        let m = Msg {
            id: 7,
            body: Sensitive::new(b"secret".to_vec()),
        };
        let rendered = format!("{m:?}");
        assert!(rendered.contains("id: 7"));
        assert!(rendered.contains("Sensitive(***)"));
        assert!(!rendered.contains("secret"));
    }

    #[test]
    fn deref_makes_reads_transparent_and_into_inner_moves_out() {
        let s = Sensitive::new(vec![1u8, 2, 3]);
        assert_eq!(s.len(), 3); // Deref → Vec methods
        assert_eq!(&s[..], &[1, 2, 3]); // Deref → slice
        assert_eq!(s.into_inner(), vec![1, 2, 3]); // explicit unwrap at a boundary
    }

    #[test]
    fn from_and_equality() {
        let a: Sensitive<Vec<u8>> = vec![9u8].into();
        assert_eq!(a, Sensitive::new(vec![9u8]));
    }
}
