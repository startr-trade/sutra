//! Shared decoded-payload traversal helpers for redactors: RFC 6901 JSON-Pointer path building
//! over a `sutra_feel::FeelValue` tree. Locators are emitted RELATIVE to whatever payload root the
//! engine hands the redactor (an empty stack renders to `""`, the whole-document pointer) — a
//! redactor never assumes a `/body` prefix; the engine applies the returned pointer against the
//! same root it passed in.

/// One segment of a JSON-Pointer path during traversal.
#[derive(Clone, Debug)]
pub enum PathSegment {
    Key(String),
    Index(usize),
}

/// Render a path stack to an RFC 6901 JSON-Pointer string. An empty stack renders to `""`.
pub fn render_pointer(path: &[PathSegment]) -> String {
    let mut s = String::new();
    for seg in path {
        s.push('/');
        match seg {
            PathSegment::Key(k) => s.push_str(&escape_token(k)),
            PathSegment::Index(i) => s.push_str(&i.to_string()),
        }
    }
    s
}

/// RFC 6901 escaping for a map-key token: `~` → `~0`, `/` → `~1` (order matters: `~` first).
pub fn escape_token(k: &str) -> String {
    k.replace('~', "~0").replace('/', "~1")
}

/// Parse an RFC 6901 JSON-Pointer into its unescaped reference tokens — the inverse of
/// [`render_pointer`]. `""` yields an empty vec (the whole-document pointer). Returns `None`
/// for a malformed pointer (a non-empty pointer that does not start with `/`), so the engine
/// fails closed rather than mis-locate.
pub fn parse_pointer(pointer: &str) -> Option<Vec<String>> {
    if pointer.is_empty() {
        return Some(Vec::new());
    }
    let rest = pointer.strip_prefix('/')?;
    Some(rest.split('/').map(unescape_token).collect())
}

/// RFC 6901 unescaping for one reference token: `~1` → `/`, then `~0` → `~` (order matters —
/// `~1` first, so a literal `~01` in the source is not corrupted). Inverse of [`escape_token`].
pub fn unescape_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stack_is_whole_document_pointer() {
        assert_eq!(render_pointer(&[]), "");
    }

    #[test]
    fn keys_and_indices_render() {
        let path = vec![
            PathSegment::Key("tx".into()),
            PathSegment::Index(0),
            PathSegment::Key("pan".into()),
        ];
        assert_eq!(render_pointer(&path), "/tx/0/pan");
    }

    #[test]
    fn special_chars_are_rfc6901_escaped() {
        let path = vec![PathSegment::Key("a/b~c".into())];
        assert_eq!(render_pointer(&path), "/a~1b~0c");
    }

    #[test]
    fn parse_pointer_is_the_inverse_of_render() {
        assert_eq!(parse_pointer(""), Some(vec![]));
        assert_eq!(
            parse_pointer("/tx/0/pan"),
            Some(vec!["tx".to_string(), "0".to_string(), "pan".to_string()])
        );
        // `/` (single slash) points at the empty-string key, per RFC 6901.
        assert_eq!(parse_pointer("/"), Some(vec![String::new()]));
        // A non-empty pointer that does not start with `/` is malformed.
        assert_eq!(parse_pointer("tx/0"), None);
    }

    #[test]
    fn unescape_token_reverses_escape() {
        assert_eq!(unescape_token("a~1b~0c"), "a/b~c");
        // Round-trip a nasty key through escape → parse.
        let key = "a/b~c~1d";
        let ptr = render_pointer(&[PathSegment::Key(key.into())]);
        assert_eq!(parse_pointer(&ptr), Some(vec![key.to_string()]));
    }
}
