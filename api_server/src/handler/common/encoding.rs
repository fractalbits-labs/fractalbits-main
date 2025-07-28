//! Module containing various helpers for encoding

/// Encode &str for use in a URI
pub fn uri_encode(string: &str, encode_slash: bool) -> String {
    let mut result = String::with_capacity(string.len() * 2);
    for c in string.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '~' | '.' => result.push(c),
            '/' if encode_slash => result.push_str("%2F"),
            '/' if !encode_slash => result.push('/'),
            _ => {
                #[allow(clippy::format_collect)]
                result.push_str(
                    &format!("{c}")
                        .bytes()
                        .map(|b| format!("%{b:02X}"))
                        .collect::<String>(),
                );
            }
        }
    }
    result
}
