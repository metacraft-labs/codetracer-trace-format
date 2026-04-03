use crate::CtfsError;

const BASE40_ALPHABET: &[u8] = b"\x000123456789abcdefghijklmnopqrstuvwxyz./-";
const BASE: u64 = 40;
const MAX_NAME_LEN: usize = 12;

fn char_to_index(c: char) -> Result<u64, CtfsError> {
    match c {
        '0'..='9' => Ok((c as u64) - ('0' as u64) + 1),
        'a'..='z' => Ok((c as u64) - ('a' as u64) + 11),
        '.' => Ok(37),
        '/' => Ok(38),
        '-' => Ok(39),
        _ => Err(CtfsError::InvalidBase40Char(c)),
    }
}

fn index_to_char(i: u64) -> char {
    BASE40_ALPHABET[i as usize] as char
}

/// Encode a filename (up to 12 chars) into a single u64 using base40.
pub fn base40_encode(name: &str) -> Result<u64, CtfsError> {
    if name.is_empty() {
        return Ok(0);
    }
    if name.len() > MAX_NAME_LEN {
        return Err(CtfsError::NameTooLong(name.to_string()));
    }
    let mut result: u64 = 0;
    for (i, c) in name.chars().enumerate() {
        let idx = char_to_index(c)?;
        result += idx * BASE.pow(i as u32);
    }
    Ok(result)
}

/// Decode a base40-encoded u64 back to a filename string.
pub fn base40_decode(mut encoded: u64) -> String {
    if encoded == 0 {
        return String::new();
    }
    let mut chars = Vec::new();
    while encoded > 0 {
        let remainder = encoded % BASE;
        encoded /= BASE;
        let c = index_to_char(remainder);
        if c != '\0' {
            chars.push(c);
        } else {
            // Trailing null — stop
            break;
        }
    }
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base40_roundtrip_all_chars() {
        // Test every single character in the alphabet
        let all_chars = "0123456789abcdefghijklmnopqrstuvwxyz./-";
        for c in all_chars.chars() {
            let s = c.to_string();
            let encoded = base40_encode(&s).unwrap();
            let decoded = base40_decode(encoded);
            assert_eq!(decoded, s, "roundtrip failed for char '{}'", c);
        }
    }

    #[test]
    fn test_base40_roundtrip_filenames() {
        let filenames = ["meta.json", "t00000000001", "syncord.log"];
        for name in &filenames {
            let encoded = base40_encode(name).unwrap();
            let decoded = base40_decode(encoded);
            assert_eq!(&decoded, name, "roundtrip failed for '{}'", name);
        }
    }

    #[test]
    fn test_base40_zero_padding() {
        let enc1 = base40_encode("t00000000001").unwrap();
        let enc2 = base40_encode("t00000000002").unwrap();
        assert!(enc2 > enc1, "t00000000002 ({}) should be > t00000000001 ({})", enc2, enc1);
    }

    #[test]
    fn test_base40_numeric_sorting() {
        // Verify incrementing CT_TID in filename = predictable u64 increment.
        // When the counter occupies the same digit position (last char),
        // the encoded values increase by a constant stride.
        let enc1 = base40_encode("t00000000001").unwrap();
        let enc2 = base40_encode("t00000000002").unwrap();
        let stride = enc2 - enc1;
        assert!(stride > 0, "stride should be positive");

        let mut prev = enc1;
        for i in 2..=9u64 {
            let name = format!("t0000000000{}", i);
            let enc = base40_encode(&name).unwrap();
            assert!(enc > prev, "{} should encode to > previous", name);
            assert_eq!(enc - prev, stride, "increment should be constant for {}", name);
            prev = enc;
        }
    }

    #[test]
    fn test_base40_edge_cases() {
        // Empty string -> 0
        assert_eq!(base40_encode("").unwrap(), 0);
        // decode(0) -> ""
        assert_eq!(base40_decode(0), "");
        // 12-char max string roundtrips
        let max_name = "abcdefghijkl";
        assert_eq!(max_name.len(), 12);
        let encoded = base40_encode(max_name).unwrap();
        let decoded = base40_decode(encoded);
        assert_eq!(decoded, max_name);
        // 13-char should fail
        assert!(base40_encode("abcdefghijklm").is_err());
    }
}
