//! CTEI v1 codec from published format specification 4fc5486.
//! Tests use actual encoded bytes; no mocks. Container membership and same-image
//! call/step/parent validation are separate required integration boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordedEntryIdentity {
    pub call_key: u64,
    pub function_id: u64,
    pub entry_step: u64,
}
fn checked_domain(entry: RecordedEntryIdentity) -> Result<(), String> {
    if entry.call_key > i64::MAX as u64 || entry.entry_step > i64::MAX as u64 {
        return Err("entry.dat: call or step exceeds nonnegative i64 domain".into());
    }
    Ok(())
}
fn put_varint(mut value: u64, bytes: &mut Vec<u8>) {
    loop {
        let part = (value & 0x7f) as u8;
        value >>= 7;
        bytes.push(part | if value == 0 { 0 } else { 0x80 });
        if value == 0 {
            break;
        }
    }
}
fn get_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let mut value = 0u64;
    for group in 0..10 {
        let byte = *bytes.get(*cursor).ok_or("entry.dat: truncated canonical varint")?;
        *cursor += 1;
        if group == 9 && byte > 1 {
            return Err("entry.dat: varint overflow".into());
        }
        value |= u64::from(byte & 0x7f) << (group * 7);
        if byte & 0x80 == 0 {
            if group > 0 && byte == 0 {
                return Err("entry.dat: noncanonical varint".into());
            }
            return Ok(value);
        }
    }
    Err("entry.dat: varint overflow".into())
}
pub fn encode_entry_identity(entry: RecordedEntryIdentity) -> Result<Vec<u8>, String> {
    checked_domain(entry)?;
    let mut bytes = b"CTEI\x01\x00\x00\x00".to_vec();
    for value in [entry.call_key, entry.function_id, entry.entry_step] {
        put_varint(value, &mut bytes);
    }
    Ok(bytes)
}
pub fn decode_entry_identity(bytes: &[u8]) -> Result<RecordedEntryIdentity, String> {
    if bytes.len() < 8 || &bytes[..4] != b"CTEI" {
        return Err("entry.dat: invalid or truncated magic".into());
    }
    if bytes[4..6] != [1, 0] {
        return Err("entry.dat: unsupported version".into());
    }
    if bytes[6..8] != [0, 0] {
        return Err("entry.dat: nonzero reserved field".into());
    }
    let mut cursor = 8;
    let entry = RecordedEntryIdentity {
        call_key: get_varint(bytes, &mut cursor)?,
        function_id: get_varint(bytes, &mut cursor)?,
        entry_step: get_varint(bytes, &mut cursor)?,
    };
    if cursor != bytes.len() {
        return Err("entry.dat: trailing bytes".into());
    }
    checked_domain(entry)?;
    Ok(entry)
}
/// Validate the final same-image ownership, after ordinary partial-call finalization.
pub fn validate_entry_image(
    entry: RecordedEntryIdentity,
    records: &[crate::call_stream::CallStreamRecord],
    step_count: u64,
    names: &std::collections::BTreeMap<u64, Vec<u8>>,
) -> Result<(), String> {
    checked_domain(entry)?;
    let refuse = || "entry.dat: inconsistent finalized call/step/parent ownership".to_string();
    let by_key: std::collections::BTreeMap<_, _> = records.iter().map(|r| (r.call_key, r)).collect();
    if by_key.len() != records.len() || entry.entry_step >= step_count {
        return Err(refuse());
    }
    let marked = *by_key.get(&entry.call_key).ok_or_else(refuse)?;
    if marked.function_id != entry.function_id || marked.first_step_id != entry.entry_step {
        return Err(refuse());
    }
    let owner = records
        .iter()
        .filter(|r| r.first_step_id <= entry.entry_step && entry.entry_step <= r.last_step_id)
        .max_by_key(|r| r.call_key)
        .ok_or_else(refuse)?;
    if owner.call_key != entry.call_key {
        return Err(refuse());
    }
    let mut current = marked;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        if !seen.insert(current.call_key) || current.first_step_id > current.last_step_id || current.last_step_id >= step_count {
            return Err(refuse());
        }
        if current.parent_key == -1 {
            if current.depth != 0 {
                return Err(refuse());
            }
            if names.get(&0).map(Vec::as_slice) == Some(b"<toplevel>".as_slice()) && (current.call_key != 0 || current.function_id != 0) {
                return Err(refuse());
            }
            break;
        }
        let parent_key = u64::try_from(current.parent_key).map_err(|_| refuse())?;
        let parent = *by_key.get(&parent_key).ok_or_else(refuse)?;
        if parent.call_key >= current.call_key
            || parent.depth.checked_add(1) != Some(current.depth)
            || parent.first_step_id > current.first_step_id
            || parent.last_step_id < current.last_step_id
            || parent.children.iter().filter(|key| **key == current.call_key).count() != 1
        {
            return Err(refuse());
        }
        current = parent;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_zero_wire_and_roundtrip() {
        let entry = RecordedEntryIdentity {
            call_key: 0,
            function_id: 0,
            entry_step: 0,
        };
        assert_eq!(encode_entry_identity(entry).unwrap(), b"CTEI\x01\x00\x00\x00\x00\x00\x00");
        assert_eq!(decode_entry_identity(&encode_entry_identity(entry).unwrap()).unwrap(), entry);
    }
    #[test]
    fn full_supported_domains() {
        let entry = RecordedEntryIdentity {
            call_key: i64::MAX as u64,
            function_id: u64::MAX,
            entry_step: i64::MAX as u64,
        };
        assert_eq!(decode_entry_identity(&encode_entry_identity(entry).unwrap()).unwrap(), entry);
    }
    #[test]
    fn refuses_noncanonical_overflow_truncation_and_tail() {
        for payload in [vec![0x80, 0, 0, 0], vec![0xff; 10], vec![0x80], vec![0, 0, 0, 0]] {
            let mut bytes = b"CTEI\x01\x00\x00\x00".to_vec();
            bytes.extend(payload);
            assert!(decode_entry_identity(&bytes).is_err());
        }
    }
    #[test]
    fn refuses_version_reserved_and_out_of_domain() {
        for header in [b"CTEI\x02\x00\x00\x00", b"CTEI\x01\x00\x01\x00"] {
            let mut bytes = header.to_vec();
            bytes.extend([0, 0, 0]);
            assert!(decode_entry_identity(&bytes).is_err());
        }
        assert!(
            encode_entry_identity(RecordedEntryIdentity {
                call_key: u64::MAX,
                function_id: 0,
                entry_step: 0
            })
            .is_err()
        );
        let mut bytes = b"CTEI\x01\x00\x00\x00".to_vec();
        put_varint(u64::MAX, &mut bytes);
        bytes.extend([0, 0]);
        assert!(decode_entry_identity(&bytes).is_err());
    }
}
