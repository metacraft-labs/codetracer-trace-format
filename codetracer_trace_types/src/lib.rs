mod base64;
mod types;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_equality_of_value_records() {
        let a = ValueRecord::Int { i: 0, type_id: TypeId(0) }; // just an example type_id
        let b = ValueRecord::Int { i: 0, type_id: TypeId(0) };
        let different = ValueRecord::Int { i: 1, type_id: TypeId(0) };

        assert_eq!(a, b);
        assert_ne!(a, different);
    }

    /// M-REC-1 acceptance: `TraceMetadata::new` mints a canonical
    /// UUIDv7 `recording_id`, the version nibble is 7, and the
    /// variant nibble is one of {8,9,a,b}.
    #[test]
    fn recording_id_is_uuid_v7_canonical() {
        let m = TraceMetadata::new("hello", vec!["arg".into()], PathBuf::from("/tmp"));
        let parsed = uuid::Uuid::parse_str(&m.recording_id).expect("recording_id must parse as a UUID");
        assert_eq!(parsed.get_version_num(), 7, "must be UUIDv7");
        // hyphenated lowercase is the canonical form we agreed on.
        assert_eq!(m.recording_id.len(), 36);
        assert_eq!(m.recording_id, m.recording_id.to_lowercase());
        // Hyphens at the canonical positions.
        let bytes = m.recording_id.as_bytes();
        assert_eq!(bytes[8], b'-');
        assert_eq!(bytes[13], b'-');
        assert_eq!(bytes[18], b'-');
        assert_eq!(bytes[23], b'-');
        // Variant nibble in {8,9,a,b}.
        assert!(
            matches!(bytes[19], b'8' | b'9' | b'a' | b'b'),
            "variant nibble must be 8,9,a, or b; got {}",
            bytes[19] as char
        );
    }

    /// M-REC-1 acceptance: two recordings made on the same host one
    /// second apart sort by id lex-ascending and have a strictly
    /// increasing embedded ms timestamp.  This is the load-bearing
    /// sortability property UUIDv7 was chosen for.
    #[test]
    fn two_ids_made_one_second_apart_sort_lex_ascending() {
        let a = TraceMetadata::new("p", vec![], PathBuf::from("/"));
        sleep(Duration::from_millis(1050));
        let b = TraceMetadata::new("p", vec![], PathBuf::from("/"));

        // Lexicographic string comparison must agree with creation
        // order — this is the property that makes `ls <traces>` and
        // SQLite indexes "just work".
        assert!(
            a.recording_id < b.recording_id,
            "later id must sort after earlier; a={} b={}",
            a.recording_id,
            b.recording_id,
        );

        // The embedded ms timestamp (first 48 bits) must also
        // strictly increase.  We parse via the `uuid` crate's
        // hyphenated form so this test exercises the canonical
        // round-trip.
        let ua = uuid::Uuid::parse_str(&a.recording_id).unwrap();
        let ub = uuid::Uuid::parse_str(&b.recording_id).unwrap();
        let bytes_a = ua.as_bytes();
        let bytes_b = ub.as_bytes();
        let ms_a = ((bytes_a[0] as u64) << 40)
            | ((bytes_a[1] as u64) << 32)
            | ((bytes_a[2] as u64) << 24)
            | ((bytes_a[3] as u64) << 16)
            | ((bytes_a[4] as u64) << 8)
            | (bytes_a[5] as u64);
        let ms_b = ((bytes_b[0] as u64) << 40)
            | ((bytes_b[1] as u64) << 32)
            | ((bytes_b[2] as u64) << 24)
            | ((bytes_b[3] as u64) << 16)
            | ((bytes_b[4] as u64) << 8)
            | (bytes_b[5] as u64);
        assert!(ms_a < ms_b, "embedded ms must strictly increase; ms_a={} ms_b={}", ms_a, ms_b,);
    }

    /// JSON round-trip preserves the `recording_id` field exactly.
    /// Pre-1.0 the field is required; deserialization without it
    /// must fail.
    #[test]
    fn recording_id_round_trips_through_json() {
        let original = TraceMetadata {
            recording_id: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb".into(),
            workdir: PathBuf::from("/tmp"),
            program: "p".into(),
            args: vec![],
        };
        let serialized = serde_json::to_string(&original).unwrap();
        assert!(
            serialized.contains("\"recording_id\":\"01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb\""),
            "serialized form missing recording_id: {}",
            serialized,
        );
        let decoded: TraceMetadata = serde_json::from_str(&serialized).unwrap();
        assert_eq!(decoded.recording_id, original.recording_id);
    }

    #[test]
    fn deserializing_without_recording_id_is_rejected() {
        // Pre-1.0: no backwards compatibility.  A meta.json (or any
        // other JSON sidecar) without recording_id must fail to
        // deserialize so callers get a clear early error rather than
        // silently working with an empty id.
        let bad = r#"{"program":"p","args":[],"workdir":"/tmp"}"#;
        let err = serde_json::from_str::<TraceMetadata>(bad).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("recording_id"), "error should mention recording_id; got {}", msg,);
    }
}
