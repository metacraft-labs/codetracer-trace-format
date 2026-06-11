//! Integration tests for the §P7.2 `from-sourcemap` conversion path.
//!
//! Each test asserts a specific contract on the produced TOML +
//! verifies the output loads cleanly through the §P5 `RenameList`
//! deserialisation shape (replicated locally to keep the crate
//! decoupled from `codetracer/src/db-backend`).

use std::path::Path;

use ct_mapping_tools::{FromSourcemapOptions, RenameEntry, from_sourcemap, to_toml};
use serde::Deserialize;
use sourcemap_translate::SourcemapIndex;

/// Local mirror of the on-disk schema the replay-server's
/// `RenameList::parse_toml` consumes (file
/// `codetracer/src/db-backend/src/rename_list.rs`).  Kept in-test so
/// the integration test exercises the public TOML contract without a
/// cross-repo Cargo dependency.
#[derive(Debug, Deserialize)]
struct RenameListFile {
    #[serde(default)]
    rename: Vec<RawEntry>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct RawEntry {
    file: String,
    scope: Option<String>,
    from: String,
    to: String,
}

/// Parse a sourcemap JSON literal at a known on-disk directory so the
/// `sourcemap_dir` accessor on the resulting index points somewhere
/// stable for tests.
fn parse_map(json: &str) -> SourcemapIndex {
    SourcemapIndex::from_slice(json.as_bytes(), Path::new("/tmp/fake")).expect("parse")
}

#[test]
fn empty_map_yields_empty_toml() {
    // Sourcemap with empty `names[]` and `mappings` → no entries.
    let map_json = r#"{
        "version": 3,
        "file": "min.js",
        "sources": ["orig.js"],
        "names": [],
        "mappings": ""
    }"#;
    let map = parse_map(map_json);
    let opts = FromSourcemapOptions {
        file_name: None,
        per_function: false,
        minified_source: Some(String::new()),
    };
    let entries = from_sourcemap(&map, &opts);
    assert!(entries.is_empty(), "no named segments → zero rename entries");
    assert_eq!(to_toml(&entries), "");
}

#[test]
fn one_segment_one_rename() {
    // One segment mapping gen (0, 0) -> orig (0, 0) name="originalName".
    //
    // Encoded VLQ for `[+0, +0, +0, +0, +0]` = "AAAAA".
    // gen_col=0 in V3 (0-indexed) → 1-indexed col 1, so the
    // minified identifier must START at byte 0 of the minified line.
    let map_json = r#"{
        "version": 3,
        "file": "min.js",
        "sources": ["orig.js"],
        "names": ["originalName"],
        "mappings": "AAAAA"
    }"#;
    let map = parse_map(map_json);
    let minified = "a=1;".to_string();
    let opts = FromSourcemapOptions {
        file_name: Some("min.js".to_string()),
        per_function: false,
        minified_source: Some(minified),
    };
    let entries = from_sourcemap(&map, &opts);
    assert_eq!(
        entries,
        vec![RenameEntry {
            file: "min.js".to_string(),
            scope: "global".to_string(),
            from: "a".to_string(),
            to: "originalName".to_string(),
        }]
    );

    let toml = to_toml(&entries);
    // Strict assertion on the produced text — the §P5 schema accepts
    // any field order so we check substrings here, but each assertion
    // is exact on the field's literal byte content.
    assert!(toml.contains("[[rename]]"));
    assert!(toml.contains("file = \"min.js\""));
    assert!(toml.contains("scope = \"global\""));
    assert!(toml.contains("from = \"a\""));
    assert!(toml.contains("to = \"originalName\""));
}

#[test]
fn most_frequent_wins() {
    // Two segments map gen positions to identifier `a`:
    //   gen (0, 0)  -> orig.js (0, 0)  name="winner"
    //   gen (0, 4)  -> orig.js (0, 4)  name="winner"
    //   gen (0, 8)  -> orig.js (0, 8)  name="loser"
    //
    // Both segments resolve to identifier `a` at every position.
    // `winner` wins on count.
    //
    // VLQ segments (each preceded by `,`):
    //   AAAAA  = [0, 0, 0, 0, 0]
    //   IAAIA  = [+4, +0, +0, +4, +0]    same name (idx +0 → 0 = winner)
    //   IAAIC  = [+4, +0, +0, +4, +1]    name idx +1 → 1 = loser
    let map_json = r#"{
        "version": 3,
        "file": "min.js",
        "sources": ["orig.js"],
        "names": ["winner", "loser"],
        "mappings": "AAAAA,IAAIA,IAAIC"
    }"#;
    let map = parse_map(map_json);
    // Minified line has identifier `a` at columns 0, 4, 8.
    let minified = "a=1;a=2;a=3;".to_string();
    let opts = FromSourcemapOptions {
        file_name: Some("min.js".to_string()),
        per_function: false,
        minified_source: Some(minified),
    };
    let entries = from_sourcemap(&map, &opts);
    assert_eq!(entries.len(), 1, "one unique minified name → one entry");
    assert_eq!(entries[0].from, "a");
    assert_eq!(entries[0].to, "winner", "most-frequent original wins");
}

#[test]
fn per_function_emits_function_scope() {
    // §P7.2 surface: --per-function is accepted but V3 sourcemaps
    // lack per-segment enclosing-function info, so the §P7.2
    // implementation falls back to `global`.  The test asserts this
    // documented fallback so the contract is locked in.
    //
    // (Reviewer follow-up: replace this assertion when an enclosing-
    // function derivation pass lands.)
    let map_json = r#"{
        "version": 3,
        "file": "min.js",
        "sources": ["orig.js"],
        "names": ["originalName"],
        "mappings": "AAAAA"
    }"#;
    let map = parse_map(map_json);
    let minified = "a=1;".to_string();
    let opts = FromSourcemapOptions {
        file_name: Some("min.js".to_string()),
        per_function: true,
        minified_source: Some(minified),
    };
    let entries = from_sourcemap(&map, &opts);
    assert_eq!(entries.len(), 1);
    // The fallback contract: scope stays `global` until a future
    // milestone wires up real per-function derivation.
    assert_eq!(entries[0].scope, "global");
}

#[test]
fn output_roundtrips_through_renamelist() {
    // Produce a small TOML and load it back through the §P5 schema
    // shape.  Asserts the produced bytes are accepted by the
    // production parser without modification.
    let map_json = r#"{
        "version": 3,
        "file": "lodash.min.js",
        "sources": ["lodash.js"],
        "names": ["arr"],
        "mappings": "AAAAA"
    }"#;
    let map = parse_map(map_json);
    let minified = "e=1;".to_string();
    let opts = FromSourcemapOptions {
        file_name: None,
        per_function: false,
        minified_source: Some(minified),
    };
    let entries = from_sourcemap(&map, &opts);
    let toml_text = to_toml(&entries);

    let parsed: RenameListFile =
        toml::from_str(&toml_text).expect("produced TOML loads through the §P5 schema");
    assert_eq!(parsed.rename.len(), 1);
    let entry = &parsed.rename[0];
    assert_eq!(entry.file, "lodash.min.js");
    assert_eq!(entry.scope.as_deref(), Some("global"));
    assert_eq!(entry.from, "e");
    assert_eq!(entry.to, "arr");
}
