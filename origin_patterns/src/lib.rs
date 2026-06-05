//! Shared schema for CodeTracer `origin-patterns.toml` files.
//!
//! This crate defines the *file-level* TOML structure for origin-pattern
//! files. The structure is shared between two consumers:
//!
//! - **Recorders** (via `codetracer_origin_pattern_discovery`): walk the
//!   filesystem at record-start, collect every
//!   `.codetracer/origin-patterns.toml`, copy them verbatim into the
//!   trace, and build the `meta_dat/origin-patterns/index.toml` manifest.
//!   Recorders only need to read enough of the file to confirm it parses
//!   as TOML and to extract the optional language hint — they do not
//!   compile the matchers (the classifier does that at replay time).
//! - **The db-backend's `origin-classifier`**: re-parses the files at
//!   replay time and produces the executable `PatternRule` set the
//!   classifier walks while building origin chains.
//!
//! Keeping the *schema* in this small crate means recorders never link
//! against the heavier classifier (which pulls SHA-256, regex, and the
//! AST library transitively). Both sides agree on the field names by
//! depending on the same `RawTomlFile` / `RawTomlRule` definitions.
//!
//! Spec reference: GUI/Debugging-Features/Value-Origin-Tracking.md §7.4
//! "Pattern file schema".

#![forbid(unsafe_code)]

use std::path::Path;

use serde::{Deserialize, Serialize};

/// One pattern file as written on disk.
///
/// Each TOML file declares zero or more rules under one of three table
/// names (`forwarder`, `trivial_copy`, `computational`). Unknown fields
/// are intentionally permitted (`#[serde(default)]`) so older readers can
/// load newer files that grow additional metadata.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct RawPatternFile {
    /// Forwarder rules (the matched call's value is treated as the
    /// receiver of the call). Spec §7.4 first table.
    #[serde(default)]
    pub forwarder: Vec<RawPatternRule>,
    /// Trivial-copy rules (non-call expressions that should be treated
    /// as forwarders). Spec §7.4 second table.
    #[serde(default)]
    pub trivial_copy: Vec<RawPatternRule>,
    /// Computational overrides (calls the default rules would treat as
    /// trivial forwarders). Spec §7.4 third table.
    #[serde(default)]
    pub computational: Vec<RawPatternRule>,
}

/// One rule inside a [`RawPatternFile`].
///
/// The optional `kind` field lets a TOML file override the
/// classification implied by the table name (e.g. mark a rule inside the
/// `forwarder` table as `field_access` instead of `trivial_copy`). The
/// classifier validates `kind` and rejects unknown values.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct RawPatternRule {
    /// The matcher expression, e.g. `$x.clone()` or
    /// `memcpy($_dst, $src, $_n)`.
    #[serde(rename = "match")]
    pub match_expr: String,
    /// Capture name to follow backward when this rule matches. Required
    /// for forwarder and trivial-copy rules; ignored for computational
    /// rules (spec §7.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<String>,
    /// Human-readable description shown by the GUI's "Show pattern
    /// provenance" affordance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional language hint (e.g. "rust", "python"). When omitted, the
    /// rule applies to every language.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Explicit `OriginKind` override. Defaults to the table's implicit
    /// kind when absent. Accepted values: "trivial_copy",
    /// "computational", "field_access", "index_access",
    /// "function_call".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// Layout of `meta_dat/origin-patterns/index.toml`, the manifest produced
/// by the recorder's discovery library and consumed by the classifier.
///
/// The manifest exists so the discovery order at record-time defines the
/// loading order at replay-time, even when the embedded library
/// directories happen to sort differently across operating systems.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct EmbeddedPatternsIndex {
    /// Discovery entries in the order the recorder walked them.
    #[serde(default)]
    pub libraries: Vec<EmbeddedPatternEntry>,
}

/// One entry in `index.toml`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct EmbeddedPatternEntry {
    /// Canonical library identifier (sub-directory under
    /// `meta_dat/origin-patterns/`). For the recorded program itself
    /// recorders use the workspace name (or the program filename when
    /// no package manager identifies it).
    pub library_id: String,
    /// Filename of the embedded TOML file, relative to
    /// `meta_dat/origin-patterns/<library_id>/`.
    pub filename: String,
    /// Absolute source path the pattern file was read from at record
    /// time, kept for diagnostic display only — the classifier never
    /// reads from this path at replay time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

/// File name of the trace-local overrides file (per spec §7.4).
pub const OVERRIDES_FILE: &str = "_overrides.toml";

/// File name of the manifest the recorder writes alongside the embedded
/// pattern directories.
pub const INDEX_FILE: &str = "index.toml";

/// Sub-directory inside `meta_dat/` that holds the embedded patterns and
/// the manifest.
pub const META_DAT_SUBDIR: &str = "origin-patterns";

/// Conventional filename a recorder looks for inside `.codetracer/`.
pub const PATTERN_FILENAME: &str = "origin-patterns.toml";

/// Conventional directory name the recorder scans for pattern files.
pub const DOT_CODETRACER_DIR: &str = ".codetracer";

/// Parse a pattern file from a TOML string.
///
/// Returns the structured form on success or a TOML error pointing at
/// the offending row/column.
pub fn parse_pattern_file(text: &str) -> Result<RawPatternFile, toml::de::Error> {
    toml::from_str(text)
}

/// Serialise a pattern file back to TOML.
///
/// The classifier never round-trips through this serializer (it copies
/// pattern files byte-for-byte from disk into the trace), but
/// discovery-library tests and override tooling use it to construct
/// minimal files programmatically.
pub fn serialise_pattern_file(file: &RawPatternFile) -> Result<String, toml::ser::Error> {
    toml::to_string(file)
}

/// Parse an `index.toml` manifest.
pub fn parse_index(text: &str) -> Result<EmbeddedPatternsIndex, toml::de::Error> {
    toml::from_str(text)
}

/// Serialise an `index.toml` manifest.
pub fn serialise_index(index: &EmbeddedPatternsIndex) -> Result<String, toml::ser::Error> {
    toml::to_string(index)
}

/// Build the canonical relative path `meta_dat/origin-patterns/<library>/<filename>`.
///
/// `library_id` is normalised: forward and backward slashes are stripped
/// because the canonical scheme is "one directory per library".
pub fn embedded_pattern_relpath(library_id: &str, filename: &str) -> std::path::PathBuf {
    let normalised_lib = library_id.replace(['/', '\\'], "_");
    let normalised_file = Path::new(filename)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| filename.to_string());
    Path::new("meta_dat").join(META_DAT_SUBDIR).join(normalised_lib).join(normalised_file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_file() {
        let parsed = parse_pattern_file("").unwrap();
        assert!(parsed.forwarder.is_empty());
        assert!(parsed.trivial_copy.is_empty());
        assert!(parsed.computational.is_empty());
    }

    #[test]
    fn roundtrip_minimal_forwarder() {
        let toml_text = r#"
            [[forwarder]]
            match = "$x.clone()"
            continuation = "$x"
            description = "Rust .clone forwards the receiver"
            language = "rust"
        "#;
        let parsed = parse_pattern_file(toml_text).unwrap();
        assert_eq!(parsed.forwarder.len(), 1);
        let rule = &parsed.forwarder[0];
        assert_eq!(rule.match_expr, "$x.clone()");
        assert_eq!(rule.continuation.as_deref(), Some("$x"));
        assert_eq!(rule.language.as_deref(), Some("rust"));
    }

    #[test]
    fn embedded_pattern_relpath_normalises_separators() {
        let p = embedded_pattern_relpath("requests-2.31.0", "origin-patterns.toml");
        assert!(
            p.ends_with("meta_dat/origin-patterns/requests-2.31.0/origin-patterns.toml")
                || p.ends_with("meta_dat\\origin-patterns\\requests-2.31.0\\origin-patterns.toml")
        );
    }

    #[test]
    fn index_roundtrips_through_toml() {
        let idx = EmbeddedPatternsIndex {
            libraries: vec![
                EmbeddedPatternEntry {
                    library_id: "faux_lib".to_string(),
                    filename: "origin-patterns.toml".to_string(),
                    source_path: Some("/tmp/faux_lib/.codetracer/origin-patterns.toml".to_string()),
                },
                EmbeddedPatternEntry {
                    library_id: "program".to_string(),
                    filename: "origin-patterns.toml".to_string(),
                    source_path: None,
                },
            ],
        };
        let serialised = serialise_index(&idx).unwrap();
        let reparsed = parse_index(&serialised).unwrap();
        assert_eq!(reparsed, idx);
    }
}
