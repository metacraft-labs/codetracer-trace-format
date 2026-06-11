//! Library half of `ct-mapping-tools` — produces TOML rename lists
//! from various inputs (sourcemap V3 today; AST aligners and LLMs in
//! later milestones).
//!
//! Spec: `codetracer-specs/Planned-Features/Column-Aware-Tracing-And-Deminification.milestones.org` §P7.
//!
//! The library is intentionally kept thin so the CLI binary and any
//! future integrations (e.g. the replay-server self-serving renames
//! from a bundled sourcemap) can share the same conversion code.
//!
//! ## What this milestone covers
//!
//! §P7.1 + §P7.2 only: a new workspace crate exposing the
//! `from-sourcemap` subcommand.  The exposed surface is:
//!
//! * [`FromSourcemapOptions`] — knobs for the conversion.
//! * [`RenameEntry`] — one row in the produced TOML.
//! * [`from_sourcemap`] — walks the sourcemap and produces entries.
//! * [`to_toml`] — serialises entries to a TOML string accepted by
//!   the replay-server's `RenameList::parse_toml`.
//! * [`extract_minified_identifier_at`] — helper that pulls the
//!   identifier at a (line, column) from a generated source string;
//!   factored out so the CLI and tests share one implementation.
//!
//! ## Conversion strategy
//!
//! A Source Map V3 alone doesn't carry the *minified* name — only the
//! original.  We recover the minified name by reading the generated
//! source at the segment's generated position and snipping the
//! identifier token there.  When multiple segments resolve the same
//! `(minified_name, original_name)` pair the count is tallied; the
//! most-frequent original wins per minified name.  This matches the
//! intuition that minifiers reuse short identifiers across many
//! distinct original bindings, but for a *single* minified name the
//! winning original is the one it most often stood in for.

use std::collections::HashMap;

use serde::Serialize;
use sourcemap_translate::SourcemapIndex;

/// Knobs for [`from_sourcemap`].
///
/// Construction is intentionally a plain struct (no builder) — there
/// are only a handful of options and the CLI fills them all in one
/// shot.
#[derive(Debug, Clone, Default)]
pub struct FromSourcemapOptions {
    /// The `file = "..."` value written into every produced
    /// `[[rename]]` entry.  When `None`, the conversion will fall
    /// back to the sourcemap's V3 `file` field (typically the
    /// generated bundle name, e.g. `lodash.min.js`).
    pub file_name: Option<String>,
    /// When set, group entries by their enclosing function (where the
    /// sourcemap segment data allows that derivation) and emit
    /// `scope = "function:<name>"` instead of the default `global`.
    ///
    /// Note: V3 sourcemaps don't carry "enclosing function" info per
    /// segment — the §P7.2 implementation falls back to `global` when
    /// no per-segment function name can be derived.  The flag is
    /// surfaced anyway so the CLI shape is forward-compatible with
    /// scope-extension RFCs.
    pub per_function: bool,
    /// Contents of the minified / generated source file the sourcemap
    /// covers.  When provided, the conversion extracts the identifier
    /// at each segment's generated position to recover the minified
    /// name.  When `None`, the conversion uses the original name as
    /// both `from` and `to` (a useless no-op rename — surfaced so the
    /// caller can detect "you forgot --minified").
    pub minified_source: Option<String>,
}

/// One row of the produced TOML rename list.
///
/// Field semantics match the §P5 schema documented in
/// `codetracer/src/db-backend/src/rename_list.rs`.  Made `pub` so
/// downstream tools (the CLI; tests; future programmatic consumers)
/// can inspect entries before they hit the TOML writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RenameEntry {
    /// File the rename applies to (matches the replay-server's
    /// recorded `file` field for the minified bundle).
    pub file: String,
    /// `global`, `function:<name>`, or `block:L<line>`.  The §P5
    /// parser canonicalises these strings; `from_sourcemap` always
    /// emits one of these three forms.
    pub scope: String,
    /// Minified identifier name as it appears in the generated source.
    pub from: String,
    /// Original identifier name as recorded in the sourcemap's
    /// `names[]` array.
    pub to: String,
}

/// JS identifier-character predicate — `[A-Za-z_$0-9]`.
///
/// Sticks to the JS charset for v1; Python (no `$`) is close enough
/// that the same scanner produces the right answer for valid
/// identifiers, and the §P7 spec calls out adding a `--language`
/// switch as a follow-up.
fn is_js_identifier_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '$'
}

/// First-character version of [`is_js_identifier_continue`] — same
/// set minus the digits.  An identifier can't START with a digit.
fn is_js_identifier_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == '$'
}

/// Pull the identifier-shaped token starting at `(line, column)`
/// (1-indexed) from `source`.
///
/// Returns `None` when:
/// * The line doesn't exist (sourcemap points past EOF — rare but
///   possible with truncated bundles).
/// * The column points past the line's end.
/// * The character at the position isn't a valid identifier-start
///   character.
///
/// Column is treated as a *byte* offset into the line; the JS
/// identifier subset we accept is all ASCII so byte / char offsets
/// coincide.  Non-ASCII identifier characters (Unicode) would need a
/// proper grapheme walker; out of scope for §P7.2.
pub fn extract_minified_identifier_at(source: &str, line: u32, column: u32) -> Option<String> {
    if line == 0 || column == 0 {
        return None;
    }
    // Source Map V3 line numbers we converted to 1-indexed; lines
    // here are 1-indexed so subtract one.
    let line_idx = (line - 1) as usize;
    let col_idx = (column - 1) as usize;
    let line_str = source.lines().nth(line_idx)?;
    if col_idx >= line_str.len() {
        return None;
    }
    let slice = &line_str[col_idx..];
    let first = slice.chars().next()?;
    if !is_js_identifier_start(first) {
        return None;
    }
    let end = slice
        .char_indices()
        .find(|(_, c)| !is_js_identifier_continue(*c))
        .map(|(i, _)| i)
        .unwrap_or(slice.len());
    Some(slice[..end].to_string())
}

/// Walk the sourcemap and produce TOML-shaped rename entries.
///
/// Algorithm:
///
/// 1. Iterate every segment that carries both a generated position
///    AND an original name from the sourcemap's `names[]` table.
/// 2. If a generated source is supplied via
///    [`FromSourcemapOptions::minified_source`], extract the
///    identifier at the segment's generated position — that's the
///    minified name.  When no source is supplied the segment is
///    skipped (we'd otherwise produce useless `from = to` entries).
/// 3. Tally `(minified_name, original_name) -> count`.
/// 4. For each minified name pick the most-frequent original
///    (ties broken by lexicographic order on the original name so the
///    output is deterministic).
/// 5. Emit one `[[rename]]` row per surviving pair, sorted by
///    `(scope, from)` for stable byte-for-byte output.
///
/// The returned vector is empty when the sourcemap has no named
/// segments or no minified source is available — that's the well-
/// defined no-rename-list case.
pub fn from_sourcemap(map: &SourcemapIndex, opts: &FromSourcemapOptions) -> Vec<RenameEntry> {
    // Resolve the file name written into every entry.  Priority:
    // explicit `--file-name` > sourcemap's V3 `file` field.  Falls
    // back to an empty string only when neither is available — the
    // CLI surface treats that as an error, but the library stays
    // permissive so tests can exercise edge cases.
    let file_name = opts
        .file_name
        .clone()
        .or_else(|| map.file().map(|s| s.to_string()))
        .unwrap_or_default();

    // (minified_name, original_name) -> count.  HashMap is fine here:
    // production sourcemaps top out around 10k unique names, well
    // within HashMap's wheelhouse.
    let mut tally: HashMap<(String, String), u32> = HashMap::new();

    for seg in map.segments() {
        let original = match seg.name {
            Some(n) => n,
            None => continue,
        };
        let minified = if let Some(source) = opts.minified_source.as_deref() {
            match extract_minified_identifier_at(source, seg.gen_line, seg.gen_column) {
                Some(id) => id,
                None => continue,
            }
        } else {
            // No way to recover the minified name → skip.  The CLI
            // wraps this in an explicit error before calling in to
            // avoid silent no-ops; the library tolerates the case so
            // unit tests can exercise the empty-output path.
            continue;
        };
        *tally.entry((minified, original)).or_insert(0) += 1;
    }

    // Pick the most-frequent original per minified name.  Ties
    // broken by:
    //   1. Higher count wins.
    //   2. Lexicographically smaller original wins (deterministic
    //      stable choice; arbitrary but reproducible).
    let mut per_minified: HashMap<String, (String, u32)> = HashMap::new();
    for ((minified, original), count) in tally {
        let entry = per_minified.entry(minified).or_insert_with(|| (String::new(), 0));
        let count_replaces = count > entry.1 || (count == entry.1 && original < entry.0);
        if count_replaces {
            *entry = (original, count);
        }
    }

    // Emit one entry per (minified, original) winner.  Sort by
    // `(scope, from)` for deterministic output regardless of HashMap
    // iteration order — important for tests and for caching the
    // produced TOML by content hash.
    let scope = if opts.per_function {
        // §P7.2 spec calls out that V3 sourcemaps don't usually carry
        // per-segment enclosing-function info; we surface the flag
        // but fall back to global so the conversion still produces
        // useful output.  The reviewer's open follow-up is to add a
        // proper function-scope derivation pass.
        "global"
    } else {
        "global"
    };

    let mut entries: Vec<RenameEntry> = per_minified
        .into_iter()
        .map(|(minified, (original, _count))| RenameEntry {
            file: file_name.clone(),
            scope: scope.to_string(),
            from: minified,
            to: original,
        })
        .collect();
    entries.sort_by(|a, b| (&a.scope, &a.from).cmp(&(&b.scope, &b.from)));
    entries
}

/// Serialise a slice of [`RenameEntry`] into the TOML format the
/// replay-server's `RenameList::parse_toml` consumes.
///
/// Output shape:
///
/// ```toml
/// [[rename]]
/// file = "lodash.min.js"
/// scope = "global"
/// from = "e"
/// to = "array"
/// ```
///
/// The function uses `toml::to_string` over a wrapping struct so the
/// output goes through the canonical TOML serialiser.  An empty
/// `entries` slice produces an empty string (the schema accepts
/// "no rename entries" as a valid document).
pub fn to_toml(entries: &[RenameEntry]) -> String {
    #[derive(Serialize)]
    struct Doc<'a> {
        // `serde(rename = "rename")` makes `toml` emit the array as
        // `[[rename]]` — matching the §P5 schema's `RawEntry` shape.
        #[serde(rename = "rename", skip_serializing_if = "<[RenameEntry]>::is_empty")]
        entries: &'a [RenameEntry],
    }
    let doc = Doc { entries };
    toml::to_string(&doc).expect("RenameEntry shape is statically serialisable to TOML")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_identifier_at_basic_position() {
        let src = "function alpha(){return beta();}";
        // 1-indexed col 10 → "alpha"
        assert_eq!(extract_minified_identifier_at(src, 1, 10).as_deref(), Some("alpha"));
        // 1-indexed col 25 → "beta"
        assert_eq!(extract_minified_identifier_at(src, 1, 25).as_deref(), Some("beta"));
    }

    #[test]
    fn extract_identifier_at_non_identifier_returns_none() {
        let src = "function alpha(){}";
        // Column points at `(` — not an identifier-start.
        assert_eq!(extract_minified_identifier_at(src, 1, 15), None);
    }

    #[test]
    fn extract_identifier_at_past_eof_returns_none() {
        let src = "ab";
        assert_eq!(extract_minified_identifier_at(src, 2, 1), None);
        assert_eq!(extract_minified_identifier_at(src, 1, 99), None);
    }

    #[test]
    fn to_toml_round_trips_through_serde() {
        let entries = vec![RenameEntry {
            file: "lodash.min.js".to_string(),
            scope: "global".to_string(),
            from: "e".to_string(),
            to: "array".to_string(),
        }];
        let out = to_toml(&entries);
        assert!(out.contains("[[rename]]"));
        assert!(out.contains("file = \"lodash.min.js\""));
        assert!(out.contains("from = \"e\""));
        assert!(out.contains("to = \"array\""));
    }

    #[test]
    fn to_toml_empty_produces_empty_string() {
        assert_eq!(to_toml(&[]), "");
    }
}
