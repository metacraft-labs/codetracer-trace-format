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
//! §P7.1 + §P7.2 + §P7.3.  The exposed surface is:
//!
//! * [`FromSourcemapOptions`] — knobs for the `from-sourcemap` path.
//! * [`RenameEntry`] — one row in the produced TOML.
//! * [`from_sourcemap`] — walks the sourcemap and produces entries.
//! * [`to_toml`] — serialises entries to a TOML string accepted by
//!   the replay-server's `RenameList::parse_toml`.
//! * [`extract_minified_identifier_at`] — helper that pulls the
//!   identifier at a (line, column) from a generated source string;
//!   factored out so the CLI and tests share one implementation.
//! * [`InferOptions`], [`InferenceResult`], [`InferenceStats`],
//!   [`Language`], [`InferError`], [`infer`] — §P7.3 AST-alignment
//!   based inference of rename pairs from a (minified, original)
//!   source pair.
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

// ---------------------------------------------------------------------------
// §P7.3 — `infer` subcommand: AST-aligned rename inference.
//
// When a sourcemap is NOT available but the developer has BOTH the
// minified and the original source, we recover rename pairs by
// parsing both sources with tree-sitter and walking the two parse
// trees in structural lock-step.  Whenever a pair of nodes have the
// same kind and they are both `identifier`-shaped, we record the
// (minified_text, original_text) pair.  The most-frequent original
// for a given minified name wins (the same rule §P7.2 uses for
// sourcemap-derived renames), gated by a configurable confidence
// threshold so ambiguous pairs are dropped.
//
// The algorithm is intentionally simple — see the §P7.3 spec for
// the rationale (a more sophisticated tree-edit-distance pass is
// outside the time-box for v1).
// ---------------------------------------------------------------------------

/// Languages the [`infer`] entry point can parse.
///
/// Kept deliberately small: `JavaScript`, `TypeScript`, `Python`.
/// Adding a new language is mostly "pick a tree-sitter grammar + add
/// its identifier-kind names to [`identifier_kinds_for`]".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    /// Plain JavaScript (the `tree-sitter-javascript` grammar; also
    /// handles JSX).
    JavaScript,
    /// TypeScript (the `tree-sitter-typescript` grammar's TypeScript
    /// variant — NOT TSX, which would need a separate grammar).
    TypeScript,
    /// Python 3 (the `tree-sitter-python` grammar).
    Python,
}

impl Language {
    /// Map a file extension to the matching [`Language`].
    ///
    /// Used by the CLI's `--language auto` mode.  Returns `None`
    /// for unrecognised extensions; the caller surfaces that as a
    /// "couldn't auto-detect, pass --language" error so the failure
    /// mode is loud.
    pub fn from_extension(ext: &str) -> Option<Self> {
        // Lowercase the extension so `.JS` works the same as `.js`.
        match ext.to_ascii_lowercase().as_str() {
            "js" | "mjs" | "cjs" | "jsx" => Some(Language::JavaScript),
            "ts" | "tsx" => Some(Language::TypeScript),
            "py" => Some(Language::Python),
            _ => None,
        }
    }

    /// Map an explicit `--language <name>` string to a [`Language`].
    ///
    /// Returns `None` for unsupported language names — the CLI turns
    /// that into a non-zero exit with an explicit message rather than
    /// a panic.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "js" | "javascript" => Some(Language::JavaScript),
            "ts" | "typescript" => Some(Language::TypeScript),
            "py" | "python" => Some(Language::Python),
            _ => None,
        }
    }

    /// Tree-sitter language handle for this language.
    fn tree_sitter_language(self) -> tree_sitter::Language {
        match self {
            Language::JavaScript => tree_sitter_javascript::LANGUAGE.into(),
            Language::TypeScript => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            Language::Python => tree_sitter_python::LANGUAGE.into(),
        }
    }
}

/// Knobs for [`infer`].
#[derive(Debug, Clone)]
pub struct InferOptions {
    /// Source language for both inputs.  Both inputs MUST be in the
    /// same language — mixing JS and Python is a logic error not
    /// detected here.
    pub language: Language,
    /// `file = "..."` value written into every produced
    /// `[[rename]]` row.  Defaults to an empty string when `None`
    /// (the CLI passes the minified file's basename in practice).
    pub file_name: Option<String>,
    /// Minimum alignment confidence in the range `[0.0, 1.0]`.
    /// Renames whose top-pair / total-occurrences ratio falls below
    /// this threshold are dropped.  Default in the CLI: `0.7`.
    pub min_confidence: f64,
}

impl Default for InferOptions {
    fn default() -> Self {
        Self {
            language: Language::JavaScript,
            file_name: None,
            // Matches the §P7.3 CLI default; documented so the library
            // and CLI agree on the "safe" cutoff for ambiguous
            // renames.
            min_confidence: 0.7,
        }
    }
}

/// Outcome of an [`infer`] call.
///
/// `entries` has already been filtered by [`InferOptions::min_confidence`].
/// `stats` carries the unfiltered counts so callers can report what
/// the threshold dropped without re-running the alignment.
#[derive(Debug, Clone)]
pub struct InferenceResult {
    /// Surviving rename entries, sorted by `(scope, from)` like the
    /// `from-sourcemap` path's output.
    pub entries: Vec<RenameEntry>,
    /// Counts gathered during the alignment walk; useful for the CLI
    /// to print a `--verbose` summary, and for tests to assert which
    /// stage dropped a rename.
    pub stats: InferenceStats,
}

/// Counts collected during an [`infer`] alignment walk.
///
/// Intentionally a flat struct: every value is interesting on its
/// own, and the JSON-shaped accessor pattern (which adds nesting and
/// builders) buys nothing here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InferenceStats {
    /// Distinct minified identifier names observed during the walk.
    /// Equals "unique `from` candidates before confidence filtering".
    pub minified_identifiers_seen: usize,
    /// Distinct minified names with at least one paired original
    /// (i.e. `top_count > 0` AND `from != to`).  Equals the count of
    /// candidate renames before the confidence filter.
    pub renames_proposed: usize,
    /// Subset of `renames_proposed` that survived the confidence
    /// filter.  Equals `entries.len()` in [`InferenceResult`].
    pub renames_above_confidence: usize,
}

/// Errors emitted by [`infer`].  Kept narrow so the CLI can map
/// them to friendly non-zero exits.
#[derive(Debug)]
pub enum InferError {
    /// Tree-sitter rejected the language handle.  Should never
    /// happen for the languages we ship — the variant exists so
    /// future grammar version-skew is reported instead of panicking.
    ParserSetup(String),
    /// The grammar parsed but the resulting tree has an unrecoverable
    /// error at the root (e.g. the source isn't valid JS at all).
    /// Identifier-level errors inside the tree are tolerated — we
    /// only fail when the parser couldn't produce any tree.
    ParseFailed { which: &'static str },
}

impl std::fmt::Display for InferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InferError::ParserSetup(msg) => write!(f, "failed to load tree-sitter grammar: {msg}"),
            InferError::ParseFailed { which } => write!(f, "failed to parse {which} source"),
        }
    }
}

impl std::error::Error for InferError {}

/// Identifier-shaped tree-sitter node kinds per language.
///
/// We intentionally accept a small, audited list rather than treating
/// "any leaf node that looks word-shaped" as an identifier — this
/// avoids tagging string literals, regex flags, etc. as identifiers
/// and skewing the alignment.
fn identifier_kinds_for(lang: Language) -> &'static [&'static str] {
    match lang {
        Language::JavaScript | Language::TypeScript => &[
            // Bare identifiers (the bread-and-butter case: variables,
            // parameters, function names).
            "identifier",
            // Member access right-hand side (`obj.foo`).  Including
            // these lets us recover renames on method/property names.
            "property_identifier",
            // ES2022 private fields (`#x`).  Rare in minified output
            // but cheap to include.
            "private_property_identifier",
            // `{foo}` shorthand in object literals + destructuring.
            "shorthand_property_identifier",
            "shorthand_property_identifier_pattern",
            // TypeScript `type X = ...`-introduced names.  Same kind
            // name as the JS grammar.
            "type_identifier",
        ],
        Language::Python => &[
            // Python's grammar uses a single `identifier` kind for
            // every name (variables, params, attribute access, etc.).
            "identifier",
        ],
    }
}

/// Run the §P7.3 AST-alignment inference.
///
/// Strategy (see the §P7.3 spec for the why):
///
/// 1. Parse both inputs with tree-sitter into `Tree`s.
/// 2. Walk the two root nodes in structural lock-step (a recursive
///    pairwise pre-order traversal).  At each step:
///    * If the two nodes have different kinds, abort the alignment
///      for this subtree (the minifier did something we can't model
///      generically; continuing would emit garbage pairs).
///    * If the two nodes share an identifier-shaped kind, record
///      `(minified_text, original_text)` in the tally.
///    * Otherwise, recurse on each pair of matching children.  When
///      children counts differ, abort this subtree.
/// 3. Per minified name, pick the most-frequent original.  Confidence
///    = `top_count / total_count_for_min_text`.
/// 4. Drop pairs whose `from == to` (no rename) or whose confidence
///    is below `opts.min_confidence`.
pub fn infer(
    minified_src: &str,
    original_src: &str,
    opts: &InferOptions,
) -> Result<InferenceResult, InferError> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&opts.language.tree_sitter_language())
        .map_err(|e| InferError::ParserSetup(e.to_string()))?;

    let min_tree = parser
        .parse(minified_src, None)
        .ok_or(InferError::ParseFailed { which: "minified" })?;
    let orig_tree = parser
        .parse(original_src, None)
        .ok_or(InferError::ParseFailed { which: "original" })?;

    let id_kinds = identifier_kinds_for(opts.language);

    // (min_text, orig_text) -> count.  Tally first, decide later.
    let mut tally: HashMap<(String, String), u32> = HashMap::new();
    align_nodes(
        min_tree.root_node(),
        orig_tree.root_node(),
        minified_src.as_bytes(),
        original_src.as_bytes(),
        id_kinds,
        &mut tally,
    );

    // Aggregate per minified name.
    //
    // For each minified name we need:
    //   * the per-original counts (to pick the winner + its share)
    //   * the total count (denominator for confidence)
    let mut per_min: HashMap<String, HashMap<String, u32>> = HashMap::new();
    for ((min_text, orig_text), count) in tally {
        *per_min
            .entry(min_text)
            .or_default()
            .entry(orig_text)
            .or_insert(0) += count;
    }

    let minified_identifiers_seen = per_min.len();
    let mut entries: Vec<(RenameEntry, f64)> = Vec::new();
    let mut renames_proposed = 0usize;

    let file_name = opts.file_name.clone().unwrap_or_default();

    for (min_text, candidates) in per_min {
        let total: u32 = candidates.values().copied().sum();
        if total == 0 {
            continue;
        }
        // Pick winner.  Tie-break: higher count, then lex-smaller
        // original (deterministic; matches `from_sourcemap`).
        let (winner_orig, winner_count) = candidates
            .iter()
            .max_by(|(a_name, a_count), (b_name, b_count)| {
                a_count.cmp(b_count).then_with(|| b_name.cmp(a_name))
            })
            .map(|(n, c)| (n.clone(), *c))
            .expect("non-empty candidates");

        // Drop no-op renames.  Counting `from == to` toward the
        // "proposed" stat would be misleading: the alignment found
        // the name unchanged across the pair, which is the boring
        // common case (function names, top-level declarations a
        // minifier often leaves alone) and not a real proposal.
        if winner_orig == min_text {
            continue;
        }

        renames_proposed += 1;
        let confidence = winner_count as f64 / total as f64;
        if confidence < opts.min_confidence {
            continue;
        }
        entries.push((
            RenameEntry {
                file: file_name.clone(),
                scope: "global".to_string(),
                from: min_text,
                to: winner_orig,
            },
            confidence,
        ));
    }

    entries.sort_by(|a, b| (&a.0.scope, &a.0.from).cmp(&(&b.0.scope, &b.0.from)));
    let renames_above_confidence = entries.len();

    Ok(InferenceResult {
        entries: entries.into_iter().map(|(e, _)| e).collect(),
        stats: InferenceStats {
            minified_identifiers_seen,
            renames_proposed,
            renames_above_confidence,
        },
    })
}

/// Recursive pairwise pre-order walk.
///
/// Invariant on entry: `min_node.kind() == orig_node.kind()`.  The
/// caller ensures this; the recursion only descends through matching
/// kinds and aborts otherwise.
fn align_nodes(
    min_node: tree_sitter::Node<'_>,
    orig_node: tree_sitter::Node<'_>,
    min_src: &[u8],
    orig_src: &[u8],
    id_kinds: &[&str],
    tally: &mut HashMap<(String, String), u32>,
) {
    // Kind mismatch ⇒ abort this subtree.  Continuing past a mismatch
    // produces garbage pairs in practice (a minifier that turned a
    // for-loop into a while-loop will mis-pair the loop body's
    // identifiers).
    if min_node.kind() != orig_node.kind() {
        return;
    }
    // Identifier-shaped leaf ⇒ record and stop (identifiers don't
    // have meaningful children for our purposes).
    if id_kinds.contains(&min_node.kind()) {
        let min_text = node_text(min_node, min_src);
        let orig_text = node_text(orig_node, orig_src);
        if let (Some(mt), Some(ot)) = (min_text, orig_text)
            && !mt.is_empty()
            && !ot.is_empty()
        {
            *tally.entry((mt, ot)).or_insert(0) += 1;
        }
        return;
    }
    // Walk named children pairwise.  We use named (not anonymous /
    // syntactic-only) children to skip punctuation tokens: this makes
    // the alignment robust against the minifier's whitespace +
    // semicolon noise.
    //
    // When child counts differ we still walk the common prefix —
    // this is the §P7.3 "structural divergence recovers" contract:
    // siblings AFTER the divergence still get a fair alignment.  The
    // mismatched child's kind check at the top of the recursive call
    // protects us from emitting garbage pairs when the i-th child
    // doesn't match.
    let min_child_count = min_node.named_child_count();
    let orig_child_count = orig_node.named_child_count();
    let common = min_child_count.min(orig_child_count);
    let mut min_cursor = min_node.walk();
    let mut orig_cursor = orig_node.walk();
    let min_children: Vec<_> = min_node.named_children(&mut min_cursor).collect();
    let orig_children: Vec<_> = orig_node.named_children(&mut orig_cursor).collect();
    for i in 0..common {
        align_nodes(
            min_children[i],
            orig_children[i],
            min_src,
            orig_src,
            id_kinds,
            tally,
        );
    }
}

/// UTF-8 slice of `node`'s byte range from `src`.
///
/// Returns `None` if the slice isn't valid UTF-8 — should never
/// happen for inputs we accept (we require `&str` in the public API),
/// but the conversion is fallible by type and we'd rather skip the
/// pair than panic.
fn node_text(node: tree_sitter::Node<'_>, src: &[u8]) -> Option<String> {
    let range = node.byte_range();
    let bytes = src.get(range)?;
    std::str::from_utf8(bytes).ok().map(|s| s.to_string())
}

// ---------------------------------------------------------------------------
// §P7.4 — `infer-llm` subcommand: LLM-proposed renames for minified-only
// sources.
//
// When neither a sourcemap nor an original source is available the only
// signal left is the structure + naming patterns the LLM recognises in
// the minified source itself.  We POST the source to the Anthropic
// Messages API, ask the model for a JSON list of `{from, to,
// confidence}` triples, parse the response, and emit it through the
// same TOML schema the other paths use.
//
// Design notes:
//
// * The library API (`infer_llm`) accepts the `api_key` as a parameter
//   rather than reading the environment — this lets tests inject a
//   throwaway key against a mock server and keeps the secret off the
//   library's surface.  The CLI shim handles env-var fallback +
//   skip-loud behavior.
//
// * `--min-confidence` defaults to **0.5** for `infer-llm` (vs **0.7**
//   for `infer`) because LLM proposals are inherently best-effort: the
//   model's confidence calibration is noisier than the AST aligner's
//   per-bind statistical share.
//
// * The prompt asks the model to embed its proposals in a JSON code
//   fence (`<backticks>json ... <backticks>`).  We tolerate either
//   "the whole response is JSON" or "JSON inside a code fence" so the
//   model has some latitude in its formatting.
// ---------------------------------------------------------------------------

/// Knobs for [`infer_llm`].
#[derive(Debug, Clone)]
pub struct InferLlmOptions {
    /// Source language used for the prompt's syntax hint (the model
    /// adapts its proposals when told "this is Python" vs "this is
    /// JavaScript").
    pub language: Language,
    /// `file = "..."` value written into every produced
    /// `[[rename]]` row.  Empty string when `None`.
    pub file_name: Option<String>,
    /// Anthropic model ID — e.g. `claude-haiku-4-5-20251001`
    /// (default).  Haiku is the cheapest + fastest production model
    /// and the §P7.4 spec calls it out explicitly for cost reasons.
    pub model: String,
    /// API base URL — defaults to the Anthropic public endpoint.
    /// Override in tests to point at a mock server.
    pub api_base: String,
    /// Drop proposed renames whose self-rated confidence falls below
    /// this threshold.  Default in the CLI: 0.5.
    pub min_confidence: f64,
    /// Cap the number of proposals embedded in the request so the
    /// prompt stays small.  Default: 50.
    pub max_bindings: usize,
}

impl Default for InferLlmOptions {
    fn default() -> Self {
        Self {
            language: Language::JavaScript,
            file_name: None,
            model: "claude-haiku-4-5-20251001".to_string(),
            api_base: "https://api.anthropic.com/v1".to_string(),
            min_confidence: 0.5,
            max_bindings: 50,
        }
    }
}

/// One rename row with the model's self-rated confidence attached.
///
/// The CLI strips the confidence before serialising to the TOML
/// schema (which has no confidence column today); the library exposes
/// it so callers wanting a review UI can surface "the model says
/// 0.83" alongside each row.
#[derive(Debug, Clone, PartialEq)]
pub struct RenameEntryWithConfidence {
    pub entry: RenameEntry,
    /// `0.0..=1.0` — the model's self-rated confidence.  Already
    /// clamped during parsing; downstream code can trust the range.
    pub confidence: f64,
}

/// Outcome of an [`infer_llm`] call.
#[derive(Debug, Clone)]
pub struct InferLlmResult {
    /// Surviving rows (already filtered by
    /// [`InferLlmOptions::min_confidence`]), sorted by
    /// `(scope, from)` like the other paths' output.
    pub entries: Vec<RenameEntryWithConfidence>,
    /// Usage statistics — useful for a `--verbose` CLI summary and
    /// for callers tracking spend.
    pub stats: InferLlmStats,
}

/// Usage statistics for a single [`infer_llm`] call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InferLlmStats {
    /// How many HTTP requests the implementation made.  Always `1`
    /// in v1 (no chunking yet); future revisions may split a large
    /// minified file across multiple requests.
    pub api_call_count: usize,
    /// `input_tokens` reported by the Anthropic API.  `0` when the
    /// response didn't include usage metadata.
    pub total_tokens_in: usize,
    /// `output_tokens` reported by the Anthropic API.  `0` when the
    /// response didn't include usage metadata.
    pub total_tokens_out: usize,
    /// Number of rename triples the model proposed (before the
    /// confidence filter).
    pub bindings_proposed: usize,
    /// Subset of `bindings_proposed` that survived the threshold.
    /// Equals `entries.len()`.
    pub bindings_above_confidence: usize,
}

/// Errors emitted by [`infer_llm`].  The CLI maps each variant to a
/// distinct exit code + user-facing message.
#[derive(Debug)]
pub enum InferLlmError {
    /// `api_key` was empty.  The CLI translates this into the
    /// skip-loud message and exit code 0 — the §P7.4 spec calls out
    /// the "no key, no harm" contract for test environments.
    NoApiKey,
    /// `reqwest` returned a transport error OR the server responded
    /// with a non-2xx status.  The payload includes the status code
    /// + body excerpt for diagnostics.
    HttpError(String),
    /// The response body wasn't valid JSON, or the embedded JSON
    /// proposals block couldn't be parsed.
    JsonParseError(String),
    /// The response was valid JSON but didn't have the shape we
    /// expect (missing `content[0].text`, etc.).
    ResponseShapeError(String),
}

impl std::fmt::Display for InferLlmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InferLlmError::NoApiKey => write!(f, "no API key configured"),
            InferLlmError::HttpError(msg) => write!(f, "HTTP error: {msg}"),
            InferLlmError::JsonParseError(msg) => write!(f, "JSON parse error: {msg}"),
            InferLlmError::ResponseShapeError(msg) => write!(f, "response shape error: {msg}"),
        }
    }
}

impl std::error::Error for InferLlmError {}

/// Human-readable name for the language — used in the prompt and the
/// fenced code block's language tag.
fn language_prompt_name(lang: Language) -> &'static str {
    match lang {
        Language::JavaScript => "javascript",
        Language::TypeScript => "typescript",
        Language::Python => "python",
    }
}

/// System prompt for the LLM.  Kept terse: the model only needs to
/// know the task shape + output format.  Verbose system prompts cost
/// tokens on every call without measurably improving output quality
/// for this kind of structured-extraction task.
const SYSTEM_PROMPT: &str = "You are a code-analysis assistant. Given a minified source, propose meaningful names for the minified identifiers based on their usage patterns. For each rename, output a confidence score 0.0-1.0 reflecting how sure you are. Only include renames where you have meaningful evidence. Respond with a single JSON code block; no prose outside it.";

/// Build the user prompt embedded in the request.
///
/// The shape is documented in the §P7.4 spec — kept here so the
/// prompt text is right next to its parser.
fn build_user_prompt(minified_src: &str, lang: Language, max_bindings: usize) -> String {
    let lang_name = language_prompt_name(lang);
    format!(
        "Here is a minified {lang_name} source:\n\n\
         ```{lang_name}\n\
         {minified_src}\n\
         ```\n\n\
         Propose renames in JSON format:\n\n\
         ```json\n\
         {{\n  \"renames\": [\n    {{\"from\": \"a\", \"to\": \"userId\", \"confidence\": 0.85, \"reasoning\": \"passed to authenticate(), looks like an ID\"}}\n  ]\n}}\n\
         ```\n\n\
         Only include renames where you have meaningful evidence. Cap at {max_bindings} entries."
    )
}

/// Extract the JSON proposals block from the assistant's text
/// content.
///
/// Strategy:
///
/// 1. Look for a fenced code block marked ```json — that's what the
///    prompt asks for.
/// 2. If no fence is found, try parsing the whole text as JSON (some
///    well-behaved models return raw JSON without a fence).
/// 3. If neither parses, return [`InferLlmError::JsonParseError`].
fn extract_proposals_json(text: &str) -> Result<serde_json::Value, InferLlmError> {
    // Look for ```json ... ``` fence first.
    if let Some(start) = text.find("```json") {
        // After the fence opener, find the matching closer.  We accept
        // either `\n` or end-of-fence-tag immediately after `json`.
        let after_tag = start + "```json".len();
        let rest = &text[after_tag..];
        if let Some(end_rel) = rest.find("```") {
            let inner = rest[..end_rel].trim();
            return serde_json::from_str(inner).map_err(|e| {
                InferLlmError::JsonParseError(format!(
                    "could not parse fenced JSON proposals block: {e}"
                ))
            });
        }
    }
    // Fall back to whole-text parse.  We trim leading whitespace; the
    // model sometimes wraps the JSON in a single blank line.
    serde_json::from_str(text.trim()).map_err(|e| {
        InferLlmError::JsonParseError(format!(
            "no fenced JSON block and whole-text parse failed: {e}"
        ))
    })
}

/// Run the §P7.4 LLM-based inference.
///
/// Wire shape:
///
/// 1. Build the prompt embedding the minified source.
/// 2. POST to `<api_base>/messages` with the Anthropic Messages
///    headers (`x-api-key`, `anthropic-version`, `content-type`).
/// 3. Parse the response, extract `content[0].text`, pull the JSON
///    proposals block out of it.
/// 4. Convert each proposal to a [`RenameEntryWithConfidence`],
///    dropping `from == to`, dropping below-threshold rows, sorting
///    deterministically.
///
/// The function takes `api_key` as a `&str` parameter (NOT through
/// env) so tests can mock without leaking real credentials and so the
/// library has no implicit dependency on process state.
pub fn infer_llm(
    minified_src: &str,
    api_key: &str,
    opts: &InferLlmOptions,
) -> Result<InferLlmResult, InferLlmError> {
    if api_key.is_empty() {
        return Err(InferLlmError::NoApiKey);
    }

    let user_prompt = build_user_prompt(minified_src, opts.language, opts.max_bindings);
    let body = serde_json::json!({
        "model": opts.model,
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": user_prompt}
        ]
    });

    let url = format!("{}/messages", opts.api_base.trim_end_matches('/'));
    let client = reqwest::blocking::Client::builder()
        // The blocking client without an explicit timeout will wait
        // forever on a hung connection.  30s is generous for the
        // Messages API (Haiku typically responds in 2-4 seconds) and
        // matches the timeout other CLIs in this workspace use.
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| InferLlmError::HttpError(format!("client setup: {e}")))?;

    let response = client
        .post(&url)
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .map_err(|e| InferLlmError::HttpError(format!("send: {e}")))?;

    let status = response.status();
    let response_text = response
        .text()
        .map_err(|e| InferLlmError::HttpError(format!("read body: {e}")))?;

    if !status.is_success() {
        // Truncate the body excerpt so an HTML error page doesn't
        // blow up the user's terminal.
        let excerpt = if response_text.len() > 500 {
            format!("{}…", &response_text[..500])
        } else {
            response_text.clone()
        };
        return Err(InferLlmError::HttpError(format!(
            "status {status}: {excerpt}"
        )));
    }

    let parsed: serde_json::Value = serde_json::from_str(&response_text)
        .map_err(|e| InferLlmError::JsonParseError(format!("response body: {e}")))?;

    // Anthropic Messages API: response shape is
    //   { "content": [ { "type": "text", "text": "..." } ], "usage": { ... } }
    // We grab `content[0].text` (the first text block) and the usage
    // counts.
    let text = parsed
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("text"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| {
            InferLlmError::ResponseShapeError(
                "expected `content[0].text` in Messages API response".to_string(),
            )
        })?;

    let usage = parsed.get("usage");
    let tokens_in = usage
        .and_then(|u| u.get("input_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let tokens_out = usage
        .and_then(|u| u.get("output_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    let proposals = extract_proposals_json(text)?;
    let renames_array = proposals
        .get("renames")
        .and_then(|r| r.as_array())
        .ok_or_else(|| {
            InferLlmError::ResponseShapeError(
                "proposals JSON missing `renames` array".to_string(),
            )
        })?;

    let file_name = opts.file_name.clone().unwrap_or_default();
    let mut entries: Vec<RenameEntryWithConfidence> = Vec::new();
    let mut bindings_proposed = 0usize;

    for item in renames_array {
        let from = item.get("from").and_then(|v| v.as_str());
        let to = item.get("to").and_then(|v| v.as_str());
        let confidence = item
            .get("confidence")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let (Some(from), Some(to)) = (from, to) else {
            // Skip malformed proposal rows but don't fail the whole
            // call — the model occasionally elides a field and we'd
            // rather use the well-formed ones than punt.
            continue;
        };
        if from.is_empty() || to.is_empty() {
            continue;
        }
        bindings_proposed += 1;
        if from == to {
            // No-op rename; drop silently.
            continue;
        }
        // Clamp confidence into [0.0, 1.0] so the model can't smuggle
        // a 1.5 past the filter via prompt injection or honest error.
        let confidence = confidence.clamp(0.0, 1.0);
        if confidence < opts.min_confidence {
            continue;
        }
        entries.push(RenameEntryWithConfidence {
            entry: RenameEntry {
                file: file_name.clone(),
                scope: "global".to_string(),
                from: from.to_string(),
                to: to.to_string(),
            },
            confidence,
        });
    }

    entries.sort_by(|a, b| (&a.entry.scope, &a.entry.from).cmp(&(&b.entry.scope, &b.entry.from)));
    let bindings_above_confidence = entries.len();

    Ok(InferLlmResult {
        entries,
        stats: InferLlmStats {
            api_call_count: 1,
            total_tokens_in: tokens_in,
            total_tokens_out: tokens_out,
            bindings_proposed,
            bindings_above_confidence,
        },
    })
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

    #[test]
    fn infer_llm_no_api_key_returns_typed_error() {
        // Empty api_key short-circuits before any HTTP setup so this
        // unit test doesn't need a mock server.
        let err = infer_llm("function a(){}", "", &InferLlmOptions::default()).unwrap_err();
        match err {
            InferLlmError::NoApiKey => {}
            other => panic!("expected NoApiKey, got {other:?}"),
        }
    }

    #[test]
    fn extract_proposals_json_handles_fenced_block() {
        let text = "Here are my proposals:\n\n```json\n{\"renames\": [{\"from\": \"a\", \"to\": \"alpha\", \"confidence\": 0.9}]}\n```\n";
        let v = extract_proposals_json(text).expect("fenced parse");
        assert_eq!(
            v["renames"][0]["from"].as_str(),
            Some("a"),
            "fenced block parsed correctly"
        );
    }

    #[test]
    fn extract_proposals_json_handles_raw_json() {
        let text = "{\"renames\": [{\"from\": \"b\", \"to\": \"beta\", \"confidence\": 0.7}]}";
        let v = extract_proposals_json(text).expect("raw parse");
        assert_eq!(v["renames"][0]["from"].as_str(), Some("b"));
    }

    #[test]
    fn extract_proposals_json_rejects_garbage() {
        let text = "Sorry, I can't propose renames for this source.";
        assert!(matches!(
            extract_proposals_json(text),
            Err(InferLlmError::JsonParseError(_))
        ));
    }
}
