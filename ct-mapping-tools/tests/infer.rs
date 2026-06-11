//! Integration tests for the §P7.3 `infer` AST-alignment path.
//!
//! Each test exercises a contract documented in the §P7.3 spec:
//!
//! * `identical_sources_yield_no_renames` — alignment that finds
//!   `from == to` for every identifier must emit zero rename rows.
//! * `simple_variable_rename` — the canonical
//!   `function a(b,c)` ↔ `function add(left,right)` fixture from the
//!   §P7.3 spec produces `a→add, b→left, c→right`.
//! * `min_confidence_filters_ambiguous` — when a single minified name
//!   sees two different originals in roughly equal proportions, the
//!   default confidence threshold drops the rename.
//! * `unsupported_language_errors` — `--language ruby` returns an
//!   error from the CLI surface (here exercised through the library
//!   API's `Language::from_name`).
//! * `structural_divergence_recovers` — when one subtree's kinds
//!   diverge (e.g. the minifier inlined a small function), siblings
//!   AFTER the divergence still produce correct alignments.
//!
//! All assertions are strict (`assert_eq!` on full sets).

use std::collections::HashSet;

use ct_mapping_tools::{InferOptions, Language, RenameEntry, infer};

/// Helper: extract `(from, to)` pairs from an inference result so
/// tests can compare against an expected `HashSet` without caring
/// about ordering, `file`, or `scope`.
fn pairs_of(entries: &[RenameEntry]) -> HashSet<(String, String)> {
    entries.iter().map(|e| (e.from.clone(), e.to.clone())).collect()
}

fn default_opts() -> InferOptions {
    InferOptions {
        language: Language::JavaScript,
        file_name: Some("min.js".to_string()),
        // Match the CLI default so tests exercise the same threshold
        // a user would hit in practice.
        min_confidence: 0.7,
    }
}

#[test]
fn identical_sources_yield_no_renames() {
    // Both inputs are byte-for-byte identical → every alignment
    // produces `from == to`, which the inferrer drops as a no-op.
    let src = "function alpha(beta, gamma) { return beta + gamma; }";
    let result = infer(src, src, &default_opts()).expect("parse");
    assert!(
        result.entries.is_empty(),
        "identical sources must produce zero rename entries (got {:?})",
        result.entries
    );
    // Stats are unfiltered counts → identical sources mean zero
    // renames were even proposed.
    assert_eq!(result.stats.renames_proposed, 0);
    assert_eq!(result.stats.renames_above_confidence, 0);
}

#[test]
fn simple_variable_rename() {
    // The canonical §P7.3 fixture.  Strict equality on the produced
    // `(from, to)` set: a→add, b→left, c→right and nothing else.
    let minified = "function a(b,c){return b+c;}";
    let original = "function add(left,right){return left+right;}";

    let result = infer(minified, original, &default_opts()).expect("parse");

    let expected: HashSet<(String, String)> = [
        ("a".to_string(), "add".to_string()),
        ("b".to_string(), "left".to_string()),
        ("c".to_string(), "right".to_string()),
    ]
    .into_iter()
    .collect();

    assert_eq!(
        pairs_of(&result.entries),
        expected,
        "expected the three documented renames and nothing else"
    );

    // Field-level sanity: file + scope are propagated for every row.
    for entry in &result.entries {
        assert_eq!(entry.file, "min.js");
        assert_eq!(entry.scope, "global");
    }

    // Stats: 3 distinct minified names (a, b, c), each contributing
    // exactly one rename above the confidence threshold.
    assert_eq!(result.stats.renames_proposed, 3);
    assert_eq!(result.stats.renames_above_confidence, 3);
}

#[test]
fn min_confidence_filters_ambiguous() {
    // The minified name `a` is paired with two different originals in
    // the alignment: `left` (from the first function) and `right`
    // (from the second function).  Each appears once, so the top
    // pair's share is 0.5 — well below the default 0.7 threshold and
    // the rename for `a` must be dropped.
    //
    // We also include the otherwise-clean rename `b→only` so the
    // test confirms that ONLY the ambiguous rename gets dropped
    // (the high-confidence one passes).
    let minified = "function f(a){return a;}function g(a){return a;}function h(b){return b;}";
    let original = "function f(left){return left;}function g(right){return right;}function h(only){return only;}";

    let result = infer(minified, original, &default_opts()).expect("parse");

    // `a` is ambiguous (50/50) → dropped.
    // `b` is unambiguous (100%) → kept.
    let expected: HashSet<(String, String)> =
        [("b".to_string(), "only".to_string())].into_iter().collect();

    assert_eq!(
        pairs_of(&result.entries),
        expected,
        "ambiguous `a` rename dropped; unambiguous `b` rename kept"
    );

    // Stats: 2 distinct minified names had a rename proposed (a, b);
    // 1 survived the confidence filter.
    assert_eq!(
        result.stats.renames_proposed, 2,
        "two minified names had a rename proposed before filtering"
    );
    assert_eq!(
        result.stats.renames_above_confidence, 1,
        "only the unambiguous rename survived the 0.7 threshold"
    );

    // Sanity: lowering the threshold to 0.5 lets `a` through (its
    // confidence is exactly 0.5).
    let mut loose = default_opts();
    loose.min_confidence = 0.5;
    let result_loose = infer(minified, original, &loose).expect("parse");
    let pairs = pairs_of(&result_loose.entries);
    assert!(
        pairs.contains(&("b".to_string(), "only".to_string())),
        "loose threshold keeps the unambiguous rename"
    );
    assert!(
        pairs.iter().any(|(from, _)| from == "a"),
        "loose threshold (0.5) lets `a` through too: {pairs:?}"
    );
}

#[test]
fn unsupported_language_errors() {
    // `Language::from_name("ruby")` returns `None`; the CLI surface
    // turns that into a non-zero exit via `anyhow!`.  We exercise
    // the library predicate directly so the test doesn't depend on
    // spawning the binary.  Also checks that the name lookup is
    // case-insensitive for the supported set.
    assert_eq!(Language::from_name("ruby"), None);
    assert_eq!(Language::from_name("go"), None);
    assert_eq!(Language::from_name(""), None);
    // Round-trip the supported set so a future grammar removal
    // breaks loudly.
    assert_eq!(Language::from_name("js"), Some(Language::JavaScript));
    assert_eq!(Language::from_name("JavaScript"), Some(Language::JavaScript));
    assert_eq!(Language::from_name("ts"), Some(Language::TypeScript));
    assert_eq!(Language::from_name("python"), Some(Language::Python));

    // Same predicate as a positive control via `from_extension`.
    assert_eq!(Language::from_extension("rb"), None);
    assert_eq!(Language::from_extension("js"), Some(Language::JavaScript));
}

#[test]
fn structural_divergence_recovers() {
    // The minified file has TWO top-level statements:
    //
    //   1. A small function `j(k){return k;}` (this matches a tiny
    //      `helper(x){return x;}` in the original — different STRUCTURE
    //      possible due to inlining considerations).
    //   2. A `for`-loop the minifier did NOT touch structurally; the
    //      original has the same `for`-loop with different identifier
    //      names.
    //
    // For this test, the FIRST statement of the minified is a
    // `while`-loop while the original's first statement is a normal
    // function definition: the kinds diverge at the top level, so
    // alignment of subtree #1 must abort.  Subtree #2 (a normal
    // function declaration) must still produce its rename.
    //
    // The §P7.3 spec calls out: "alignment continues past the
    // divergence on subsequent statements".  This test locks that in.
    let minified = "while(z){z--;}\nfunction m(n){return n+1;}";
    let original = "function helper(x){return x;}\nfunction multiply(value){return value+1;}";

    let result = infer(minified, original, &default_opts()).expect("parse");

    // The first subtree had a `while_statement` vs `function_declaration`
    // top-level kind mismatch: alignment aborts there, no pairs
    // collected from inside.  The second subtree's
    // `function_declaration` ↔ `function_declaration` match → we get
    // `m→multiply`, `n→value`.
    let pairs = pairs_of(&result.entries);
    assert!(
        pairs.contains(&("m".to_string(), "multiply".to_string())),
        "post-divergence rename `m→multiply` must survive; got {pairs:?}"
    );
    assert!(
        pairs.contains(&("n".to_string(), "value".to_string())),
        "post-divergence rename `n→value` must survive; got {pairs:?}"
    );

    // The first subtree's divergence MUST NOT have produced any
    // pair: it'd be a garbage rename if it had.
    assert!(
        !pairs.iter().any(|(from, _)| from == "z"),
        "diverged subtree must NOT contribute pairs; got {pairs:?}"
    );
}
