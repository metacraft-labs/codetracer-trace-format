//! A census, not a comment: no stream-writing path may call the streaming Zstd
//! API.
//!
//! # Why a repo-wide scan rather than a per-site review
//!
//! `zstd::encode_all` produces a frame whose header does not pledge the
//! decompressed size. Every reader in the canonical Nim implementation sizes its
//! destination buffer from `ZSTD_getFrameContentSize` and treats
//! `ZSTD_CONTENTSIZE_UNKNOWN` as failure — and for three of the five stream
//! families that failure surfaces as **zero records**, not as an error. So a
//! single `encode_all` in a writer makes a whole stream read back as empty.
//!
//! The site count is what makes this a census. The defect was found on
//! `steps.dat`, fixed there, and left in place on `values.dat`, `calls.dat`,
//! `events.dat` and `codetracer_ctfs::chunked` (`events.log`) — four more
//! instances of the same one-line form, in the same workspace, all of which
//! reported green. Fixing an instance and moving on is exactly how the other
//! four survived, so the form is banned by a check that can fail rather than by
//! a paragraph.
//!
//! Decompression is unaffected: `zstd::decode_all` reads both framings and every
//! reader here uses it.

use std::path::{Path, PathBuf};

/// Source roots that produce container bytes. Anything under these must go
/// through [`codetracer_ctfs::compress_pledged`].
const WRITER_ROOTS: [&str; 2] = ["codetracer_ctfs/src", "codetracer_trace_writer/src"];

/// Files allowed to name the streaming API, and why. Each is a place that
/// *documents* or *tests* the difference rather than writing a stream with it.
const ALLOWED: [(&str, &str); 5] = [
    (
        "codetracer_ctfs/src/zstd_frame.rs",
        "defines the replacement and uses encode_all as its negative control",
    ),
    (
        "codetracer_ctfs/src/zstd_compat.rs",
        "the per-target codec `compress_pledged` is BUILT ON, not a stream writer. It is the one \
         place the wasm32 build's pure-Rust encoder is reached, and on wasm the pledge is added to \
         its output afterwards by `zstd_frame::pledge_frame_content_size`. Adding it here was \
         forced by this census going red on the merge that brought the two branches together, \
         which is the behaviour it was written for.",
    ),
    (
        "codetracer_trace_writer/src/column_aware.rs",
        "module docs explaining why the streaming API is wrong here",
    ),
    ("codetracer_trace_writer/src/step_stream.rs", "the same explanation beside the call site"),
    (
        "codetracer_trace_writer/src/value_stream.rs",
        "comment naming the reader error a streaming frame produces",
    ),
];

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is <root>/codetracer_ctfs.
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().expect("workspace root").to_path_buf()
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Strip whole-line comments so a *citation* of the name is not counted as a
/// *call* of it — the two are opposites, and a fixed-string search cannot tell
/// them apart. Lines that are only a comment are dropped; a trailing comment on
/// a code line is left alone, because a call could be on that line too.
fn code_lines(src: &str) -> Vec<(usize, String)> {
    src.lines()
        .enumerate()
        .map(|(i, l)| (i + 1, l.to_string()))
        .filter(|(_, l)| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("*") || t.starts_with("/*"))
        })
        .collect()
}

#[test]
fn no_stream_writer_uses_the_streaming_zstd_api() {
    let root = workspace_root();
    let mut scanned = 0usize;
    let mut offenders: Vec<String> = Vec::new();
    let mut in_allowed_files = 0usize;

    for r in WRITER_ROOTS {
        let mut files = Vec::new();
        rust_files(&root.join(r), &mut files);
        assert!(!files.is_empty(), "{r} produced no .rs files — the scanner is looking in the wrong place");
        for f in files {
            scanned += 1;
            let rel = f.strip_prefix(&root).expect("under root").to_string_lossy().replace('\\', "/");
            let src = std::fs::read_to_string(&f).expect("read source");
            for (n, line) in code_lines(&src) {
                if line.contains("zstd::encode_all") || line.contains("encode_all(") {
                    if ALLOWED.iter().any(|(a, _)| *a == rel) {
                        in_allowed_files += 1;
                    } else {
                        offenders.push(format!("{rel}:{n}: {}", line.trim()));
                    }
                }
            }
        }
    }

    assert!(
        scanned >= 10,
        "control: the scanner must have read a meaningful number of files, got {scanned}"
    );
    assert!(
        offenders.is_empty(),
        "these writer paths use the streaming Zstd API, so the streams they produce read back as EMPTY \
         through the reference reader — route them through codetracer_ctfs::compress_pledged:\n  {}",
        offenders.join("\n  ")
    );

    // The scanner must be able to SEE the needle, or "zero offenders" is a
    // statement about the scanner. `zstd_frame.rs` keeps one deliberate
    // occurrence as its own negative control, so this count is never zero.
    assert!(
        in_allowed_files >= 1,
        "control: the scan found no occurrence of the needle ANYWHERE, including in the file that \
         deliberately keeps one — the needle or the roots are wrong"
    );
}

/// The scanner's own control: it must reject a line it is supposed to reject,
/// and must not count a comment as a call.
#[test]
fn the_scanner_tells_a_call_from_a_citation() {
    let with_call = "fn f() {\n    let x = zstd::encode_all(c, 3)?;\n}\n";
    let with_citation = "// never use zstd::encode_all here\nfn f() {}\n";
    let hits = |s: &str| code_lines(s).iter().filter(|(_, l)| l.contains("encode_all")).count();
    assert_eq!(hits(with_call), 1, "a real call must be seen");
    assert_eq!(hits(with_citation), 0, "a whole-line comment must not be counted as a call");
}
