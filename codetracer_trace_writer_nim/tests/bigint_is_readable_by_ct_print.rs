//! A `BigInt` value must survive a round trip through the PRODUCTION Nim reader.
//!
//! # Why this test exists
//!
//! `ValueRecord::BigInt` is the only full-precision variant in `ValueRecord` — the one a
//! recorder reaches for when a value does not fit an `i64`. Until 2026-09-02 it could not be
//! read back at all: `codetracer_trace_types::base64` serialised the magnitude through
//! `String::serialize`, which in CBOR is a **text** string (major type 3), while the Nim
//! reader reads a **byte** string (major type 2). `ct-print` failed with
//!
//! ```text
//! Error reading events: failed to decode events: cbor: expected byte string (major 2), got major 3
//! ```
//!
//! and — this is the part that made it a blocker rather than a per-value defect — it failed
//! while decoding the EVENT STREAM, so one wide value anywhere made the WHOLE recording
//! unreadable. Recorders worked around it by truncating or by rendering to text instead.
//!
//! # What this asserts, and why it is not a unit test
//!
//! `codetracer_trace_types`' own unit tests assert that the encoder emits major type 2. That
//! is a claim about this workspace. **This test is the claim about the OTHER implementation**:
//! that the reader this campaign actually ships accepts what this writer actually produces.
//! The two are different questions, and only the second one is the bug that was reported.
//!
//! Nothing is mocked: the real pure-Rust `CtfsTraceWriter` writes a real `.ct` container and
//! the real prebuilt `ct-print` reads it.
//!
//! # This test was vacuous once, and the reason is worth keeping
//!
//! The first version drove `NimTraceWriter`. It passed — and went on passing with the encoder
//! mutated back to the broken form, because that writer hands a `BigInt` straight to
//! `ct_value_write_bigint` over the FFI and never reaches the serde encoder at all. Only the
//! pure-Rust writer serialises through `codetracer_trace_types::base64`. The green tick was
//! measuring nothing. Mutating the encoder is what exposed it, and re-running that mutation
//! is the only way to know this test still bites.
//!
//! # Skip policy
//!
//! `ct-print` is a build product of the sibling `codetracer-trace-format-nim` checkout, so it
//! is not guaranteed present. When it is absent this test SKIPS LOUDLY rather than passing:
//! a green tick for a cross-implementation claim that was never checked is exactly the
//! failure mode this campaign keeps finding.

use std::path::{Path, PathBuf};
use std::process::Command;

use codetracer_trace_types::{Line, TypeId, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;

/// The prebuilt production reader from the sibling checkout, if it is there.
fn ct_print() -> Option<PathBuf> {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../codetracer-trace-format-nim/ct-print");
    p.exists().then(|| p.canonicalize().unwrap_or(p))
}

#[test]
fn a_bigint_wider_than_i128_is_read_back_by_the_production_nim_reader() {
    let Some(ct_print) = ct_print() else {
        eprintln!(
            "SKIP: ../../codetracer-trace-format-nim/ct-print is not built, so the \
             cross-implementation half of the BigInt encoding claim did NOT run. This is the \
             only test that checks the Nim reader accepts what this writer emits; build the \
             sibling checkout to exercise it."
        );
        return;
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let program = "bigint_readback";
    let src = dir.path().join("main.nr");
    std::fs::write(&src, "fn main() {}\n").expect("write source");

    // 2^200 — comfortably past i128, which is the case BigInt exists for and the case that
    // used to poison the whole container.
    let mut magnitude = vec![0u8; 26];
    magnitude[0] = 1;

    {
        // THE PURE-RUST CTFS WRITER, and that choice is the whole point of this test.
        // `NimTraceWriter` hands a `BigInt` straight to `ct_value_write_bigint` over the FFI
        // (`codetracer_trace_writer_nim/src/lib.rs:1149`) and never touches
        // `codetracer_trace_types::base64`, so a test written against it passes with the
        // encoder in EITHER state and measures nothing. That mistake was made and caught by
        // mutation here. This is also the writer `aztec-avm-runtime/SOURCE-MAPPING.md` §4.2
        // used when it recorded the original `ct-print` refusal.
        let mut w = CtfsTraceWriter::new(program, &[]);
        TraceWriter::begin_writing_trace_events(&mut w, &dir.path().join(program))
            .expect("begin events");
        w.register_path_with_line_lengths(&src, &[13]);
        AbstractTraceWriter::register_function(&mut w, "main", &src, Line(1));
        AbstractTraceWriter::register_step_with_column(&mut w, &src, Line(1), None);
        AbstractTraceWriter::register_variable_with_full_value(
            &mut w,
            "wide",
            ValueRecord::BigInt { b: magnitude.clone(), negative: false, type_id: TypeId(0) },
        );
        // A small Int beside it, so a reader that dropped the whole stream is distinguishable
        // from one that merely rendered the BigInt oddly.
        AbstractTraceWriter::register_variable_with_full_value(
            &mut w,
            "small",
            ValueRecord::Int { i: 42, type_id: TypeId(0) },
        );
        TraceWriter::finish_writing_trace_events(&mut w).expect("finish events");
    }

    let container = dir.path().join(format!("{program}.ct"));
    assert!(container.exists(), "the writer produced no container at {}", container.display());

    let out = Command::new(&ct_print)
        .arg("--full")
        .arg(&container)
        .output()
        .expect("run ct-print");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        out.status.success(),
        "ct-print REFUSED a container carrying a BigInt (exit {:?}).\n\
         This is the defect `codetracer_trace_types::base64` was changed to fix: a magnitude \
         written as a CBOR text string (major 3) is rejected by this reader, which wants a \
         byte string (major 2), and the rejection takes the whole event stream with it.\n\
         --- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status.code()
    );

    // The control must be present, or "success" could mean the reader emitted nothing.
    assert!(
        stdout.contains("42") || stdout.contains("small"),
        "ct-print exited 0 but did not report the control value, so this container was not \
         really read.\n--- stdout ---\n{stdout}"
    );
}
