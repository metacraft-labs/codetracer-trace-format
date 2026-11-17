//! Writing a deeply nested call chain costs time near-linear in its depth.
//!
//! A recursion's returns arrive innermost first, so the records of every
//! frame complete in the reverse of their key order. The calls stream must
//! still hold them in key (entry) order. The Nim writer once restored that
//! order with an insertion sort, which a recursion drives to its worst case:
//! about N^2/2 record moves for N nested calls, eleven minutes at 100k. This
//! writer keeps records in a key-indexed buffer and hands out the completed
//! prefix, so it has no such pass; this pins that for parity.
//!
//! The assertion is on growth, not on a time: the same recursion at N and 8N
//! depth, fastest of several repetitions each. Linear work grows about 8x —
//! somewhat more in a debug build, where a deeper buffer misses cache more —
//! and quadratic work about 64x; the bound sits at 20x, near the geometric
//! midpoint. Falsifiability: re-sort the buffer by key on every return and the
//! ratio goes well above 20.
//!
//! No mocks: the real writer, writing a real container to a temporary
//! directory.

use std::path::Path;
use std::time::{Duration, Instant};

use codetracer_trace_types::{Line, NONE_VALUE};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;

fn write_recursion(depth: usize) -> Duration {
    let dir = tempfile::tempdir().unwrap();
    let mut w = CtfsTraceWriter::new("deep-recursion", &[]);
    TraceWriter::begin_writing_trace_events(&mut w, &dir.path().join("trace")).unwrap();
    let src = Path::new("/src/rec.erl");
    let f = AbstractTraceWriter::ensure_function_id(&mut w, "rec", src, Line(1));
    let t0 = Instant::now();
    for _ in 0..depth {
        AbstractTraceWriter::register_step(&mut w, src, Line(2));
        AbstractTraceWriter::register_call(&mut w, f, vec![]);
    }
    for _ in 0..depth {
        AbstractTraceWriter::register_step(&mut w, src, Line(3));
        AbstractTraceWriter::register_return(&mut w, NONE_VALUE);
    }
    let took = t0.elapsed();
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    took
}

fn fastest(depth: usize, reps: usize) -> Duration {
    (0..reps).map(|_| write_recursion(depth)).min().unwrap()
}

#[test]
fn nested_call_records_are_not_quadratic_in_depth() {
    const N: usize = 5_000;
    const BOUND: f64 = 20.0;
    write_recursion(N); // warm the allocator and caches
    let small = fastest(N, 3);
    let large = fastest(8 * N, 3);
    let ratio = large.as_nanos() as f64 / small.as_nanos().max(1) as f64;
    eprintln!("depth {N}: {small:?}; depth {}: {large:?}; ratio {ratio:.2}", 8 * N);
    assert!(
        ratio < BOUND,
        "writing an 8x deeper recursion took {ratio:.2}x as long ({small:?} -> {large:?}); \
         linear work is ~8x, quadratic ~64x, and the bound is {BOUND}x"
    );
}
