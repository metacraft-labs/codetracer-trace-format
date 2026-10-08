//! Following a container while it is written (`ctfs-container.md` §6,
//! "Live progress: per-stream following"), by both readers, of both writers'
//! output.
//!
//! Each writer records in stages into a file and is left open between them.
//! At every stage the Rust [`TraceFollower`] and the Nim reader (through its C
//! ABI, `ct_reader_refresh`) refresh the handles they opened at the first
//! stage, and each is compared with a reader of its own kind opened afresh at
//! that moment and with the other implementation. What is asserted, and how
//! each can fail:
//!
//! 1. A refreshed reader answers what a fresh open answers: record counts and
//!    every step's position. A refresh that missed new chunks, or exposed one
//!    before its index entry, differs.
//! 2. The two implementations answer alike at every stage, on both writers'
//!    containers.
//! 3. The Rust follower is incremental: a refresh that finds nothing new
//!    decodes nothing and keeps the chunk it holds; one that finds new chunks
//!    decodes one (to count the new last chunk), and reading the new steps
//!    after it decodes only new chunks.
//! 4. A container that changes in a way no writer changes one is refused by
//!    both: a member that shrank, and a published chunk that moved.
//!
//! No mocks: the writers, the readers and the container files are the
//! shipped ones; the malformed containers are made by editing a real one's
//! root directory and index bytes.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::base40::base40_encode;
use codetracer_trace_reader::trace_follower::TraceFollower;
use codetracer_trace_types::{EventLogKind, Line, TypeKind, ValueRecord};
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "follow";
const SRC: &str = "/src/follow.py";
/// Steps per stage: more than one 4096-record `steps.dat` chunk, so a stage
/// seals step chunks, and many 256-record value chunks and 64-record event
/// chunks.
const STEPS_PER_STAGE: i64 = 5000;
const STAGES: usize = 3;

#[derive(Clone, Copy, Debug)]
enum Writer {
    Nim,
    Rust,
}

/// One of the two writers. Each implements its own crate's `TraceWriter`,
/// whose methods have the same names; `each!` calls one through whichever
/// this is.
enum AnyWriter {
    Nim(NimTraceWriter),
    Rust(Box<CtfsTraceWriter>),
}

macro_rules! each {
    ($w:expr, $x:ident => $body:expr) => {
        match $w {
            AnyWriter::Nim($x) => $body,
            AnyWriter::Rust($x) => {
                use codetracer_trace_writer::trace_writer::TraceWriter as _;
                $body
            }
        }
    };
}

/// A writer recording into `dir`, left open between stages.
struct Recording {
    w: AnyWriter,
    ct: PathBuf,
}

impl Recording {
    fn begin(writer: Writer, dir: &Path) -> Recording {
        let src = PathBuf::from(SRC);
        let mut w = match writer {
            Writer::Nim => {
                let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
                w.begin_writing_trace_events(&dir.join("e.json")).expect("nim begin_events");
                AnyWriter::Nim(w)
            }
            Writer::Rust => {
                use codetracer_trace_writer::trace_writer::TraceWriter as _;
                let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
                w.begin_writing_trace_events(&dir.join(PROGRAM)).expect("rust begin_events");
                AnyWriter::Rust(Box::new(w))
            }
        };
        each!(&mut w, x => x.start(&src, Line(1)));
        Recording {
            w,
            ct: dir.join(format!("{PROGRAM}.ct")),
        }
    }

    /// One stage: steps carrying a value, an I/O event every 100 steps and a
    /// call returning every 50.
    fn stage(&mut self, stage: usize) {
        let src = PathBuf::from(SRC);
        each!(&mut self.w, w => {
            let int = w.ensure_type_id(TypeKind::Int, "Int");
            let f = w.ensure_function_id("f", &src, Line(3));
            for i in 0..STEPS_PER_STAGE {
                let n = stage as i64 * STEPS_PER_STAGE + i;
                w.register_step(&src, Line(1 + n % 9));
                w.register_variable_with_full_value("x", ValueRecord::Int { i: n, type_id: int });
                if n % 100 == 0 {
                    w.register_special_event(EventLogKind::Write, "", &format!("line {n}\n"));
                }
                if n % 50 == 0 {
                    w.register_call(f, vec![]);
                    w.register_step(&src, Line(3));
                    w.register_return(ValueRecord::None { type_id: int });
                }
            }
        })
    }

    fn finish(mut self) -> PathBuf {
        each!(&mut self.w, w => {
            w.finish_writing_trace_events().expect("finish_events");
            w.close().expect("close");
        });
        self.ct
    }
}

/// What a reader answers about a container: its record counts and every
/// step's position.
#[derive(Debug, PartialEq, Eq)]
struct View {
    steps: u64,
    calls: u64,
    events: u64,
    positions: Vec<u64>,
}

fn rust_view(f: &mut TraceFollower) -> View {
    let positions = match f.steps() {
        None => Vec::new(),
        Some(s) => (0..s.count())
            .map(|i| match s.read(i).expect("rust step read") {
                StepStreamRecord::Step { global_line_index } => global_line_index,
                other => panic!("step {i} is not a step: {other:?}"),
            })
            .collect(),
    };
    View {
        steps: positions.len() as u64,
        calls: f.calls().map_or(0, |c| c.count()),
        events: f.events().map_or(0, |e| e.count()),
        positions,
    }
}

fn nim_view(h: &NimTraceReaderHandle) -> View {
    let steps = h.step_count();
    let mut positions = vec![0u64; steps as usize];
    if steps > 0 {
        let got = h.step_global_line_indices(0, steps, &mut positions).expect("nim step positions");
        assert_eq!(got, steps, "the Nim reader returned fewer positions than it counts steps");
    }
    View {
        steps,
        calls: h.call_count(),
        events: h.event_count(),
        positions,
    }
}

fn path_str(p: &Path) -> &str {
    p.to_str().expect("utf-8 path")
}

/// Compare the refreshed readers with fresh ones and with each other.
fn agree(writer: Writer, when: &str, ct: &Path, rust: &mut TraceFollower, nim: &mut NimTraceReaderHandle) -> View {
    rust.refresh().unwrap_or_else(|e| panic!("{writer:?} {when}: rust refresh: {e}"));
    nim.refresh(path_str(ct))
        .unwrap_or_else(|e| panic!("{writer:?} {when}: nim refresh: {e}"));
    let followed = rust_view(rust);
    let fresh = rust_view(&mut TraceFollower::open(ct).expect("rust fresh open"));
    assert_eq!(followed, fresh, "{writer:?} {when}: the refreshed Rust reader differs from a fresh one");
    let nim_followed = nim_view(nim);
    let nim_fresh = nim_view(&NimTraceReaderHandle::open(path_str(ct)).expect("nim fresh open"));
    assert_eq!(
        nim_followed, nim_fresh,
        "{writer:?} {when}: the refreshed Nim reader differs from a fresh one"
    );
    assert_eq!(followed, nim_followed, "{writer:?} {when}: the Rust and Nim readers differ");
    followed
}

/// Follow `writer`'s recording through every stage with both readers.
fn follow_alike(writer: Writer) {
    let dir = tempfile::tempdir().unwrap();
    let mut rec = Recording::begin(writer, dir.path());
    rec.stage(0);
    let ct = rec.ct.clone();
    let mut rust = TraceFollower::open(&ct).expect("rust follower open");
    let mut nim = NimTraceReaderHandle::open(path_str(&ct)).expect("nim open");
    let mut seen = agree(writer, "stage 0", &ct, &mut rust, &mut nim).steps;
    assert!(seen > 0, "{writer:?}: nothing was readable while recording");
    for stage in 1..STAGES {
        rec.stage(stage);
        let view = agree(writer, &format!("stage {stage}"), &ct, &mut rust, &mut nim);
        assert!(view.steps > seen, "{writer:?} stage {stage}: the stage's sealed chunks were not followed");
        seen = view.steps;
    }
    rec.finish();
    let view = agree(writer, "after close", &ct, &mut rust, &mut nim);
    assert!(view.steps > seen, "{writer:?}: the steps sealed at close were not followed");
}

#[test]
fn both_readers_follow_a_nim_written_container_alike() {
    let _g = nim_lock();
    follow_alike(Writer::Nim);
}

#[test]
fn both_readers_follow_a_rust_written_container_alike() {
    let _g = nim_lock();
    follow_alike(Writer::Rust);
}

#[test]
fn a_finished_container_is_followed_to_its_end() {
    let _g = nim_lock();
    for writer in [Writer::Nim, Writer::Rust] {
        let dir = tempfile::tempdir().unwrap();
        let mut rec = Recording::begin(writer, dir.path());
        rec.stage(0);
        let ct = rec.ct.clone();
        let mut rust = TraceFollower::open(&ct).expect("rust follower open");
        let mut nim = NimTraceReaderHandle::open(path_str(&ct)).expect("nim open");
        rec.stage(1);
        let ct = rec.finish();
        rust.refresh().expect("rust refresh after close");
        nim.refresh(path_str(&ct)).expect("nim refresh after close");
        let followed = rust_view(&mut rust);
        assert_eq!(
            followed.steps,
            2 * STEPS_PER_STAGE as u64 + 2 * STEPS_PER_STAGE as u64 / 50 + 1,
            "{writer:?}"
        );
        assert_eq!(
            followed,
            rust_view(&mut TraceFollower::open(&ct).unwrap()),
            "{writer:?}: rust, after close"
        );
        assert_eq!(followed, nim_view(&nim), "{writer:?}: rust and nim, after close");
        assert!(rust.step_map().expect("step-map.ns").is_some(), "{writer:?}: step-map.ns, added at close");
    }
}

#[test]
fn the_rust_follower_decodes_only_what_is_new() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut rec = Recording::begin(Writer::Rust, dir.path());
    rec.stage(0);
    let ct = rec.ct.clone();
    let mut f = TraceFollower::open(&ct).expect("open");
    let first = f.steps().expect("steps").count();
    assert!(first > 0);
    // Read the last readable step, so its chunk is the one held.
    f.steps().unwrap().read(first - 1).expect("read");
    let held = f.steps().unwrap().cached_chunk();
    let before = f.steps().unwrap().inflations();

    // Nothing new: nothing decoded, the held chunk kept.
    f.refresh().expect("refresh");
    assert_eq!(
        f.steps().unwrap().inflations(),
        before,
        "a refresh that found nothing new decoded a chunk"
    );
    assert_eq!(
        f.steps().unwrap().cached_chunk(),
        held,
        "a refresh that found nothing new dropped the held chunk"
    );

    rec.stage(1);
    f.refresh().expect("refresh");
    let s = f.steps().unwrap();
    let grown = s.count();
    assert!(grown > first, "the second stage's sealed chunks were not followed");
    assert_eq!(s.inflations(), before + 1, "a refresh decodes the new last chunk only");
    assert_eq!(s.cached_chunk(), held, "a refresh that found new chunks dropped the held chunk");

    // The new steps, read in order: each chunk they lie in once, and the held
    // chunk not again.
    let cs = s.chunk_size() as u64;
    let after_refresh = s.inflations();
    for i in first..grown {
        s.read(i).expect("read new step");
    }
    let touched = (grown - 1) / cs - first / cs + 1;
    let expected = touched - u64::from(Some((first / cs) as usize) == held);
    assert_eq!(
        s.inflations() - after_refresh,
        expected,
        "reading the new steps decoded chunks it had no need of"
    );
    drop(rec);
}

/// The byte offset in `ct` of member `name`'s root-directory entry.
fn root_entry(ct: &[u8], name: &str) -> usize {
    let key = base40_encode(name).expect("name");
    let header = if ct[5] == 6 { 24 } else { 16 };
    (0..)
        .map(|i| header + i * 24)
        .take_while(|&at| at + 24 <= 4096)
        .find(|&at| u64::from_le_bytes(ct[at + 16..at + 24].try_into().unwrap()) == key)
        .unwrap_or_else(|| panic!("{name} has no root entry"))
}

/// Open both readers on a live recording, apply `damage` to its file, and
/// assert both refuse the refresh naming `member`.
fn refused_on_refresh(writer: Writer, member: &str, damage: impl Fn(&mut Vec<u8>)) {
    let dir = tempfile::tempdir().unwrap();
    let mut rec = Recording::begin(writer, dir.path());
    rec.stage(0);
    let ct = rec.ct.clone();
    let copy = dir.path().join("followed.ct");
    std::fs::copy(&ct, &copy).unwrap();
    let mut rust = TraceFollower::open(&copy).expect("rust open");
    let mut nim = NimTraceReaderHandle::open(path_str(&copy)).expect("nim open");
    // The Nim reader opens each stream when it is first asked about it; ask,
    // so that both readers have read every stream before the damage.
    let before = nim_view(&nim);
    assert_eq!(before, rust_view(&mut rust), "{writer:?}: the readers differ before the damage");
    let mut bytes = std::fs::read(&copy).unwrap();
    damage(&mut bytes);
    std::fs::write(&copy, &bytes).unwrap();
    let r = rust.refresh().expect_err("the Rust follower accepted the damaged container");
    assert!(r.contains(member), "{writer:?}: the Rust refusal does not name {member}: {r}");
    let n = nim.refresh(path_str(&copy)).expect_err("the Nim reader accepted the damaged container");
    assert!(n.to_string().contains(member), "{writer:?}: the Nim refusal does not name {member}: {n}");
    drop(rec);
}

#[test]
fn a_member_that_shrank_is_refused_by_both() {
    let _g = nim_lock();
    for writer in [Writer::Nim, Writer::Rust] {
        refused_on_refresh(writer, "values.dat", |ct| {
            let at = root_entry(ct, "values.dat");
            let size = u64::from_le_bytes(ct[at..at + 8].try_into().unwrap());
            ct[at..at + 8].copy_from_slice(&(size - 1).to_le_bytes());
        });
    }
}

#[test]
fn a_published_chunk_that_moved_is_refused_by_both() {
    let _g = nim_lock();
    for writer in [Writer::Nim, Writer::Rust] {
        refused_on_refresh(writer, "steps.idx", |ct| {
            let at = root_entry(ct, "steps.idx");
            let size = u64::from_le_bytes(ct[at..at + 8].try_into().unwrap()) as usize;
            let map = u64::from_le_bytes(ct[at + 8..at + 16].try_into().unwrap());
            assert!(map & (1 << 63) != 0, "steps.idx is not a one-block member");
            let block = (map & !(1 << 63)) as usize;
            // Move the first chunk's offset (always 0) by one byte.
            assert!(size >= 12, "no published step chunk");
            ct[block * 4096 + 4] = 1;
        });
    }
}
