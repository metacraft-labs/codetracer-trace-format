//! The two writers write the flag-gated `meta.dat` blocks they are given —
//! MCR fields, replay-launch fields, a layout snapshot and the trace-filter
//! provenance chain (`internal-files.md` §"Extended Fields") — byte for byte
//! alike, and both refuse one given after the first record.
//!
//! The Rust writer takes the blocks through `CtfsTraceWriter::set_meta_blocks`,
//! the Nim writer through the C ABI (`trace_writer_set_mcr_fields`,
//! `_set_replay_launch_fields`, `_set_layout_snapshot`,
//! `_add_filter_provenance`). Both record the same one-step trace under one
//! recording id, and each container's `meta.dat` is read back out of the file
//! the writer produced. No mocks.

use std::path::{Path, PathBuf};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::Line;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::meta_dat::{
    decode_meta_dat, AtomicMode, FilterProvenance, LayoutSnapshot, McrFields, MetaDatBlocks, ReplayLaunchFields, TickSource,
};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimMcrFields, NimTraceWriter, TraceEventsFileFormat};

const PROGRAM: &str = "prog";
const RECORDING_ID: &str = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb";
const WORKDIR: &str = "/wd";

fn fixture() -> MetaDatBlocks {
    MetaDatBlocks {
        mcr: Some(McrFields {
            tick_source: TickSource::PerfCounter,
            total_threads: 7,
            atomic_mode: AtomicMode::SeqCst,
            total_events: 1 << 40,
            total_checkpoints: 300,
            start_time_unix_us: 1_760_000_000_000_000,
            platform: "linux-x86_64".into(),
            tick_granularity: "ns".into(),
            tick_source_str: "perf_counter".into(),
            atomic_mode_str: "seq_cst".into(),
            start_time_str: "2026-10-05T22:00:00Z".into(),
            hook_profile: "default".into(),
            hook_strategies: vec!["ldpreload".into(), "seccomp_unotify".into()],
        }),
        replay_launch: Some(ReplayLaunchFields { aslr_disabled: true }),
        layout_snapshot: Some(LayoutSnapshot {
            layout_hash: 0x0123_4567_89ab_cdef,
            layout_fingerprint: (0..200u8).collect(),
        }),
        filter_provenance: Some(vec![
            FilterProvenance {
                path: "<inline:builtin-default>".into(),
                sha256: [0xab; 32],
            },
            FilterProvenance {
                path: "/p/.trace-filter.toml".into(),
                sha256: core::array::from_fn(|i| i as u8),
            },
        ]),
    }
}

/// Hand `blocks` to the Nim writer, every block it carries.
fn give_nim(w: &mut NimTraceWriter, blocks: &MetaDatBlocks) -> Result<(), String> {
    let e = |e: Box<dyn std::error::Error>| e.to_string();
    if let Some(m) = &blocks.mcr {
        w.set_mcr_fields(&NimMcrFields {
            tick_source: m.tick_source as u8,
            total_threads: m.total_threads,
            atomic_mode: m.atomic_mode as u8,
            total_events: m.total_events,
            total_checkpoints: m.total_checkpoints,
            start_time_unix_us: m.start_time_unix_us,
            platform: &m.platform,
            tick_granularity: &m.tick_granularity,
            tick_source_str: &m.tick_source_str,
            atomic_mode_str: &m.atomic_mode_str,
            start_time_str: &m.start_time_str,
            hook_profile: &m.hook_profile,
            hook_strategies: &m.hook_strategies,
        })
        .map_err(e)?;
    }
    if let Some(r) = &blocks.replay_launch {
        w.set_replay_launch_fields(r.aslr_disabled).map_err(e)?;
    }
    if let Some(l) = &blocks.layout_snapshot {
        w.set_layout_snapshot(l.layout_hash, &l.layout_fingerprint).map_err(e)?;
    }
    if let Some(chain) = &blocks.filter_provenance {
        if chain.is_empty() {
            w.record_empty_filter_provenance().map_err(e)?;
        }
        for entry in chain {
            w.add_filter_provenance(&entry.path, &entry.sha256).map_err(e)?;
        }
    }
    Ok(())
}

/// Two steps through the Nim writer, given `blocks` before them; the
/// container's path and what giving them again after them said.
fn write_nim(dir: &Path, blocks: &MetaDatBlocks) -> (PathBuf, Result<(), String>) {
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.set_recording_id(RECORDING_ID).unwrap();
    w.set_workdir(Path::new(WORKDIR));
    w.begin_writing_trace_events(&dir.join("trace.json")).unwrap();
    w.begin_writing_trace_metadata(&dir.join("trace_metadata.json")).unwrap();
    w.begin_writing_trace_paths(&dir.join("trace_paths.json")).unwrap();
    give_nim(&mut w, blocks).unwrap();
    // The C ABI holds a step until the next record, so the first step is
    // written, and `meta.dat` with it, when the second is registered.
    w.register_step(Path::new("/src/a.py"), Line(1));
    w.register_step(Path::new("/src/a.py"), Line(2));
    let late = give_nim(&mut w, &fixture());
    w.finish_writing_trace_events().unwrap();
    w.finish_writing_trace_metadata().unwrap();
    w.finish_writing_trace_paths().unwrap();
    // The refused late call leaves the writer's sticky failure set, which
    // `close` reports; the container is finalized all the same.
    let _ = w.close();
    (dir.join(format!("{PROGRAM}.ct")), late)
}

/// The same through the Rust writer.
fn write_rust(dir: &Path, blocks: &MetaDatBlocks) -> (PathBuf, Result<(), String>) {
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.set_recording_id(RECORDING_ID);
    AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
    w.set_meta_blocks(blocks.clone()).unwrap();
    let out = dir.join(PROGRAM);
    TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
    AbstractTraceWriter::register_step(&mut w, Path::new("/src/a.py"), Line(1));
    AbstractTraceWriter::register_step(&mut w, Path::new("/src/a.py"), Line(2));
    let late = w.set_meta_blocks(fixture());
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    (out.with_extension("ct"), late)
}

fn meta_of(ct: &Path) -> Vec<u8> {
    CtfsReader::open(ct).unwrap().read_file("meta.dat").unwrap()
}

#[test]
fn both_writers_write_the_blocks_they_are_given_byte_for_byte() {
    let (nim_dir, rust_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (nim_ct, nim_late) = write_nim(nim_dir.path(), &fixture());
    let (rust_ct, rust_late) = write_rust(rust_dir.path(), &fixture());
    let (nim_meta, rust_meta) = (meta_of(&nim_ct), meta_of(&rust_ct));
    assert_eq!(nim_meta, rust_meta, "the two writers' meta.dat differ");
    let decoded = decode_meta_dat(&nim_meta).unwrap();
    assert_eq!(decoded.blocks, fixture());
    assert!(decoded.trailing.is_empty());

    // Both refuse a block given after the first record, and saying so.
    let nim_late = nim_late.unwrap_err();
    let rust_late = rust_late.unwrap_err();
    assert!(nim_late.contains("first record"), "{nim_late}");
    assert!(rust_late.contains("first record"), "{rust_late}");

    // Control: the same recording given no blocks writes a different meta.dat,
    // so the comparison above is able to fail, and the two still agree.
    let (nim_dir, rust_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let bare_nim = meta_of(&write_nim(nim_dir.path(), &MetaDatBlocks::default()).0);
    let bare_rust = meta_of(&write_rust(rust_dir.path(), &MetaDatBlocks::default()).0);
    assert_eq!(bare_nim, bare_rust);
    assert_ne!(bare_nim, nim_meta);
    assert_eq!(decode_meta_dat(&bare_nim).unwrap().blocks, MetaDatBlocks::default());
}
