//! The flag-gated `meta.dat` blocks — MCR fields, replay-launch fields, a
//! layout snapshot and the trace-filter provenance chain — across
//! implementations: the Rust and Nim encoders write the same bytes for the
//! same fields, each decoder reads the other's header to the same fields, and
//! the two refuse the same damaged headers.
//!
//! The Nim side is `codetracer_trace_writer/meta_dat.nim` of the sibling
//! `codetracer-trace-format-nim` checkout (`writeMetaDatToBuffer`,
//! `readMetaDat`), built through that repo's own dev shell; the program that
//! drives it is below. No mocks.

#[path = "../../codetracer_ctfs/tests/nim_adjudicator/mod.rs"]
mod nim_adjudicator;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use codetracer_trace_writer::meta_dat::{
    AtomicMode, FLAG_HAS_STEP_STREAM, FilterProvenance, LayoutSnapshot, McrFields, MetaDat, MetaDatBlocks, ReplayLaunchFields, TickSource,
    decode_meta_dat, encode_meta_dat, encode_meta_dat_with_blocks,
};

const RID: &str = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb";

/// `encode <out> all|empty-chain` writes a header with the fixture's blocks
/// (or only an empty provenance chain); `decode <file>` prints its fields as
/// `describe` below does, or `refused: <why>`; `cuts <file> <from>` prints one
/// `ok` or `refused` line per prefix of the file from `from` bytes on.
const NIM_DRIVER: &str = r#"
import std/[os, options, strutils]
import results
import codetracer_trace_types
import codetracer_trace_writer/meta_dat

proc bytesOf(path: string): seq[byte] =
  let s = readFile(path)
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc hex(b: openArray[byte]): string =
  for x in b: result.add(toHex(x, 2).toLowerAscii)

proc describe(c: MetaDatContents): string =
  result = c.recordingId & " " & c.program & " " & c.workdir & " " & c.recorderId & "\n"
  if c.mcrFields.isSome:
    let m = c.mcrFields.get()
    result.add "mcr " & $ord(m.tickSource) & " " & $m.totalThreads & " " & $ord(m.atomicMode) & " " &
      $m.totalEvents & " " & $m.totalCheckpoints & " " & $m.startTimeUnixUs & " " &
      [m.platform, m.tickGranularity, m.tickSourceStr, m.atomicModeStr, m.startTimeStr,
       m.hookProfile, m.hookStrategies.join(",")].join("|") & "\n"
  if c.replayLaunchFields.isSome:
    result.add "replay " & $int(c.replayLaunchFields.get().aslrDisabled) & "\n"
  if c.layoutSnapshotFields.isSome:
    let l = c.layoutSnapshotFields.get()
    result.add "layout " & $l.layoutHash & " " & hex(l.layoutFingerprint) & "\n"
  if c.hasFilterProvenance:
    result.add "provenance " & $c.filterProvenance.len & "\n"
    for e in c.filterProvenance:
      result.add "entry " & e.path & " " & hex(e.sha256) & "\n"

let args = commandLineParams()
case args[0]
of "encode":
  let meta = TraceMetadata(recordingId: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
    program: "prog", workdir: "/wd", args: @["a"])
  var buf: seq[byte]
  if args[2] == "all":
    var sha2: array[32, byte]
    for i in 0 ..< 32: sha2[i] = byte(i)
    var sha1: array[32, byte]
    for i in 0 ..< 32: sha1[i] = 0xab
    var fp: seq[byte]
    for i in 0 ..< 200: fp.add(byte(i))
    buf = writeMetaDatToBuffer(meta, recorderId = "rec",
      mcrFields = some(McrMetaFields(tickSource: tsPerfCounter, totalThreads: 7,
        atomicMode: amSeqCst, totalEvents: 1'u64 shl 40, totalCheckpoints: 300,
        startTimeUnixUs: 1_760_000_000_000_000'u64, platform: "linux-x86_64",
        tickGranularity: "ns", tickSourceStr: "perf_counter", atomicModeStr: "seq_cst",
        startTimeStr: "2026-10-05T22:00:00Z", hookProfile: "default",
        hookStrategies: @["ldpreload", "seccomp_unotify"])),
      replayLaunchFields = some(ReplayLaunchFields(aslrDisabled: true)),
      layoutSnapshotFields = some(LayoutSnapshotFields(layoutHash: 0x0123_4567_89ab_cdef'u64,
        layoutFingerprint: fp)),
      filterProvenance = [FilterProvenance(path: "<inline:builtin-default>", sha256: sha1),
                          FilterProvenance(path: "/p/.trace-filter.toml", sha256: sha2)],
      hasStepStream = true)
  else:
    buf = writeMetaDatToBuffer(meta, recorderId = "rec", emitFilterProvenance = true,
      hasStepStream = true)
  var s = newString(buf.len)
  for k, b in buf: s[k] = char(b)
  writeFile(args[1], s)
  echo "encoded"
of "decode":
  let r = readMetaDat(bytesOf(args[1]))
  if r.isErr: echo "refused: ", r.error
  else: stdout.write describe(r.get())
of "cuts":
  let data = bytesOf(args[1])
  for cut in parseInt(args[2]) ..< data.len:
    echo(if readMetaDat(data[0 ..< cut]).isOk: "ok" else: "refused")
else:
  quit(2)
"#;

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

fn empty_chain() -> MetaDatBlocks {
    MetaDatBlocks {
        filter_provenance: Some(Vec::new()),
        ..MetaDatBlocks::default()
    }
}

fn rust_encode(blocks: &MetaDatBlocks) -> Vec<u8> {
    encode_meta_dat_with_blocks(RID, "prog", &["a".to_string()], "/wd", "rec", FLAG_HAS_STEP_STREAM, 0, blocks)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The decoded header as the Nim driver prints it.
fn describe(m: &MetaDat) -> String {
    let mut out = format!("{} {} {} {}\n", m.recording_id, m.program, m.workdir, m.recorder_id);
    let b = &m.blocks;
    if let Some(c) = &b.mcr {
        let strings = [
            c.platform.clone(),
            c.tick_granularity.clone(),
            c.tick_source_str.clone(),
            c.atomic_mode_str.clone(),
            c.start_time_str.clone(),
            c.hook_profile.clone(),
            c.hook_strategies.join(","),
        ];
        out += &format!(
            "mcr {} {} {} {} {} {} {}\n",
            c.tick_source as u8,
            c.total_threads,
            c.atomic_mode as u8,
            c.total_events,
            c.total_checkpoints,
            c.start_time_unix_us,
            strings.join("|")
        );
    }
    if let Some(r) = &b.replay_launch {
        out += &format!("replay {}\n", r.aslr_disabled as u8);
    }
    if let Some(l) = &b.layout_snapshot {
        out += &format!("layout {} {}\n", l.layout_hash, hex(&l.layout_fingerprint));
    }
    if let Some(chain) = &b.filter_provenance {
        out += &format!("provenance {}\n", chain.len());
        for e in chain {
            out += &format!("entry {} {}\n", e.path, hex(&e.sha256));
        }
    }
    out
}

struct Nim {
    exe: PathBuf,
}

impl Nim {
    fn run(&self, args: &[&str]) -> String {
        let out = Command::new(&self.exe).args(args).output().unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn encode(&self, work: &Path, which: &str) -> Vec<u8> {
        let p = work.join(format!("nim-{which}.dat"));
        let said = self.run(&["encode", p.to_str().unwrap(), which]);
        assert!(said.contains("encoded"), "{said}");
        fs::read(p).unwrap()
    }

    fn decode(&self, work: &Path, bytes: &[u8]) -> String {
        let p = work.join("decode.dat");
        fs::write(&p, bytes).unwrap();
        self.run(&["decode", p.to_str().unwrap()])
    }
}

#[test]
fn the_two_encoders_agree_byte_for_byte_and_the_decoders_read_each_other() {
    let work = tempfile::tempdir().unwrap();
    let Some(exe) = nim_adjudicator::build_driver(work.path(), "meta_dat_driver", NIM_DRIVER) else {
        return;
    };
    let nim = Nim { exe };
    for (which, blocks) in [("all", fixture()), ("empty-chain", empty_chain())] {
        let from_nim = nim.encode(work.path(), which);
        let rust = rust_encode(&blocks);
        assert_eq!(rust, from_nim, "{which}: the two encoders write different bytes");
        let decoded = decode_meta_dat(&from_nim).unwrap();
        assert_eq!(decoded.blocks, blocks, "{which}: the Rust decoder reads the Nim header");
        assert!(decoded.trailing.is_empty());
        assert_eq!(
            nim.decode(work.path(), &rust),
            describe(&decoded),
            "{which}: the Nim decoder reads the Rust header"
        );
    }
    // Control: a header without the blocks is not described as one with them.
    let bare = encode_meta_dat(RID, "prog", &["a".to_string()], "/wd", "rec", FLAG_HAS_STEP_STREAM);
    assert_ne!(
        nim.decode(work.path(), &bare),
        describe(&decode_meta_dat(&rust_encode(&fixture())).unwrap())
    );
}

/// Every prefix that ends inside the blocks is refused by both decoders, and
/// so is a tick source or atomic mode outside its enumeration.
#[test]
fn the_two_decoders_refuse_the_same_damaged_headers() {
    let work = tempfile::tempdir().unwrap();
    let Some(exe) = nim_adjudicator::build_driver(work.path(), "meta_dat_driver", NIM_DRIVER) else {
        return;
    };
    let nim = Nim { exe };
    let full = rust_encode(&fixture());
    let core = rust_encode(&MetaDatBlocks::default()).len();
    let p = work.path().join("full.dat");
    fs::write(&p, &full).unwrap();
    let nim_verdicts: Vec<String> = nim
        .run(&["cuts", p.to_str().unwrap(), &core.to_string()])
        .lines()
        .map(str::to_string)
        .collect();
    let rust_verdicts: Vec<String> = (core..full.len())
        .map(|cut| if decode_meta_dat(&full[..cut]).is_ok() { "ok" } else { "refused" }.to_string())
        .collect();
    assert_eq!(nim_verdicts.len(), full.len() - core);
    assert_eq!(rust_verdicts, nim_verdicts);
    assert!(rust_verdicts.iter().all(|v| v == "refused"), "every cut inside the blocks is refused");

    for (at, value, field) in [(core, 3u8, "tick_source"), (core + 2, 2, "atomic_mode")] {
        let mut bad = full.clone();
        bad[at] = value;
        let rust = decode_meta_dat(&bad).unwrap_err();
        let said = nim.decode(work.path(), &bad);
        assert!(
            rust.contains(field) && said.starts_with("refused") && said.contains(field),
            "{field}: {rust} / {said}"
        );
    }
}

/// The writer carries the blocks it is given into the container's `meta.dat`,
/// and refuses them once the first record has written `meta.dat`.
#[test]
fn the_writer_writes_the_blocks_it_is_given_before_its_first_record() {
    use codetracer_trace_types::Line;
    use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
    use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
    use codetracer_trace_writer::trace_writer::TraceWriter;

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("trace");
    let mut w = CtfsTraceWriter::new("prog", &[]);
    w.set_meta_blocks(fixture()).unwrap();
    TraceWriter::begin_writing_trace_events(&mut w, &base).unwrap();
    AbstractTraceWriter::register_step(&mut w, Path::new("/src/a.py"), Line(1));
    let late = w.set_meta_blocks(empty_chain()).unwrap_err();
    assert!(late.contains("first record"), "{late}");
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();

    let mut r = codetracer_ctfs::CtfsReader::open(&base.with_extension("ct")).unwrap();
    let meta = decode_meta_dat(&r.read_file("meta.dat").unwrap()).unwrap();
    assert_eq!(meta.blocks, fixture());
    assert!(meta.trailing.is_empty());
}
