//! This library implements the C ABI the Nim library declares, and a C host
//! cannot tell the two apart.
//!
//! One C host (`tests/c_abi/parity_host.c`) is compiled against the Nim
//! library's header, `include/codetracer_trace_writer.h` in
//! `codetracer-trace-format-nim`, and linked twice: against the archive the
//! Nim repository's own `build_ffi.nims` builds, and against this crate's
//! staticlib. Every scenario runs under both, and must give:
//!
//! * the same transcript — every return value, and whether each call left an
//!   error message (the host clears the buffer before each call; the wording
//!   of a message is not compared, its presence is);
//! * the same containers — the same member list, in the same order, with the
//!   same bytes, and the same header; a compact container byte for byte.
//!   (A full-profile container's members are placed in blocks as each writer
//!   publishes them, which the format leaves to the writer.)
//!
//! Then each host reads every container either one wrote, and a set of
//! malformed ones (truncated, garbage, and containers whose structure is
//! sound but whose one member is not), and the two read transcripts must be
//! identical: the same answers, and the same refusals.
//!
//! No mocks: both libraries are the shipped builds, driven by a real C host
//! through the real ABI, writing and reading real files. When the Nim
//! repository or its toolchain cannot be found the test fails, naming what is
//! missing; it never passes without having compared.

use std::path::{Path, PathBuf};
use std::process::Command;

fn nim_repo() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = std::env::var_os("CODETRACER_TRACE_FORMAT_NIM_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest.join("../../codetracer-trace-format-nim"));
    assert!(
        repo.join("include/codetracer_trace_writer.h").is_file() && repo.join("build_ffi.nims").is_file(),
        "the Nim library is not at {} (set CODETRACER_TRACE_FORMAT_NIM_DIR to a codetracer-trace-format-nim checkout); \
         this test compares this library against it and cannot run without it",
        repo.display()
    );
    repo
}

fn run(cmd: &mut Command, what: &str) -> String {
    let out = cmd.output().unwrap_or_else(|e| panic!("{what}: could not run {cmd:?}: {e}"));
    assert!(
        out.status.success(),
        "{what} failed ({}):\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The Nim archive, built by the Nim repository's own build script.
fn build_nim_archive(repo: &Path, work: &Path) -> PathBuf {
    if std::env::var_os("CODETRACER_TRACE_FORMAT_NIM_SKIP_NIMBLE_INSTALL").is_none() {
        run(
            Command::new("nimble").args(["install", "--depsOnly", "-y"]).current_dir(repo),
            "nimble install --depsOnly (the Nim library's dependencies)",
        );
    }
    let out = work.join("libcodetracer_trace_writer.a");
    run(
        Command::new("nim")
            .arg("e")
            .arg("--hints:off")
            .arg("build_ffi.nims")
            .arg(format!("--nimcache:{}", work.join("nimcache").display()))
            .arg(format!("--out:{}", out.display()))
            .current_dir(repo),
        "building the Nim C ABI archive with build_ffi.nims",
    );
    out
}

/// This crate's staticlib, built now: an integration test does not link the
/// library (the crate builds no rlib), so Cargo does not rebuild it for one.
fn rust_archive() -> PathBuf {
    let exe = std::env::current_exe().expect("test executable");
    let profile_dir = exe.parent().and_then(Path::parent).expect("target/<profile>/deps/<test>");
    let target_dir = profile_dir.parent().expect("target/<profile>");
    let mut cmd = Command::new(env!("CARGO"));
    cmd.args(["build", "--lib", "-p", "codetracer_trace_writer_ffi"])
        .arg("--target-dir")
        .arg(target_dir)
        .current_dir(env!("CARGO_MANIFEST_DIR"));
    if profile_dir.file_name().is_some_and(|n| n == "release") {
        cmd.arg("--release");
    }
    run(&mut cmd, "building this crate's staticlib");
    let lib = profile_dir.join("libcodetracer_trace_writer_ffi.a");
    assert!(lib.is_file(), "this crate's staticlib is not at {}", lib.display());
    lib
}

fn zstd_libs() -> Vec<String> {
    Command::new("pkg-config")
        .args(["--libs", "libzstd"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).split_whitespace().map(str::to_string).collect())
        .unwrap_or_else(|| vec!["-lzstd".to_string()])
}

fn compile_host(nim_include: &Path, archive: &Path, extra: &[String], out: &Path) {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/c_abi/parity_host.c");
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let mut cmd = Command::new(cc);
    cmd.arg("-o")
        .arg(out)
        .arg(&source)
        .arg(archive)
        .arg(format!("-I{}", nim_include.display()))
        .args(extra)
        .args(["-lpthread", "-ldl", "-lm"]);
    run(&mut cmd, &format!("compiling the C host against {}", archive.display()));
}

const WRITER_SCENARIOS: &[&str] = &[
    "basic",
    "threads",
    "columns",
    "late_columns",
    "linecounts",
    "undeclared_reload",
    "no_line_count_versions",
    "values",
    "meta",
    "in_memory",
    "trailing",
    "free_closes",
    "nulls",
    "not_ready",
    "container",
    "reader_nulls",
    "craft",
    "annotations",
];

/// Run `host` in `dir` and return its transcript; a host that crashes or
/// exits non-zero fails the test.
fn transcript(host: &Path, dir: &Path, args: &[&str]) -> String {
    std::fs::create_dir_all(dir).unwrap();
    run(Command::new(host).args(args).current_dir(dir), &format!("{} {args:?}", host.display()))
}

/// Every `.ct` under `dir`, relative, sorted.
fn containers(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    for scenario in std::fs::read_dir(dir).unwrap().flatten() {
        if !scenario.path().is_dir() {
            continue;
        }
        for f in std::fs::read_dir(scenario.path()).unwrap().flatten() {
            if f.path().extension().is_some_and(|e| e == "ct") {
                out.push(format!("{}/{}", scenario.file_name().to_string_lossy(), f.file_name().to_string_lossy()));
            }
        }
    }
    out.sort();
    out
}

/// The differences between two containers: header, member list and order,
/// member bytes; whole bytes for a compact container.
fn container_differences(a: &Path, b: &Path) -> Vec<String> {
    let (ba, bb) = (std::fs::read(a).unwrap(), std::fs::read(b).unwrap());
    let (ra, rb) = match (
        codetracer_ctfs::CtfsReader::from_bytes(ba.clone()),
        codetracer_ctfs::CtfsReader::from_bytes(bb.clone()),
    ) {
        (Ok(x), Ok(y)) => (x, y),
        _ => {
            return if ba == bb {
                vec![]
            } else {
                vec!["unreadable, and not byte-identical".to_string()]
            };
        }
    };
    let (mut ra, mut rb) = (ra, rb);
    let mut diffs = Vec::new();
    if (ra.block_size(), ra.max_entries(), ra.profile()) != (rb.block_size(), rb.max_entries(), rb.profile()) {
        diffs.push("header".to_string());
    }
    if ra.profile() == codetracer_ctfs::compact::Profile::Compact && ba != bb {
        diffs.push("compact container bytes".to_string());
    }
    let (fa, fb) = (ra.list_files(), rb.list_files());
    if fa != fb {
        diffs.push(format!("members {fa:?} vs {fb:?}"));
    }
    for f in &fa {
        if ra.read_file(f).ok() != rb.read_file(f).ok() {
            diffs.push(format!("{f} bytes"));
        }
    }
    diffs
}

fn first_difference(a: &str, b: &str) -> String {
    for (i, (x, y)) in a.lines().zip(b.lines()).enumerate() {
        if x != y {
            return format!("line {}:\n  nim:  {x}\n  rust: {y}", i + 1);
        }
    }
    format!("lengths {} vs {} lines", a.lines().count(), b.lines().count())
}

#[test]
fn a_c_host_gets_the_same_answers_from_both_libraries() {
    let repo = nim_repo();
    let work = tempfile::Builder::new()
        .prefix("c_abi_parity")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let work = work.path();
    let nim_lib = build_nim_archive(&repo, work);
    let rust_lib = rust_archive();
    let include = repo.join("include");
    let (host_nim, host_rust) = (work.join("host_nim"), work.join("host_rust"));
    compile_host(&include, &nim_lib, &zstd_libs(), &host_nim);
    compile_host(&include, &rust_lib, &[], &host_rust);

    let mut failures = Vec::new();
    let (nim_out, rust_out) = (work.join("nim"), work.join("rust"));
    for s in WRITER_SCENARIOS {
        let (dn, dr) = (nim_out.join(s), rust_out.join(s));
        let tn = transcript(&host_nim, &dn, &[s, dn.to_str().unwrap()]);
        let tr = transcript(&host_rust, &dr, &[s, dr.to_str().unwrap()]);
        assert!(tn.lines().count() > 5, "scenario {s} ran too little to compare:\n{tn}");
        if tn != tr {
            failures.push(format!("scenario {s}: transcripts differ at {}", first_difference(&tn, &tr)));
        }
    }
    let written = containers(&nim_out);
    assert!(written.len() > 15, "the writer scenarios wrote too few containers: {written:?}");
    assert_eq!(written, containers(&rust_out), "both libraries write the same containers");
    for c in &written {
        let d = container_differences(&nim_out.join(c), &rust_out.join(c));
        if !d.is_empty() {
            failures.push(format!("{c}: {}", d.join(", ")));
        }
    }

    // The reads: what either library wrote, and what is malformed.
    let malformed = work.join("malformed");
    std::fs::create_dir_all(&malformed).unwrap();
    let sample = std::fs::read(nim_out.join("basic/basic.ct")).unwrap();
    let mut flipped = sample.clone();
    for i in (4096..flipped.len()).step_by(97) {
        flipped[i] ^= 0x5a;
    }
    for (name, bytes) in [
        ("truncated_half.ct", sample[..sample.len() / 2].to_vec()),
        ("block0_only.ct", sample[..4096].to_vec()),
        ("garbage.ct", (0..9000u32).map(|i| ((i * 37 + 11) % 256) as u8).collect()),
        ("zeros.ct", vec![0u8; 100]),
        ("empty_file.ct", vec![]),
        ("flipped.ct", flipped),
    ] {
        std::fs::write(malformed.join(name), bytes).unwrap();
    }
    let mut malformed_files = vec![];
    for f in std::fs::read_dir(nim_out.join("craft")).unwrap().flatten() {
        std::fs::copy(f.path(), malformed.join(f.file_name())).unwrap();
    }
    for f in std::fs::read_dir(&malformed).unwrap().flatten() {
        malformed_files.push(f.file_name().to_string_lossy().into_owned());
    }
    malformed_files.sort();
    malformed_files.push("missing.ct".to_string());

    let read = |host: &Path, dir: &Path, files: &[String]| {
        let mut args = vec!["read", "."];
        args.extend(files.iter().map(String::as_str));
        transcript(host, dir, &args)
    };
    for (label, dir, files) in [
        ("Nim-written", &nim_out, &written),
        ("Rust-written", &rust_out, &written),
        ("malformed", &malformed, &malformed_files),
    ] {
        let (tn, tr) = (read(&host_nim, dir, files), read(&host_rust, dir, files));
        if tn != tr {
            failures.push(format!(
                "reading the {label} containers: transcripts differ at {}",
                first_difference(&tn, &tr)
            ));
        }
        if label == "Nim-written" {
            // The read did read: the basic recording's steps, values and calls.
            assert!(
                tn.contains("== basic.ct\nopen -> handle err=0\nstep_count -> 9 err=0\ncall_count -> 5 err=0"),
                "{tn}"
            );
            assert!(
                tn.contains("{\"kind\":\"delta_step\",\"line_delta\":1}"),
                "the reads reached a delta step"
            );
            assert!(tn.contains("\"varname_id\":0,\"type_id\":0,\"data\":[163"), "the reads reached a value");
        }
    }

    assert!(failures.is_empty(), "the two libraries differ:\n{}", failures.join("\n"));
}
