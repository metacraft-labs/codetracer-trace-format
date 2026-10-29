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

/// Cargo's target-native outputs, including the system closure rustc reports.
struct NativeBuild {
    archive: PathBuf,
    system_libs: Vec<String>,
    zstd_root: PathBuf,
}

fn compiler(work: &Path) -> cc::Tool {
    let tool = cc::Build::new()
        .host(env!("CT_PARITY_HOST"))
        .target(env!("CT_PARITY_TARGET"))
        .opt_level(0)
        .debug(false)
        .static_crt(cfg!(target_feature = "crt-static"))
        .cargo_metadata(false)
        .out_dir(work)
        .get_compiler();
    assert_eq!(
        tool.is_like_msvc(),
        cfg!(target_env = "msvc"),
        "the C host compiler must match the Rust target ABI: {:?}",
        tool.path()
    );
    tool
}

/// Nim's passC value is inserted into its compiler command line, so preserve
/// each cc::Tool argument as one argument through that extra parsing boundary.
fn nim_compiler_arg(arg: &std::ffi::OsStr) -> String {
    let arg = arg.to_str().expect("Nim compiler arguments must be UTF-8");
    if cfg!(windows) {
        // Windows argv quoting: backslashes before a quote or the closing
        // quote are doubled. Ordinary path separators are preserved.
        let mut quoted = String::from("\"");
        let mut slashes = 0;
        for c in arg.chars() {
            if c == '\\' {
                slashes += 1;
                continue;
            }
            if c == '\"' {
                quoted.extend(std::iter::repeat_n('\\', 2 * slashes + 1));
            } else {
                quoted.extend(std::iter::repeat_n('\\', slashes));
            }
            quoted.push(c);
            slashes = 0;
        }
        quoted.extend(std::iter::repeat_n('\\', 2 * slashes));
        quoted.push('\"');
        quoted
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}

/// The Nim archive is still its own build script, with the same real target
/// compiler/environment and Cargo-built Zstd headers as the independent C host.
fn build_nim_archive(repo: &Path, work: &Path, tool: &cc::Tool, native: &NativeBuild) -> PathBuf {
    if std::env::var_os("CODETRACER_TRACE_FORMAT_NIM_SKIP_NIMBLE_INSTALL").is_none() {
        run(
            Command::new("nimble").args(["install", "--depsOnly", "-y"]).current_dir(repo),
            "nimble install --depsOnly (the Nim library's dependencies)",
        );
    }
    let msvc = cfg!(target_env = "msvc");
    let target = if msvc {
        "msvc"
    } else if cfg!(windows) {
        "mingw"
    } else {
        "posix"
    };
    let family = if tool.is_like_msvc() {
        "vcc"
    } else if tool.is_like_clang() {
        "clang"
    } else {
        "gcc"
    };
    let out = work.join(if msvc {
        "codetracer_trace_writer.lib"
    } else {
        "libcodetracer_trace_writer.a"
    });
    let include = native.zstd_root.join("include");
    assert!(
        include.join("zstd.h").is_file(),
        "Cargo's Zstd output has no header: {}",
        include.display()
    );
    let mut cmd = Command::new("nim");
    cmd.arg("e")
        .arg("--hints:off")
        .arg("build_ffi.nims")
        .arg(format!("--target:{target}"))
        .arg(format!("--nimcache:{}", work.join("nimcache").display()))
        .arg(format!("--out:{}", out.display()))
        .arg("--")
        .arg(format!("--cc:{family}"))
        .arg(format!("--{family}.exe:{}", tool.path().display()));
    // Keep cc's target/CRT/sysroot and caller compiler configuration, in its
    // original order, in both independent compiles. The owning Nim script
    // retains its runtime/threads/ABI flags; these configure its C compiler.
    for arg in tool.args() {
        cmd.arg(format!("--passC:{}", nim_compiler_arg(arg)));
    }
    let include_arg = std::ffi::OsString::from(format!("{}{}", if msvc { "/I" } else { "-I" }, include.display()));
    cmd.arg(format!("--passC:{}", nim_compiler_arg(&include_arg)))
        .envs(tool.env().iter().cloned())
        .current_dir(repo);
    run(&mut cmd, "building the Nim C ABI archive with build_ffi.nims");
    assert!(out.is_file(), "the owning Nim build did not produce {}", out.display());
    out
}

/// Build the actual Rust library and read Cargo's documented messages. The
/// same artifact/build-script metadata is emitted for cached builds too:
/// https://doc.rust-lang.org/cargo/reference/external-tools.html
/// No target-directory glob or PATH data lookup can substitute another Zstd.
fn rust_archive() -> NativeBuild {
    let exe = std::env::current_exe().expect("test executable");
    let profile_dir = exe.parent().and_then(Path::parent).expect("target/<profile>/deps/<test>");
    let parent = profile_dir.parent().expect("target/<profile>");
    let explicit_target = parent.file_name().is_some_and(|p| p == env!("CT_PARITY_TARGET"));
    let target_dir = if explicit_target {
        parent.parent().expect("target root")
    } else {
        parent
    };
    let mut cmd = Command::new(env!("CARGO"));
    cmd.args(["rustc", "--locked", "--lib", "-p", "codetracer_trace_writer_ffi", "--message-format=json"])
        .arg("--target-dir")
        .arg(target_dir)
        .current_dir(env!("CARGO_MANIFEST_DIR"));
    if explicit_target || env!("CT_PARITY_HOST") != env!("CT_PARITY_TARGET") {
        cmd.args(["--target", env!("CT_PARITY_TARGET")]);
    }
    if profile_dir.file_name().is_some_and(|n| n == "release") {
        cmd.arg("--release");
    }
    // Ask rustc for its real native system closure, including ordered framework
    // argument pairs; copying a Winlibs list would silently drift with Rust.
    cmd.args(["--", "--print=native-static-libs"]);
    let output = run(&mut cmd, "building this crate's staticlib and native link closure");
    let mut archives = std::collections::BTreeSet::new();
    let mut zstd_roots = std::collections::BTreeSet::new();
    let mut system_libs = None;
    for line in output.lines().filter(|line| line.starts_with('{')) {
        let value: serde_json::Value = serde_json::from_str(line).expect("Cargo JSON message");
        match value["reason"].as_str() {
            Some("compiler-artifact") if value["target"]["name"] == "codetracer_trace_writer_ffi" => {
                let name = if cfg!(target_env = "msvc") {
                    "codetracer_trace_writer_ffi.lib"
                } else {
                    "libcodetracer_trace_writer_ffi.a"
                };
                // A cdylib also emits a .dll.lib on MSVC: that is NOT the
                // independent static library this test promises to compare.
                for file in value["filenames"].as_array().expect("artifact filenames") {
                    let file = PathBuf::from(file.as_str().expect("artifact path"));
                    if file.file_name().is_some_and(|n| n == name) {
                        archives.insert(file);
                    }
                }
            }
            Some("build-script-executed")
                if value["package_id"]
                    .as_str()
                    .is_some_and(|id| id.rsplit('#').next().is_some_and(|name| name.starts_with("zstd-sys@"))) =>
            {
                zstd_roots.insert(PathBuf::from(value["out_dir"].as_str().expect("Zstd OUT_DIR")));
            }
            Some("compiler-message") if value["target"]["name"] == "codetracer_trace_writer_ffi" => {
                if let Some(libs) = value["message"]["message"].as_str().and_then(|s| s.strip_prefix("native-static-libs:")) {
                    let libs: Vec<String> = libs.split_whitespace().map(str::to_owned).collect();
                    if let Some(previous) = &system_libs {
                        assert_eq!(previous, &libs, "ambiguous native link closure");
                    }
                    system_libs = Some(libs);
                }
            }
            _ => {}
        }
    }
    assert_eq!(archives.len(), 1, "Cargo must report exactly one target static archive: {archives:?}");
    assert_eq!(zstd_roots.len(), 1, "Cargo must report one target-native Zstd output: {zstd_roots:?}");
    let archive = archives.into_iter().next().unwrap();
    assert!(archive.is_file(), "Cargo's static archive is absent: {}", archive.display());
    let zstd_root = zstd_roots.into_iter().next().unwrap();
    let system_libs = system_libs.expect("rustc must report native-static-libs");
    assert!(!system_libs.is_empty(), "the native system link closure must be explicit");
    NativeBuild {
        archive,
        system_libs,
        zstd_root,
    }
}

fn compile_host(nim_include: &Path, archive: &Path, extra: &[PathBuf], out: &Path, tool: &cc::Tool, native: &NativeBuild) {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/c_abi/parity_host.c");
    let mut cmd = tool.to_command();
    // MSVC writes intermediate .obj/.pdb files too: they belong to this private
    // test directory, never to the checked-out source or another running test.
    cmd.current_dir(out.parent().expect("private host output directory"));
    if tool.is_like_msvc() {
        cmd.arg("/nologo")
            .arg("/TC")
            .arg(format!("/Fe:{}", out.display()))
            .arg(format!("/I{}", nim_include.display()))
            .arg(&source)
            .arg(archive)
            .args(extra)
            .arg("/link")
            .args(&native.system_libs);
    } else {
        cmd.arg("-o")
            .arg(out)
            .arg(&source)
            .arg(archive)
            .arg(format!("-I{}", nim_include.display()))
            .args(extra)
            .args(&native.system_libs);
    }
    run(&mut cmd, &format!("compiling the independent C host against {}", archive.display()));
    assert!(out.is_file(), "the native C host was not produced: {}", out.display());
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
    "non_utf8",
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
    let native = rust_archive();
    let tool = compiler(work);
    let nim_lib = build_nim_archive(&repo, work, &tool, &native);
    let zstd_lib = native.zstd_root.join(if cfg!(target_env = "msvc") { "zstd.lib" } else { "libzstd.a" });
    assert!(
        zstd_lib.is_file(),
        "Cargo's matching native Zstd archive is absent: {}",
        zstd_lib.display()
    );
    let include = repo.join("include");
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let (host_nim, host_rust) = (work.join(format!("host_nim{suffix}")), work.join(format!("host_rust{suffix}")));
    compile_host(&include, &nim_lib, &[zstd_lib], &host_nim, &tool, &native);
    compile_host(&include, &native.archive, &[], &host_rust, &tool, &native);

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
                tn.contains("== basic.ct\nopen -> handle err=0\nstep_count -> 9 err=0\nlogical_step_count -> 9 err=0\ncall_count -> 5 err=0"),
                "{tn}"
            );
            // The threads recording's raise, catch and thread records are
            // exec records but not steps: four steps among ten records.
            assert!(
                tn.contains("== threads.ct\nopen -> handle err=0\nstep_count -> 10 err=0\nlogical_step_count -> 4 err=0"),
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
