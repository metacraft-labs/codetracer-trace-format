//! The compact profile, across implementations: the Rust and Nim reference
//! encoders lay the same members out byte for byte, each decoder reads the
//! other's image to the same directory, and the two refuse the same damaged
//! directories (`ctfs-container.md` §1d).
//!
//! The Nim side is `codetracer_ctfs/compact.nim` of the sibling
//! `codetracer-trace-format-nim` checkout, built through that repo's own dev
//! shell by the `nim_adjudicator` plumbing; the program that drives it is
//! below and is compiled against the checkout's sources. The members come from
//! a container the production `CtfsWriter` wrote. No mocks.

mod nim_adjudicator;

use std::fs;
use std::path::Path;
use std::process::Command;

use codetracer_ctfs::compact::{
    encode_compact_container, read_compact_directory, WholeFileCompression, COMPACT_DIRECTORY_OFFSET, COMPACT_ENTRY_SIZE,
};
use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter};

/// `encode <out> <scheme> <name> <payload file>...` lays the members out with
/// `encodeCompactContainer`; `decode <image>` prints the directory
/// `readCompactDirectory` returns, one `entry` line per member, or `refused`.
const NIM_DRIVER: &str = r#"
import std/os
import results
import codetracer_ctfs/types
import codetracer_ctfs/compact

proc bytesOf(path: string): seq[byte] =
  let s = readFile(path)
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

let args = commandLineParams()
case args[0]
of "encode":
  var members: seq[CompactMember]
  var i = 3
  while i < args.len:
    members.add(CompactMember(name: args[i], payload: bytesOf(args[i + 1])))
    i += 2
  let scheme = if args[2] == "1": wfcZstd else: wfcNone
  let image = encodeCompactContainer(members, scheme)
  if image.isErr:
    echo "refused: ", image.error
    quit(1)
  var s = newString(image.get().len)
  for k, b in image.get():
    s[k] = char(b)
  writeFile(args[1], s)
  echo "encoded"
of "decode":
  let dir = readCompactDirectory(bytesOf(args[1]), bodyReconstructed = true)
  if dir.isErr:
    echo "refused: ", dir.error
  else:
    for e in dir.get().entries:
      echo "entry ", e.name, " ", e.offset, " ", e.length
else:
  quit(2)
"#;

fn members() -> Vec<(String, Vec<u8>)> {
    let mut w = CtfsWriter::create_in_memory(4096, 32, CompressionMethod::None).unwrap();
    for (i, (name, len)) in [
        ("meta.dat", 149usize),
        ("events.fmt", 12),
        ("empty.dat", 0),
        ("steps.dat", 9000),
        ("paths.off", 40),
    ]
    .into_iter()
    .enumerate()
    {
        let h = w.add_file(name).unwrap();
        let body: Vec<u8> = (0..len).map(|k| (k * 31 + i * 7) as u8).collect();
        w.write(h, &body).unwrap();
    }
    CtfsReader::from_bytes(w.finish_to_bytes().unwrap()).unwrap().members().unwrap()
}

struct Nim {
    exe: std::path::PathBuf,
}

impl Nim {
    fn build(work: &Path) -> Option<Nim> {
        let (repo, direnv, home) = nim_adjudicator::nim_checker()?;
        let src = work.join("compact_driver.nim");
        fs::write(&src, NIM_DRIVER).unwrap();
        let exe = work.join("compact_driver");
        let out = Command::new("env")
            .args([
                "-i".into(),
                format!("HOME={}", home.display()),
                "PATH=/run/current-system/sw/bin:/usr/bin:/bin".into(),
                direnv.display().to_string(),
                "exec".into(),
                repo.display().to_string(),
                "nim".into(),
                "c".into(),
                "-d:release".into(),
                format!("-p:{}", repo.join("src").display()),
                "--hints:off".into(),
                format!("--nimcache:{}", work.join("nimcache").display()),
                format!("-o:{}", exe.display()),
                src.display().to_string(),
            ])
            .current_dir(&repo)
            .output()
            .expect("failed to spawn env/direnv");
        assert!(
            exe.exists(),
            "the sibling repo's Nim toolchain could not build the compact driver, so the cross-implementation half \
             did not run:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        Some(Nim { exe })
    }

    fn run(&self, args: &[String]) -> String {
        let out = Command::new(&self.exe).args(args).output().unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn encode(&self, work: &Path, members: &[(String, Vec<u8>)], scheme: WholeFileCompression) -> Vec<u8> {
        let out = work.join("nim.ct");
        let mut args = vec!["encode".to_string(), out.display().to_string(), (scheme as u8).to_string()];
        for (i, (name, payload)) in members.iter().enumerate() {
            let p = work.join(format!("payload-{i}"));
            fs::write(&p, payload).unwrap();
            args.extend([name.clone(), p.display().to_string()]);
        }
        let said = self.run(&args);
        assert!(said.contains("encoded"), "{said}");
        fs::read(out).unwrap()
    }

    /// The directory the Nim decoder reads, as `name offset length` lines, or
    /// `None` when it refuses.
    fn decode(&self, work: &Path, image: &[u8]) -> Option<Vec<String>> {
        let p = work.join("decode.ct");
        fs::write(&p, image).unwrap();
        let said = self.run(&["decode".to_string(), p.display().to_string()]);
        if said.starts_with("refused") {
            return None;
        }
        Some(said.lines().filter_map(|l| l.strip_prefix("entry ").map(str::to_string)).collect())
    }
}

fn rust_directory(image: &[u8]) -> Option<Vec<String>> {
    let dir = read_compact_directory(image, true).ok()?;
    Some(dir.iter().map(|e| format!("{} {} {}", e.name, e.offset, e.length)).collect())
}

#[test]
fn the_two_encoders_agree_byte_for_byte_and_the_decoders_read_each_other() {
    let work = tempfile::tempdir().unwrap();
    let Some(nim) = Nim::build(work.path()) else { return };
    let members = members();
    let refs: Vec<(&str, &[u8])> = members.iter().map(|(n, b)| (n.as_str(), b.as_slice())).collect();
    for scheme in [WholeFileCompression::None, WholeFileCompression::Zstd] {
        let rust = encode_compact_container(&refs, scheme).unwrap();
        let nim_image = nim.encode(work.path(), &members, scheme);
        assert_eq!(rust, nim_image, "{scheme:?}: the two encoders lay the members out differently");
        assert_eq!(nim.decode(work.path(), &rust), rust_directory(&rust), "{scheme:?}");
        assert!(rust_directory(&rust).is_some());
    }
    // And the members a reader serves out of the Nim image are the members.
    let nim_image = nim.encode(work.path(), &members, WholeFileCompression::None);
    assert_eq!(CtfsReader::from_bytes(nim_image).unwrap().members().unwrap(), members);
}

/// Every single-field perturbation of the directory is refused by both
/// decoders, or by neither.
#[test]
fn the_two_decoders_refuse_the_same_directories() {
    let work = tempfile::tempdir().unwrap();
    let Some(nim) = Nim::build(work.path()) else { return };
    let members = members();
    let refs: Vec<(&str, &[u8])> = members.iter().map(|(n, b)| (n.as_str(), b.as_slice())).collect();
    let image = encode_compact_container(&refs, WholeFileCompression::None).unwrap();
    let mut cases: Vec<Vec<u8>> = Vec::new();
    for i in 0..members.len() {
        for field in [0usize, 8, 16] {
            let at = COMPACT_DIRECTORY_OFFSET + i * COMPACT_ENTRY_SIZE + field;
            let v = u64::from_le_bytes(image[at..at + 8].try_into().unwrap());
            for w in [v.wrapping_add(1), v.wrapping_sub(1), 0, 40u64.pow(12)] {
                let mut b = image.clone();
                b[at..at + 8].copy_from_slice(&w.to_le_bytes());
                cases.push(b);
            }
        }
    }
    for (at, v) in [(7usize, 1u8), (8, 1), (12, 1), (16, 2), (17, 2), (19, 1), (24, 99)] {
        let mut b = image.clone();
        b[at] = v;
        cases.push(b);
    }
    let mut longer = image.clone();
    longer.push(0);
    cases.push(longer);
    cases.push(image[..image.len() - 1].to_vec());
    let mut refused = 0;
    for (k, case) in cases.iter().enumerate() {
        let rust = rust_directory(case);
        assert_eq!(nim.decode(work.path(), case), rust, "case {k}");
        refused += usize::from(rust.is_none());
    }
    assert!(refused > cases.len() / 2, "most perturbations must be refused, or the cases test nothing");
}
