//! The recorder side of the shared trace-storage contract, as a check a
//! recorder runs over its own checkout.
//!
//! `trace_storage` is the one implementation of the storage configuration,
//! the upload manifest and the managed sender. A recorder binds it (the Rust
//! recorders through this crate, the native recorder through the Nim
//! `codetracer_ctfs/trace_storage_config` and `managed_sender` modules)
//! instead of carrying a private parser, schema type or sender of its own:
//! a private copy drifts from the shared one, and the drift reaches the
//! storage service as a manifest or upload sequence nobody specified.
//!
//! Each recorder owns the test that proves it, in its own suite, by calling
//! [`scan_recorder_sources`] on its checkout root and asserting that nothing
//! is found. The check lives with the recorder so the dependency points from
//! the recorder to this crate; this crate never reads a recorder checkout.
//! The tokens it looks for are listed once, in `recorder_conformance_tokens.txt`
//! beside this file, which the native recorder's Nim test reads as well.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The forbidden-token list, `<kind> <token>` per line (see the file's header).
pub const FORBIDDEN_TOKENS_LIST: &str = include_str!("recorder_conformance_tokens.txt");

/// File extensions scanned: the source languages the recorders are written in.
pub const SOURCE_EXTENSIONS: &[&str] = &["rs", "nim", "py", "rb", "ts", "js"];

/// Directory names never descended into. None of them holds a recorder's own
/// sources: `.git` is VCS state; `target`, `node_modules`, `.venv` and
/// `.nimble` hold build output or third-party packages, whose code the
/// recorder does not own and whose type names it cannot choose; `.direnv` and
/// `.repro` hold environment caches (reprobuild's with GC roots into the Nix
/// store).
pub const SKIPPED_DIRECTORIES: &[&str] = &[".git", "target", "node_modules", ".venv", ".nimble", ".direnv", ".repro"];

/// What a forbidden token marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// A private storage placement parser or schema type (`StorageServer`, ...).
    StorageSchema,
    /// A private managed sender lifecycle or static API parser.
    Sender,
}

impl TokenKind {
    fn from_list_word(word: &str) -> Option<Self> {
        match word {
            "storage-schema" => Some(Self::StorageSchema),
            "sender" => Some(Self::Sender),
            _ => None,
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Self::StorageSchema => "private storage placement parser/schema token",
            Self::Sender => "private managed sender lifecycle/static API parser token",
        }
    }
}

/// One entry of [`FORBIDDEN_TOKENS_LIST`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForbiddenToken {
    pub kind: TokenKind,
    pub token: &'static str,
}

/// The parsed [`FORBIDDEN_TOKENS_LIST`].
///
/// The list is compiled in and its parse is pinned by this module's tests, so
/// a malformed line is a defect in this crate; it is reported as an error
/// rather than skipped, because a skipped line is a token nobody checks.
pub fn forbidden_tokens() -> io::Result<Vec<ForbiddenToken>> {
    parse_token_list(FORBIDDEN_TOKENS_LIST)
}

fn parse_token_list(list: &'static str) -> io::Result<Vec<ForbiddenToken>> {
    let mut tokens = Vec::new();
    for (index, line) in list.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let malformed = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recorder_conformance_tokens.txt line {}: `{line}`", index + 1),
            )
        };
        let (word, token) = line.split_once(char::is_whitespace).ok_or_else(malformed)?;
        let kind = TokenKind::from_list_word(word).ok_or_else(malformed)?;
        let token = token.trim();
        if token.is_empty() {
            return Err(malformed());
        }
        tokens.push(ForbiddenToken { kind, token });
    }
    Ok(tokens)
}

/// A forbidden token found in a recorder source file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub path: PathBuf,
    pub kind: TokenKind,
    pub token: &'static str,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} must not define {} `{}`", self.path.display(), self.kind.describe(), self.token)
    }
}

/// The outcome of [`scan_recorder_sources`].
#[derive(Debug, Default)]
pub struct ScanReport {
    /// Source files read. A caller asserts this is non-zero, so a wrong root
    /// cannot pass by scanning nothing.
    pub files_scanned: usize,
    pub violations: Vec<Violation>,
}

/// Scan every source file of the checkout at `root` for the forbidden tokens.
///
/// Walks `root` recursively, skipping [`SKIPPED_DIRECTORIES`], and reads every
/// file with one of the [`SOURCE_EXTENSIONS`]. Symbolic links are neither
/// followed nor read: a link into the Nix store or out of the tree is not a
/// source of the checkout, a link back into the tree names a file the walk
/// reaches anyway, and following one can loop (`.repro`'s links into ncurses'
/// `include/` once did, failing the walk with ELOOP). A file that is not
/// UTF-8 is matched lossily.
///
/// Fails when `root` is not a directory or a directory cannot be read.
pub fn scan_recorder_sources(root: &Path) -> io::Result<ScanReport> {
    let tokens = forbidden_tokens()?;
    if !fs::metadata(root)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a directory", root.display()),
        ));
    }
    let mut report = ScanReport::default();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                let skipped = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| SKIPPED_DIRECTORIES.contains(&name));
                if !skipped {
                    stack.push(path);
                }
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let is_source = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| SOURCE_EXTENSIONS.contains(&ext));
            if !is_source {
                continue;
            }
            let bytes = fs::read(&path)?;
            let text = String::from_utf8_lossy(&bytes);
            report.files_scanned += 1;
            for forbidden in &tokens {
                if text.contains(forbidden.token) {
                    report.violations.push(Violation {
                        path: path.clone(),
                        kind: forbidden.kind,
                        token: forbidden.token,
                    });
                }
            }
        }
    }
    report.violations.sort_by(|a, b| (&a.path, a.token).cmp(&(&b.path, b.token)));
    Ok(report)
}

/// Whether the Cargo manifest text declares `crate_name` as a normal (not
/// dev- or build-) dependency: under `[dependencies]`, a
/// `[target.<cfg>.dependencies]` table, or as a `[dependencies.<crate_name>]`
/// table of its own.
///
/// A line-level reading of the TOML the recorders' manifests use, not a TOML
/// parser: a dependency spelled in another valid form is reported missing,
/// which fails the caller's check loudly rather than passing it.
pub fn manifest_declares_dependency(manifest: &str, crate_name: &str) -> bool {
    let mut in_dependencies = false;
    for line in manifest.lines() {
        let line = line.trim();
        if let Some(header) = line.strip_prefix('[') {
            let header = header.trim_end_matches(']').trim();
            if header == format!("dependencies.{crate_name}") || header.ends_with(&format!(".dependencies.{crate_name}")) {
                return true;
            }
            in_dependencies = header == "dependencies" || (header.starts_with("target.") && header.ends_with(".dependencies"));
            continue;
        }
        if !in_dependencies {
            continue;
        }
        if let Some(rest) = line.strip_prefix(crate_name) {
            let rest = rest.trim_start();
            if rest.starts_with('=') || rest.starts_with('.') {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, relative: &str, text: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    #[test]
    fn the_token_list_parses_into_both_kinds() {
        let tokens = forbidden_tokens().unwrap();
        assert_eq!(tokens.iter().filter(|t| t.kind == TokenKind::StorageSchema).count(), 10);
        assert_eq!(tokens.iter().filter(|t| t.kind == TokenKind::Sender).count(), 7);
        assert!(tokens.contains(&ForbiddenToken {
            kind: TokenKind::Sender,
            token: "requestSliceUploadUrl(",
        }));
    }

    /// The tokens name the shared implementation's own definitions, so a
    /// rename in `trace_storage` that leaves the list behind fails here
    /// rather than silently turning the recorders' check into a no-op. The
    /// recorders were held to this before the check moved into their suites.
    #[test]
    fn the_shared_implementation_defines_what_a_recorder_must_not() {
        let shared = include_str!("trace_storage.rs");
        for definition in [
            "struct StorageServer",
            "struct StoragePool",
            "struct CtfsShardPolicy",
            "trait SharedSenderBackend",
            "struct ManagedTraceSender",
        ] {
            assert!(shared.contains(definition), "trace_storage.rs no longer defines `{definition}`");
            assert!(
                forbidden_tokens().unwrap().iter().any(|t| t.token == definition),
                "`{definition}` is not in recorder_conformance_tokens.txt"
            );
        }
    }

    #[test]
    fn a_malformed_token_line_is_an_error_not_a_skipped_token() {
        assert!(parse_token_list("storage-schema\n").is_err());
        assert!(parse_token_list("unknown-kind struct Thing\n").is_err());
        assert!(parse_token_list("# comment\n\nsender   finalizeUploadSession(  \n").unwrap()[0].token == "finalizeUploadSession(");
    }

    #[test]
    fn a_clean_recorder_tree_has_no_violations_and_counts_its_files() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), "src/lib.rs", "use codetracer_ctfs::trace_storage::TraceStorageConfig;\n");
        write(root.path(), "lib/recorder.rb", "class Recorder; end\n");
        write(root.path(), "README.md", "struct StorageServer is only prose here\n");
        let report = scan_recorder_sources(root.path()).unwrap();
        assert_eq!(report.files_scanned, 2);
        assert!(report.violations.is_empty(), "{:?}", report.violations);
    }

    #[test]
    fn a_private_parser_and_a_private_sender_are_both_found() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), "src/storage.rs", "pub struct StoragePool { id: String }\n");
        write(
            root.path(),
            "packages/upload.ts",
            "export function go() { return finalizeUploadSession(1); }\n",
        );
        let report = scan_recorder_sources(root.path()).unwrap();
        let found: Vec<_> = report.violations.iter().map(|v| (v.kind, v.token)).collect();
        assert_eq!(
            found,
            vec![
                (TokenKind::Sender, "finalizeUploadSession("),
                (TokenKind::StorageSchema, "struct StoragePool"),
            ]
        );
        assert!(report.violations[1].to_string().contains("private storage placement parser/schema token"));
    }

    #[test]
    fn skipped_directories_are_not_scanned() {
        let root = tempfile::tempdir().unwrap();
        for dir in SKIPPED_DIRECTORIES {
            write(root.path(), &format!("{dir}/dep/mod.py"), "class StorageServer: pass\n");
        }
        write(root.path(), "src/ok.py", "pass\n");
        let report = scan_recorder_sources(root.path()).unwrap();
        assert_eq!(report.files_scanned, 1);
        assert!(report.violations.is_empty(), "{:?}", report.violations);
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_links_are_not_followed() {
        let outside = tempfile::tempdir().unwrap();
        write(outside.path(), "store/private.nim", "type StorageServer = object\n");
        let root = tempfile::tempdir().unwrap();
        write(root.path(), "src/ok.nim", "discard\n");
        std::os::unix::fs::symlink(outside.path().join("store"), root.path().join("linked-dir")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("store/private.nim"), root.path().join("linked.nim")).unwrap();
        std::os::unix::fs::symlink(root.path(), root.path().join("src/loop")).unwrap();
        let report = scan_recorder_sources(root.path()).unwrap();
        assert_eq!(report.files_scanned, 1);
        assert!(report.violations.is_empty(), "{:?}", report.violations);
    }

    #[test]
    fn a_missing_root_is_an_error_not_an_empty_scan() {
        let root = tempfile::tempdir().unwrap();
        assert!(scan_recorder_sources(&root.path().join("absent")).is_err());
    }

    #[test]
    fn only_normal_dependencies_count() {
        let manifest = "[package]\nname = \"rec\"\n\n[dependencies]\nserde = \"1\"\ncodetracer_ctfs = { path = \"../ctfs\" }\n";
        assert!(manifest_declares_dependency(manifest, "codetracer_ctfs"));
        assert!(manifest_declares_dependency(
            "[dependencies.codetracer_ctfs]\npath = \"x\"\n",
            "codetracer_ctfs"
        ));
        assert!(manifest_declares_dependency(
            "[target.'cfg(unix)'.dependencies]\ncodetracer_ctfs.path = \"x\"\n",
            "codetracer_ctfs"
        ));
        assert!(!manifest_declares_dependency(
            "[dev-dependencies]\ncodetracer_ctfs = \"1\"\n",
            "codetracer_ctfs"
        ));
        assert!(!manifest_declares_dependency(
            "[dependencies]\ncodetracer_ctfs_extra = \"1\"\n",
            "codetracer_ctfs"
        ));
        assert!(!manifest_declares_dependency(
            "[build-dependencies]\ncodetracer_ctfs = \"1\"\n",
            "codetracer_ctfs"
        ));
    }
}
