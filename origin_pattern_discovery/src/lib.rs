//! Recorder-side discovery library for CodeTracer origin patterns.
//!
//! Recorders consume this crate at record-start to discover
//! `.codetracer/origin-patterns.toml` files in the recorded program's
//! source tree and its dependency closure, then embed them in the trace
//! under `meta_dat/origin-patterns/<library_id>/<filename>.toml` along
//! with a manifest `index.toml` recording discovery order.
//!
//! Spec reference: GUI/Debugging-Features/Value-Origin-Tracking.md §7.4
//! "Discovery (recording-time)" and "Embedding (trace-time)".
//!
//! # Usage
//!
//! ```no_run
//! use std::path::PathBuf;
//! use codetracer_origin_pattern_discovery::{DiscoveryConfig, discover_and_embed};
//!
//! let config = DiscoveryConfig {
//!     source_root: PathBuf::from("/abs/path/to/program/source"),
//!     dependency_roots: vec![PathBuf::from("/abs/path/to/site-packages")],
//!     extra_includes: vec![],
//!     enabled: true,
//! };
//! let trace_root = PathBuf::from("/tmp/my-trace/");
//! let report = discover_and_embed(&config, &trace_root).unwrap();
//! println!("embedded {} pattern files", report.entries.len());
//! ```

#![forbid(unsafe_code)]

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use codetracer_origin_patterns::{
    DOT_CODETRACER_DIR, EmbeddedPatternEntry, EmbeddedPatternsIndex, INDEX_FILE, META_DAT_SUBDIR, PATTERN_FILENAME, parse_pattern_file,
    serialise_index,
};

pub use codetracer_origin_patterns;

/// Discovery configuration. Recorders fill this in from their CLI flags
/// and the active language ecosystem (Python's `site-packages`,
/// Cargo's manifest path map, npm's `node_modules`, etc.).
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// The recorded program's source root. The walker visits every
    /// `.codetracer/origin-patterns.toml` reachable from this directory
    /// and treats them as patterns shipped with the program.
    pub source_root: PathBuf,
    /// Dependency roots resolved by the active package manager. Each
    /// dependency root contributes one or more pattern files; the
    /// `<library_id>` is derived from the dependency's directory name.
    pub dependency_roots: Vec<PathBuf>,
    /// Extra roots supplied with `--origin-patterns-include <path>` on
    /// the recorder CLI.
    pub extra_includes: Vec<PathBuf>,
    /// When false (set by `--no-origin-patterns`), the walker emits
    /// neither pattern files nor a manifest — replay still works but
    /// falls back to the built-in catalogue.
    pub enabled: bool,
}

impl DiscoveryConfig {
    /// Empty configuration with discovery enabled and no roots. Useful
    /// for tests that want to add roots one at a time.
    pub fn new() -> Self {
        DiscoveryConfig {
            source_root: PathBuf::new(),
            dependency_roots: Vec::new(),
            extra_includes: Vec::new(),
            enabled: true,
        }
    }

    /// Builder helper: set the program's source root.
    pub fn with_source_root(mut self, root: PathBuf) -> Self {
        self.source_root = root;
        self
    }

    /// Builder helper: append a dependency root.
    pub fn with_dependency_root(mut self, root: PathBuf) -> Self {
        self.dependency_roots.push(root);
        self
    }

    /// Builder helper: append an extra root (CLI `--origin-patterns-include`).
    pub fn with_extra_include(mut self, root: PathBuf) -> Self {
        self.extra_includes.push(root);
        self
    }

    /// Builder helper: disable discovery (CLI `--no-origin-patterns`).
    pub fn disabled() -> Self {
        DiscoveryConfig {
            enabled: false,
            ..Self::new()
        }
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of [`discover_and_embed`]. The entries reflect the on-disk
/// state of `meta_dat/origin-patterns/` after embedding.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryReport {
    /// One entry per discovered + embedded pattern file. The order
    /// matches the manifest's order, which is also the order the
    /// classifier loads patterns at replay time.
    pub entries: Vec<EmbeddedPatternEntry>,
    /// Pattern files the walker found but skipped because they could
    /// not be parsed as TOML. Recorders log these; the trace itself
    /// only contains valid files so the classifier never sees a broken
    /// embedded pattern.
    pub skipped: Vec<SkippedFile>,
}

/// One file the discovery walker rejected (with the parser error).
#[derive(Debug, Clone)]
pub struct SkippedFile {
    pub source_path: PathBuf,
    pub reason: String,
}

/// Run the discovery walk described by `config` and embed the discovered
/// pattern files into `trace_root` under `meta_dat/origin-patterns/`.
///
/// When `config.enabled` is `false` the function is a no-op (returns an
/// empty report) — the recorder still completes successfully, but the
/// classifier falls back to the built-in catalogue at replay time.
///
/// The walk visits roots in this canonical order, mirroring spec §7.4:
///
/// 1. The recorded program's source root.
/// 2. Each declared dependency root, in the order the recorder supplied.
/// 3. Each `--origin-patterns-include <path>` extra root, in CLI order.
///
/// Within each root we recursively look for files named
/// `origin-patterns.toml` whose parent directory is named
/// `.codetracer`. Each file becomes one library entry; the `library_id`
/// is the directory name two levels above the file (i.e. the dependency
/// directory or the program source root's name). When a file would
/// otherwise clash (two libraries with the same name), the later entry
/// is renamed with a numeric suffix.
pub fn discover_and_embed(config: &DiscoveryConfig, trace_root: &Path) -> io::Result<DiscoveryReport> {
    if !config.enabled {
        return Ok(DiscoveryReport::default());
    }

    let mut report = DiscoveryReport::default();
    let mut used_library_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Canonical walk order: source root first, then dependency roots in
    // the order the recorder supplied, then extra includes.
    let mut roots: Vec<(&Path, RootKind)> = Vec::new();
    if !config.source_root.as_os_str().is_empty() {
        roots.push((config.source_root.as_path(), RootKind::Program));
    }
    for dep in &config.dependency_roots {
        roots.push((dep.as_path(), RootKind::Dependency));
    }
    for inc in &config.extra_includes {
        roots.push((inc.as_path(), RootKind::ExtraInclude));
    }

    for (root, kind) in roots {
        if !root.exists() {
            continue;
        }
        let discovered = walk_root_for_patterns(root)?;
        for pattern_path in discovered {
            let library_id = derive_library_id(root, &pattern_path, kind);
            let library_id = uniquify_library_id(library_id, &mut used_library_ids);
            embed_pattern_file(trace_root, &library_id, &pattern_path, &mut report)?;
        }
    }

    // Always write the manifest, even when no patterns were discovered,
    // so the classifier can tell "discovery ran but found nothing" apart
    // from "discovery never ran" (`meta_dat/origin-patterns/` absent).
    write_manifest(trace_root, &report.entries)?;
    Ok(report)
}

#[derive(Debug, Clone, Copy)]
enum RootKind {
    Program,
    Dependency,
    ExtraInclude,
}

/// Find every `<root>/**/.codetracer/origin-patterns.toml` under `root`.
fn walk_root_for_patterns(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    // `walkdir` follows symlinks by default off; that matches what we want
    // (we don't want to escape the configured root tree).
    for entry in walkdir::WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path.file_name().and_then(|n| n.to_str()) != Some(PATTERN_FILENAME) {
            continue;
        }
        if path.parent().and_then(|p| p.file_name()).and_then(|n| n.to_str()) != Some(DOT_CODETRACER_DIR) {
            continue;
        }
        out.push(path.to_path_buf());
    }
    // `walkdir` does not guarantee a deterministic order across platforms.
    // We sort the result so the manifest is stable across CI runs.
    out.sort();
    Ok(out)
}

/// Derive a library id from the discovered pattern file's path. We strip
/// the trailing `.codetracer/origin-patterns.toml` and use the immediate
/// containing directory's name. For the program root the id is the root
/// directory's name (so a workspace called `myapp` gets a stable
/// `myapp` library id).
fn derive_library_id(root: &Path, pattern_path: &Path, kind: RootKind) -> String {
    let library_dir = pattern_path
        .parent() // .codetracer
        .and_then(Path::parent); // .codetracer's parent = the library

    match library_dir {
        Some(dir) => match dir.file_name().and_then(|n| n.to_str()) {
            Some(name) if !name.is_empty() => name.to_string(),
            _ => fallback_id(root, kind),
        },
        None => fallback_id(root, kind),
    }
}

fn fallback_id(root: &Path, kind: RootKind) -> String {
    let base = root.file_name().and_then(|n| n.to_str()).unwrap_or("unknown").to_string();
    match kind {
        RootKind::Program => base,
        RootKind::Dependency => format!("dep_{base}"),
        RootKind::ExtraInclude => format!("include_{base}"),
    }
}

fn uniquify_library_id(candidate: String, used: &mut std::collections::HashSet<String>) -> String {
    if used.insert(candidate.clone()) {
        return candidate;
    }
    for n in 2u32..u32::MAX {
        let id = format!("{candidate}-{n}");
        if used.insert(id.clone()) {
            return id;
        }
    }
    candidate // unreachable in practice; we have ~4G of suffix space
}

fn embed_pattern_file(trace_root: &Path, library_id: &str, source: &Path, report: &mut DiscoveryReport) -> io::Result<()> {
    let raw = fs::read_to_string(source)?;
    // Validate as TOML so the embedded set only contains files the
    // classifier can load. Files that fail to parse are recorded in
    // `skipped` so the recorder can surface them in its logs.
    if let Err(e) = parse_pattern_file(&raw) {
        report.skipped.push(SkippedFile {
            source_path: source.to_path_buf(),
            reason: e.to_string(),
        });
        return Ok(());
    }
    let dest_dir = trace_root.join("meta_dat").join(META_DAT_SUBDIR).join(library_id);
    fs::create_dir_all(&dest_dir)?;
    let filename = source
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| PATTERN_FILENAME.to_string());
    let dest_file = dest_dir.join(&filename);
    fs::write(&dest_file, raw.as_bytes())?;
    report.entries.push(EmbeddedPatternEntry {
        library_id: library_id.to_string(),
        filename,
        source_path: Some(source.to_string_lossy().into_owned()),
    });
    Ok(())
}

fn write_manifest(trace_root: &Path, entries: &[EmbeddedPatternEntry]) -> io::Result<()> {
    let manifest_dir = trace_root.join("meta_dat").join(META_DAT_SUBDIR);
    fs::create_dir_all(&manifest_dir)?;
    let manifest_path = manifest_dir.join(INDEX_FILE);
    let index = EmbeddedPatternsIndex { libraries: entries.to_vec() };
    let toml_text = serialise_index(&index).map_err(|e| io::Error::other(format!("serialising origin-patterns index: {e}")))?;
    fs::write(manifest_path, toml_text)?;
    Ok(())
}

/// Read a trace-local `_overrides.toml` file if present. Returns `None`
/// when the file does not exist (the override file is optional per spec
/// §7.4). The classifier handles the actual parsing; this helper exists
/// so recorder-side tooling that wants to round-trip an override file
/// can do so without depending on the classifier crate.
pub fn read_trace_local_overrides(trace_root: &Path) -> io::Result<Option<String>> {
    let path = trace_root
        .join("meta_dat")
        .join(META_DAT_SUBDIR)
        .join(codetracer_origin_patterns::OVERRIDES_FILE);
    match fs::read_to_string(&path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Write a trace-local `_overrides.toml` file. Recorders or downstream
/// tooling use this when materialising override files programmatically.
pub fn write_trace_local_overrides(trace_root: &Path, contents: &str) -> io::Result<()> {
    let dir = trace_root.join("meta_dat").join(META_DAT_SUBDIR);
    fs::create_dir_all(&dir)?;
    fs::write(dir.join(codetracer_origin_patterns::OVERRIDES_FILE), contents)?;
    Ok(())
}

/// CLI helper: parse `--origin-patterns-include <path>` and
/// `--no-origin-patterns` flags out of an argv vector. Recorder CLIs
/// call this from their argument parser so the flag semantics stay in
/// one place. Unrecognised flags are returned unchanged in `remainder`
/// for the recorder's own parser to handle.
pub fn parse_cli_flags<'a, I>(args: I) -> CliParseResult
where
    I: IntoIterator<Item = &'a str>,
{
    let mut result = CliParseResult::default();
    let mut iter = args.into_iter().peekable();
    while let Some(arg) = iter.next() {
        match arg {
            "--no-origin-patterns" => {
                result.disabled = true;
            }
            "--origin-patterns-include" => match iter.next() {
                Some(path) => result.includes.push(PathBuf::from(path)),
                None => result.errors.push("--origin-patterns-include requires a path argument".to_string()),
            },
            other if other.starts_with("--origin-patterns-include=") => {
                let path = &other["--origin-patterns-include=".len()..];
                if path.is_empty() {
                    result.errors.push("--origin-patterns-include= requires a non-empty path".to_string());
                } else {
                    result.includes.push(PathBuf::from(path));
                }
            }
            _ => result.remainder.push(arg.to_string()),
        }
    }
    result
}

/// Output of [`parse_cli_flags`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CliParseResult {
    /// `true` iff `--no-origin-patterns` was on the command line.
    pub disabled: bool,
    /// Every `--origin-patterns-include <path>` argument, in order.
    pub includes: Vec<PathBuf>,
    /// Arguments the helper did not consume (the recorder's own parser
    /// handles these).
    pub remainder: Vec<String>,
    /// Non-fatal CLI errors (e.g. missing value after flag).
    pub errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cli_flags_no_origin_patterns() {
        let r = parse_cli_flags(["--foo", "--no-origin-patterns", "bar"]);
        assert!(r.disabled);
        assert_eq!(r.remainder, vec!["--foo".to_string(), "bar".to_string()]);
    }

    #[test]
    fn parse_cli_flags_includes_with_space() {
        let r = parse_cli_flags(["--origin-patterns-include", "/some/path", "--keep"]);
        assert_eq!(r.includes, vec![PathBuf::from("/some/path")]);
        assert!(!r.disabled);
        assert_eq!(r.remainder, vec!["--keep".to_string()]);
    }

    #[test]
    fn parse_cli_flags_includes_with_equals() {
        let r = parse_cli_flags(["--origin-patterns-include=/another"]);
        assert_eq!(r.includes, vec![PathBuf::from("/another")]);
    }

    #[test]
    fn parse_cli_flags_missing_value_yields_error() {
        let r = parse_cli_flags(["--origin-patterns-include"]);
        assert!(r.includes.is_empty());
        assert_eq!(r.errors.len(), 1);
    }

    #[test]
    fn discovery_disabled_writes_nothing() {
        let trace_root = tempfile::tempdir().unwrap();
        let config = DiscoveryConfig::disabled();
        let report = discover_and_embed(&config, trace_root.path()).unwrap();
        assert!(report.entries.is_empty());
        assert!(!trace_root.path().join("meta_dat").exists());
    }
}
