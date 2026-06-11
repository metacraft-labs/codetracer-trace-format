//! Catalog of curated TOML rename lists for popular CDN-distributed JS
//! libraries.
//!
//! Spec: `codetracer-specs/Planned-Features/Column-Aware-Tracing-And-Deminification.milestones.org` §P8.
//!
//! ## What this crate does
//!
//! Exposes a small typed wrapper over a `codetracer-mapping-catalog/`
//! directory layout:
//!
//! ```text
//! codetracer-mapping-catalog/
//! ├── index.toml                 # [[entry]] rows, machine-readable
//! └── catalog/
//!     └── <library>/<version>/<file>.toml      # the rename list
//! ```
//!
//! The crate's responsibility is **lookup + filesystem mechanics**:
//!
//! * Locate the catalog on disk (`CT_CATALOG_PATH` env, or a default
//!   under the user's cache dir).
//! * Parse the `index.toml`.
//! * Provide three lookup paths:
//!   * by SHA-256 of the recorded minified source (the replay-server's
//!     auto-load hot path),
//!   * by library name (+ optional version),
//!   * by raw substring filter (the CLI's `catalog list --filter` path).
//! * Compute SHA-256 of an on-disk file (helper that both consumers
//!   share so they agree on encoding / streaming behaviour).
//!
//! It does **not** parse the per-entry rename TOML — that's the
//! `RenameList` parser's job, owned by the db-backend.  Keeping the
//! split lets the catalog crate stay tiny and lets the rename-schema
//! evolve without rippling here.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// One entry in the catalog's `index.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CatalogEntry {
    /// Library name (npm package name in practice — `lodash`,
    /// `jquery`, ...).
    pub library: String,
    /// Library version string.  We treat it as opaque text; no semver
    /// parsing is done in the lookup path so a `4.17.21` entry doesn't
    /// match a `^4.17` recording-side query.
    pub version: String,
    /// Filename the entry covers — typically the minified bundle's
    /// basename as the user would download it from the CDN
    /// (`lodash.min.js`, `jquery.min.js`, ...).
    pub file: String,
    /// SHA-256 of the minified bundle as it shipped.  Hex-encoded,
    /// 64 lowercase characters.  The replay-server compares this to
    /// the recorded source's on-disk SHA before applying.
    pub sha256: String,
    /// Path to the rename TOML, **relative** to the catalog root so
    /// the catalog directory can be relocated without rewriting the
    /// index.
    pub toml_path: String,
    /// Free-form provenance tag (`from-sourcemap`, `infer`,
    /// `infer-llm`, `hand-curated`).  Surfaced by `catalog list` so
    /// users can sanity-check how a given entry was derived before
    /// trusting it.
    pub provenance: String,
}

/// Top-level shape of `index.toml`.
#[derive(Debug, Deserialize)]
struct RawIndex {
    /// `[[entry]]` array.  `default` so an empty index parses to an
    /// empty catalog (the §P8 spec accepts "no entries" as valid).
    #[serde(default)]
    entry: Vec<CatalogEntry>,
}

/// In-memory catalog.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    /// Absolute path to the catalog root (the directory containing
    /// `index.toml`).  Preserved so the resolved `toml_path` for each
    /// entry can be turned into an absolute on-disk path.
    root: PathBuf,
    /// Parsed entries, in the order they appear in `index.toml`.
    entries: Vec<CatalogEntry>,
}

/// Errors surfaced by [`Catalog::load`] and friends.
#[derive(Debug)]
pub enum CatalogError {
    /// Filesystem error while reading the index or an entry's TOML.
    Io(io::Error),
    /// `index.toml` failed to parse.
    ParseToml(toml::de::Error),
    /// Requested entry was not found by the lookup helper that returns
    /// `Result` (the `Option`-returning helpers return `None`).
    MissingEntry { library: String, version: Option<String> },
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::Io(e) => write!(f, "catalog I/O error: {e}"),
            CatalogError::ParseToml(e) => write!(f, "catalog index.toml parse error: {e}"),
            CatalogError::MissingEntry { library, version: Some(v) } => {
                write!(f, "no catalog entry for {library}@{v}")
            }
            CatalogError::MissingEntry { library, version: None } => {
                write!(f, "no catalog entry for {library}")
            }
        }
    }
}

impl std::error::Error for CatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CatalogError::Io(e) => Some(e),
            CatalogError::ParseToml(e) => Some(e),
            CatalogError::MissingEntry { .. } => None,
        }
    }
}

impl From<io::Error> for CatalogError {
    fn from(e: io::Error) -> Self {
        CatalogError::Io(e)
    }
}

impl From<toml::de::Error> for CatalogError {
    fn from(e: toml::de::Error) -> Self {
        CatalogError::ParseToml(e)
    }
}

impl Catalog {
    /// Load a catalog from `<catalog_path>/index.toml`.
    ///
    /// `catalog_path` is the directory containing `index.toml` (NOT the
    /// `index.toml` file itself).  Use [`catalog_path_from_env`] to
    /// resolve the default path from `CT_CATALOG_PATH` or the user
    /// cache dir.
    pub fn load(catalog_path: &Path) -> Result<Self, CatalogError> {
        let index_path = catalog_path.join("index.toml");
        let bytes = fs::read(&index_path).map_err(|e| {
            // Wrap so the error includes the index path the loader was
            // trying to read — invaluable when the user mistyped
            // `CT_CATALOG_PATH`.
            CatalogError::Io(io::Error::new(
                e.kind(),
                format!("reading {}: {e}", index_path.display()),
            ))
        })?;
        let text = std::str::from_utf8(&bytes).map_err(|e| {
            CatalogError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is not valid UTF-8: {e}", index_path.display()),
            ))
        })?;
        let raw: RawIndex = toml::from_str(text)?;
        Ok(Catalog {
            root: catalog_path.to_path_buf(),
            entries: raw.entry,
        })
    }

    /// Build an empty catalog rooted at `catalog_path`.  Used by tests
    /// + by code paths that want to surface "I tried to look up but
    /// there's no catalog" without erroring (e.g. the replay-server's
    /// best-effort autoload path).
    pub fn empty(catalog_path: &Path) -> Self {
        Catalog {
            root: catalog_path.to_path_buf(),
            entries: Vec::new(),
        }
    }

    /// Absolute path to the catalog root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// All parsed entries in `index.toml` order.
    pub fn entries(&self) -> &[CatalogEntry] {
        &self.entries
    }

    /// `true` when the catalog has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Resolve an entry's `toml_path` to an absolute on-disk path.
    pub fn entry_toml_path(&self, entry: &CatalogEntry) -> PathBuf {
        self.root.join(&entry.toml_path)
    }

    /// Lookup by SHA-256 of the recorded minified bundle.
    ///
    /// The replay-server's auto-load hot path.  Match is on the full
    /// 64-character hex string, case-insensitively (the
    /// `compute_file_sha256` helper emits lowercase but we tolerate
    /// either to keep the format forgiving).
    pub fn lookup_by_sha(&self, sha256: &str) -> Option<&CatalogEntry> {
        let needle = sha256.trim().to_ascii_lowercase();
        if needle.len() != 64 {
            // Defensive: a truncated or empty sha cannot match anything;
            // fail closed rather than risk a partial hex collision.
            return None;
        }
        self.entries
            .iter()
            .find(|e| e.sha256.trim().eq_ignore_ascii_case(&needle))
    }

    /// Lookup by library name (and optional version).
    ///
    /// When `version` is `Some`, an exact-equality match is required
    /// (no semver coercion).  When `None`, all versions of the named
    /// library are returned in `index.toml` order.
    pub fn lookup_by_library(&self, library: &str, version: Option<&str>) -> Vec<&CatalogEntry> {
        self.entries
            .iter()
            .filter(|e| e.library.eq_ignore_ascii_case(library))
            .filter(|e| match version {
                Some(v) => e.version == v,
                None => true,
            })
            .collect()
    }

    /// Filter entries by case-insensitive substring across the
    /// `library`, `version`, and `file` columns.  Powers `catalog list
    /// --filter <substring>`.
    pub fn filter_substring(&self, needle: &str) -> Vec<&CatalogEntry> {
        let n = needle.to_ascii_lowercase();
        self.entries
            .iter()
            .filter(|e| {
                e.library.to_ascii_lowercase().contains(&n)
                    || e.version.to_ascii_lowercase().contains(&n)
                    || e.file.to_ascii_lowercase().contains(&n)
            })
            .collect()
    }
}

/// Resolve the catalog directory the consumer should read.
///
/// Resolution order:
///
/// 1. `CT_CATALOG_PATH` environment variable (when set and non-empty).
/// 2. `$XDG_CACHE_HOME/codetracer/mapping-catalog/` (or the OS
///    equivalent, via the `dirs` crate).
/// 3. `./codetracer-mapping-catalog/` next to the current working
///    directory — last-resort fallback so a developer who cloned the
///    catalog into their workspace doesn't need to set the env var.
pub fn catalog_path_from_env() -> PathBuf {
    if let Ok(p) = std::env::var("CT_CATALOG_PATH")
        && !p.trim().is_empty()
    {
        return PathBuf::from(p);
    }
    if let Some(cache) = dirs::cache_dir() {
        return cache.join("codetracer").join("mapping-catalog");
    }
    PathBuf::from("./codetracer-mapping-catalog")
}

/// SHA-256 of an on-disk file, hex-encoded, 64 lowercase characters.
///
/// Streams the file in 64 KiB chunks so a large bundle doesn't have to
/// be held fully in memory.  Used by both the replay-server's
/// auto-load hook (to hash the recorded source) and the
/// `ct-mapping-tools catalog install` path (to verify the cataloged
/// entry's sha matches a target file).
pub fn compute_file_sha256(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    io::copy(&mut file, &mut hasher)?;
    Ok(hex_encode(&hasher.finalize()))
}

/// Hex-encode a byte slice, lowercase, with no separator.
///
/// Kept in this crate (rather than pulling in `hex` as a dep) because
/// the use case is exactly one buffer size (32 bytes for a SHA-256
/// digest) and a manual `format!` is simpler than another transitive
/// dependency.
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Build a minimal but realistic catalog on disk for tests to load.
    fn write_test_catalog(dir: &Path) {
        fs::create_dir_all(dir.join("catalog/lodash/4.17.21")).unwrap();
        fs::create_dir_all(dir.join("catalog/tinylib/1.0.0")).unwrap();
        fs::write(
            dir.join("catalog/lodash/4.17.21/lodash.min.js.toml"),
            "# placeholder\n",
        )
        .unwrap();
        fs::write(
            dir.join("catalog/tinylib/1.0.0/tinylib.min.js.toml"),
            r#"
                [[rename]]
                file = "tinylib.min.js"
                from = "a"
                to = "add"
            "#,
        )
        .unwrap();
        fs::write(
            dir.join("index.toml"),
            r#"
                [[entry]]
                library = "lodash"
                version = "4.17.21"
                file = "lodash.min.js"
                sha256 = "aaaa000000000000000000000000000000000000000000000000000000000000"
                toml_path = "catalog/lodash/4.17.21/lodash.min.js.toml"
                provenance = "from-sourcemap"

                [[entry]]
                library = "tinylib"
                version = "1.0.0"
                file = "tinylib.min.js"
                sha256 = "f8fe147e9644f8cd3ef7cb4e4971ee212fab57950cbe664101d42c3b48b7f9de"
                toml_path = "catalog/tinylib/1.0.0/tinylib.min.js.toml"
                provenance = "hand-curated"
            "#,
        )
        .unwrap();
    }

    #[test]
    fn load_parses_entries_in_order() {
        let dir = tempfile::tempdir().unwrap();
        write_test_catalog(dir.path());
        let cat = Catalog::load(dir.path()).expect("load");
        let entries = cat.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].library, "lodash");
        assert_eq!(entries[1].library, "tinylib");
        assert_eq!(entries[1].sha256.len(), 64);
    }

    #[test]
    fn lookup_by_sha_matches_case_insensitive() {
        let dir = tempfile::tempdir().unwrap();
        write_test_catalog(dir.path());
        let cat = Catalog::load(dir.path()).expect("load");
        let hit = cat
            .lookup_by_sha("F8FE147E9644F8CD3EF7CB4E4971EE212FAB57950CBE664101D42C3B48B7F9DE")
            .expect("hit");
        assert_eq!(hit.library, "tinylib");
    }

    #[test]
    fn lookup_by_sha_rejects_truncated_input() {
        let dir = tempfile::tempdir().unwrap();
        write_test_catalog(dir.path());
        let cat = Catalog::load(dir.path()).expect("load");
        assert!(cat.lookup_by_sha("aaaa").is_none());
        assert!(cat.lookup_by_sha("").is_none());
    }

    #[test]
    fn lookup_by_library_filters_versions() {
        let dir = tempfile::tempdir().unwrap();
        write_test_catalog(dir.path());
        let cat = Catalog::load(dir.path()).expect("load");
        let hits = cat.lookup_by_library("lodash", None);
        assert_eq!(hits.len(), 1);
        let hits = cat.lookup_by_library("lodash", Some("4.17.21"));
        assert_eq!(hits.len(), 1);
        let hits = cat.lookup_by_library("lodash", Some("9.9.9"));
        assert_eq!(hits.len(), 0);
    }

    #[test]
    fn filter_substring_spans_columns() {
        let dir = tempfile::tempdir().unwrap();
        write_test_catalog(dir.path());
        let cat = Catalog::load(dir.path()).expect("load");
        // Matches `library`.
        assert_eq!(cat.filter_substring("lodash").len(), 1);
        // Matches `version`.
        assert_eq!(cat.filter_substring("1.0.0").len(), 1);
        // Matches `file`.
        assert_eq!(cat.filter_substring("min.js").len(), 2);
    }

    #[test]
    fn compute_file_sha256_matches_known_value() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("data.txt");
        fs::write(&p, b"hello").unwrap();
        // sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        assert_eq!(
            compute_file_sha256(&p).unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn catalog_path_from_env_respects_override() {
        // SAFETY: env mutation here is process-global; tests run in a
        // single binary on a thread pool. The CT_CATALOG_PATH var is
        // not read by anything else in this crate's tests; even so we
        // restore the var at function exit.
        let key = "CT_CATALOG_PATH";
        let orig = std::env::var(key).ok();
        unsafe { std::env::set_var(key, "/tmp/some-catalog") };
        let resolved = catalog_path_from_env();
        assert_eq!(resolved, PathBuf::from("/tmp/some-catalog"));
        match orig {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    #[test]
    fn empty_index_parses_to_zero_entries() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("index.toml"), "").unwrap();
        let cat = Catalog::load(dir.path()).expect("empty parse");
        assert!(cat.is_empty());
        assert_eq!(cat.len(), 0);
    }

    #[test]
    fn missing_index_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = Catalog::load(dir.path()).expect_err("no index");
        assert!(matches!(err, CatalogError::Io(_)));
    }
}
