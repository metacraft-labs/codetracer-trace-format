//! `ct-mapping-tools` CLI.
//!
//! Subcommand surface (only `from-sourcemap` is implemented in §P7.2):
//!
//! ```text
//! ct-mapping-tools from-sourcemap <map_file> [--minified <path>] [--file-name <name>] [--per-function] [--out <output.toml>]
//! ct-mapping-tools infer        <minified> <original> [--out <output.toml>]    # §P7.3 (stub)
//! ct-mapping-tools infer-llm    <minified>            [--out <output.toml>]    # §P7.4 (stub)
//! ct-mapping-tools catalog      <list|install>        ...                       # §P8.2 (stub)
//! ```
//!
//! Spec: `codetracer-specs/Planned-Features/Column-Aware-Tracing-And-Deminification.milestones.org` §P7.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};

use ct_mapping_tools::{FromSourcemapOptions, from_sourcemap, to_toml};
use sourcemap_translate::SourcemapIndex;

/// Top-level CLI parser.
#[derive(Debug, Parser)]
#[command(
    name = "ct-mapping-tools",
    version,
    about = "Produce CodeTracer TOML rename lists from sourcemaps and source pairs.",
    long_about = "Standalone tooling that produces our TOML rename schema from the inputs users actually have.\n\nAll commands are language-agnostic — they operate on sourcemap V3 + source pairs, not language-specific AST.\n\nSpec: codetracer-specs/Planned-Features/Column-Aware-Tracing-And-Deminification.milestones.org §P7"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Subcommand set.  `from-sourcemap` is the §P7.2 milestone; the
/// others are stubbed so `--help` advertises the full surface and so
/// the binary refuses unknown subcommands with a meaningful clap
/// error rather than crashing.
#[derive(Debug, Subcommand)]
enum Command {
    /// Convert a Source Map V3 (.map / .json) into a TOML rename list.
    ///
    /// The sourcemap's `names[]` array carries the *original*
    /// identifier name for each named segment; the *minified* name is
    /// recovered by reading the generated source at the segment's
    /// generated (line, column).  When two segments resolve the same
    /// minified name to different originals, the most-frequent
    /// original wins.
    FromSourcemap {
        /// Path to the Source Map V3 file (.map or .json).
        map_file: PathBuf,
        /// Generated / minified source file the sourcemap covers.
        ///
        /// Required in practice: without the generated source the CLI
        /// has no way to recover the minified name and the produced
        /// TOML would carry useless `from = to` entries.  The CLI
        /// errors out when this flag is missing AND no sibling file
        /// resolves from the sourcemap's `file` field.
        #[arg(long = "minified", value_name = "PATH")]
        minified: Option<PathBuf>,
        /// `file = "..."` value written into every emitted
        /// `[[rename]]` row.  When omitted, derives from the
        /// sourcemap's V3 `file` field (typically the generated
        /// bundle's basename, e.g. `lodash.min.js`).
        #[arg(long = "file-name", value_name = "NAME")]
        file_name: Option<String>,
        /// Emit `scope = "function:<name>"` entries grouped by
        /// enclosing function (best-effort: V3 sourcemaps rarely
        /// carry the info; falls back to `global` per the §P7.2
        /// spec).
        #[arg(long = "per-function", default_value_t = false)]
        per_function: bool,
        /// Write the produced TOML to this file.  Defaults to stdout
        /// when omitted.
        #[arg(long = "out", value_name = "PATH")]
        out: Option<PathBuf>,
    },

    /// Produce a TOML rename list from a minified+original source
    /// pair using a generic AST aligner (§P7.3 — not yet implemented).
    Infer {
        /// Minified source.
        minified: PathBuf,
        /// Original source.
        original: PathBuf,
        /// Output file (default: stdout).
        #[arg(long = "out", value_name = "PATH")]
        out: Option<PathBuf>,
    },

    /// Produce a TOML rename list from a minified-only source via an
    /// LLM (§P7.4 — not yet implemented).
    InferLlm {
        /// Minified source.
        minified: PathBuf,
        /// Output file (default: stdout).
        #[arg(long = "out", value_name = "PATH")]
        out: Option<PathBuf>,
    },

    /// Operations on the curated mapping catalog (§P8.2 — not yet
    /// implemented).
    Catalog {
        #[command(subcommand)]
        op: CatalogOp,
    },
}

#[derive(Debug, Subcommand)]
enum CatalogOp {
    /// List the catalog index.
    List,
    /// Install a catalog entry into the conventional sibling
    /// location for a trace.
    Install {
        library: String,
        #[arg(long = "version")]
        version: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::FromSourcemap {
            map_file,
            minified,
            file_name,
            per_function,
            out,
        } => run_from_sourcemap(&map_file, minified.as_deref(), file_name, per_function, out.as_deref()),
        Command::Infer { .. } => {
            // §P7.3 — out of scope for this milestone.  The stub
            // surfaces a clear "not yet implemented" error via
            // `anyhow::bail!` rather than `unimplemented!()` so the
            // process exits non-zero with a friendly message instead
            // of a `panicked at ...` trace.
            bail!("`infer` is not yet implemented — planned for milestone §P7.3.")
        }
        Command::InferLlm { .. } => {
            bail!("`infer-llm` is not yet implemented — planned for milestone §P7.4.")
        }
        Command::Catalog { .. } => {
            bail!("`catalog` is not yet implemented — planned for milestone §P8.2.")
        }
    }
}

/// Implementation of the `from-sourcemap` subcommand.  Pulled into a
/// named function so the CLI handler stays small and so future
/// programmatic callers (e.g. integration tests spawning the binary
/// or invoking it via the library) can share the resolution +
/// I/O logic.
fn run_from_sourcemap(
    map_file: &Path,
    minified: Option<&Path>,
    file_name: Option<String>,
    per_function: bool,
    out: Option<&Path>,
) -> Result<()> {
    let map = SourcemapIndex::open(map_file)
        .map_err(|e| anyhow!("failed to open sourcemap {}: {e}", map_file.display()))?;

    // Resolve the minified source: explicit `--minified` first, then
    // sibling file derived from the sourcemap's `file` field.
    let minified_source = match minified {
        Some(path) => Some(
            fs::read_to_string(path)
                .with_context(|| format!("failed to read --minified {}", path.display()))?,
        ),
        None => discover_sibling_minified(map_file, &map)?,
    };

    // The §P7.2 spec requires we error out (non-zero exit) when no
    // minified source can be located — the produced TOML would
    // otherwise be useless `from = original_name, to = original_name`
    // rows.
    if minified_source.is_none() {
        return Err(anyhow!(
            "no minified source available — pass --minified <path> or place the generated file next to the sourcemap (looked for '{}')",
            map.file().unwrap_or("<unknown>")
        ));
    }

    let opts = FromSourcemapOptions {
        file_name,
        per_function,
        minified_source,
    };
    let entries = from_sourcemap(&map, &opts);
    let toml_text = to_toml(&entries);

    match out {
        Some(path) => {
            fs::write(path, &toml_text)
                .with_context(|| format!("failed to write {}", path.display()))?;
        }
        None => {
            // Write to stdout, but don't panic on `BrokenPipe` (e.g.
            // when piped through `head`); convert that into a clean
            // exit instead of an error.
            let mut stdout = std::io::stdout().lock();
            if let Err(e) = stdout.write_all(toml_text.as_bytes())
                && e.kind() != std::io::ErrorKind::BrokenPipe
            {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

/// Try to locate the minified / generated source file next to the
/// sourcemap by following the sourcemap's V3 `file` field.
///
/// Returns:
/// * `Ok(Some(content))` — found and read.
/// * `Ok(None)` — no `file` field, or the named sibling doesn't
///   exist (the caller decides whether that's a hard error).
/// * `Err(_)` — sibling existed but couldn't be read.
fn discover_sibling_minified(map_file: &Path, map: &SourcemapIndex) -> Result<Option<String>> {
    let file_name = match map.file() {
        Some(f) => f,
        None => return Ok(None),
    };
    let dir = map_file.parent().unwrap_or_else(|| Path::new("."));
    let candidate = dir.join(file_name);
    if !candidate.is_file() {
        return Ok(None);
    }
    let content = fs::read_to_string(&candidate)
        .with_context(|| format!("failed to read sibling minified source {}", candidate.display()))?;
    Ok(Some(content))
}
