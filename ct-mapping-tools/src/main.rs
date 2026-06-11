//! `ct-mapping-tools` CLI.
//!
//! Subcommand surface (`from-sourcemap` + `infer` are implemented):
//!
//! ```text
//! ct-mapping-tools from-sourcemap <map_file> [--minified <path>] [--file-name <name>] [--per-function] [--out <output.toml>]
//! ct-mapping-tools infer        <minified> <original> [--file-name <name>] [--language js|ts|python|auto] [--min-confidence <0.0-1.0>] [--out <output.toml>]
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

use ct_mapping_tools::{
    FromSourcemapOptions, InferLlmError, InferLlmOptions, InferOptions, Language, RenameEntry,
    from_sourcemap, infer, infer_llm, to_toml,
};
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
    /// pair using a generic AST aligner (§P7.3).
    ///
    /// Walks the two parse trees in structural lock-step, recording
    /// `(minified, original)` identifier pairs where both nodes share
    /// the same tree-sitter kind.  For each minified name the most-
    /// frequent original wins.  Renames whose top-pair confidence
    /// falls below `--min-confidence` are dropped.
    Infer {
        /// Minified source.
        minified: PathBuf,
        /// Original source.
        original: PathBuf,
        /// `file = "..."` value written into every emitted
        /// `[[rename]]` row.  Defaults to the minified file's basename.
        #[arg(long = "file-name", value_name = "NAME")]
        file_name: Option<String>,
        /// Source language for both inputs.  `auto` (the default)
        /// derives the language from `<minified>`'s extension; pass
        /// an explicit value when the extension is non-standard.
        #[arg(long = "language", value_name = "LANG", default_value = "auto")]
        language: String,
        /// Minimum alignment confidence (top-pair count / total).
        /// Renames below this threshold are dropped.  Default: 0.7.
        #[arg(
            long = "min-confidence",
            value_name = "F64",
            default_value_t = 0.7
        )]
        min_confidence: f64,
        /// Output file (default: stdout).
        #[arg(long = "out", value_name = "PATH")]
        out: Option<PathBuf>,
    },

    /// Produce a TOML rename list from a minified-only source via an
    /// LLM (§P7.4).
    ///
    /// POSTs the minified source to the Anthropic Messages API and
    /// asks the model to propose renames with self-rated confidences.
    /// Requires `CT_LLM_API_KEY` (preferred) or `ANTHROPIC_API_KEY` in
    /// the environment; exits 0 with a `SKIP infer-llm` message when
    /// neither is set so the subcommand is safe to call in CI without
    /// guards.
    InferLlm {
        /// Minified source.
        minified: PathBuf,
        /// Source language for the prompt's syntax hint.  `auto` (the
        /// default) derives from `<minified>`'s extension.
        #[arg(long = "language", value_name = "LANG", default_value = "auto")]
        language: String,
        /// `file = "..."` value written into every emitted
        /// `[[rename]]` row.  Defaults to the minified file's basename.
        #[arg(long = "file-name", value_name = "NAME")]
        file_name: Option<String>,
        /// Output file (default: stdout).
        #[arg(long = "out", value_name = "PATH")]
        out: Option<PathBuf>,
        /// Anthropic model ID.
        #[arg(
            long = "model",
            value_name = "MODEL",
            default_value = "claude-haiku-4-5-20251001"
        )]
        model: String,
        /// API base URL — override to point at a mock server in tests.
        #[arg(
            long = "api-base",
            value_name = "URL",
            default_value = "https://api.anthropic.com/v1"
        )]
        api_base: String,
        /// Minimum self-rated confidence.  Lower than the `infer`
        /// default because LLM proposals are inherently best-effort.
        #[arg(
            long = "min-confidence",
            value_name = "F64",
            default_value_t = 0.5
        )]
        min_confidence: f64,
        /// Cap the number of proposals the model returns (keeps the
        /// prompt + parse cost bounded).
        #[arg(
            long = "max-bindings",
            value_name = "N",
            default_value_t = 50
        )]
        max_bindings: usize,
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
        Command::Infer {
            minified,
            original,
            file_name,
            language,
            min_confidence,
            out,
        } => run_infer(&minified, &original, file_name, &language, min_confidence, out.as_deref()),
        Command::InferLlm {
            minified,
            language,
            file_name,
            out,
            model,
            api_base,
            min_confidence,
            max_bindings,
        } => run_infer_llm(
            &minified,
            &language,
            file_name,
            out.as_deref(),
            model,
            api_base,
            min_confidence,
            max_bindings,
        ),
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

/// Implementation of the `infer` subcommand (§P7.3).
///
/// Resolves the language (auto-detect via extension when
/// `--language auto`), reads both source files, runs the alignment
/// inference, and writes the produced TOML to `out` (or stdout).
fn run_infer(
    minified: &Path,
    original: &Path,
    file_name: Option<String>,
    language: &str,
    min_confidence: f64,
    out: Option<&Path>,
) -> Result<()> {
    // Bounds-check confidence early — clap doesn't validate the f64
    // range, and a negative / >1 value would silently mean "let
    // everything through" / "let nothing through".  Surface as a
    // non-zero exit with the actual offending value.
    if !(0.0..=1.0).contains(&min_confidence) {
        bail!(
            "--min-confidence must be in [0.0, 1.0]; got {}",
            min_confidence
        );
    }

    let lang = resolve_language(language, minified)?;

    let minified_src = fs::read_to_string(minified)
        .with_context(|| format!("failed to read minified source {}", minified.display()))?;
    let original_src = fs::read_to_string(original)
        .with_context(|| format!("failed to read original source {}", original.display()))?;

    // Default the `file = "..."` value to the minified source's
    // basename so the produced TOML records the file the rename
    // applies to (the replay-server matches renames by `file`).
    let file_name = file_name.or_else(|| {
        minified
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
    });

    let opts = InferOptions {
        language: lang,
        file_name,
        min_confidence,
    };
    let result = infer(&minified_src, &original_src, &opts)
        .map_err(|e| anyhow!("inference failed: {e}"))?;

    write_toml_output(&result.entries, out)
}

/// Implementation of the `infer-llm` subcommand (§P7.4).
///
/// Resolves the language (auto-detect via extension when
/// `--language auto`), reads the env var for the API key, and
/// dispatches to [`infer_llm`].  When no API key is set, prints a
/// loud `SKIP` message and exits 0 — the §P7.4 spec calls this
/// behaviour out so CI environments without a key don't fail the
/// command.
#[allow(clippy::too_many_arguments)]
fn run_infer_llm(
    minified: &Path,
    language: &str,
    file_name: Option<String>,
    out: Option<&Path>,
    model: String,
    api_base: String,
    min_confidence: f64,
    max_bindings: usize,
) -> Result<()> {
    if !(0.0..=1.0).contains(&min_confidence) {
        bail!(
            "--min-confidence must be in [0.0, 1.0]; got {}",
            min_confidence
        );
    }

    // Env-var priority: `CT_LLM_API_KEY` (workspace-specific name) wins
    // over `ANTHROPIC_API_KEY` (the upstream default).  Empty-string
    // values count as unset so a user can opt out by setting the var
    // to "" without unsetting it.
    let api_key = std::env::var("CT_LLM_API_KEY")
        .ok()
        .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
        .filter(|k| !k.is_empty());

    let Some(api_key) = api_key else {
        // Skip-loud: print to stdout (NOT stderr) so it shows up in
        // `cargo run` capture and CI logs without being mistaken for
        // an error.  Exit 0 — the spec's contract for "no key, no
        // harm".
        println!("SKIP infer-llm: no API key configured.");
        println!(
            "Set CT_LLM_API_KEY=<your-anthropic-api-key> or ANTHROPIC_API_KEY=<...> to enable."
        );
        return Ok(());
    };

    let lang = resolve_language(language, minified)?;
    let minified_src = fs::read_to_string(minified)
        .with_context(|| format!("failed to read minified source {}", minified.display()))?;

    let file_name = file_name.or_else(|| {
        minified
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
    });

    let opts = InferLlmOptions {
        language: lang,
        file_name,
        model,
        api_base,
        min_confidence,
        max_bindings,
    };

    let result = match infer_llm(&minified_src, &api_key, &opts) {
        Ok(r) => r,
        Err(InferLlmError::NoApiKey) => {
            // Defensive: env-var path above should have intercepted
            // this, but the library returns it for empty keys as
            // well — handle uniformly.
            println!("SKIP infer-llm: no API key configured.");
            return Ok(());
        }
        Err(other) => return Err(anyhow!("infer-llm failed: {other}")),
    };

    // The §P5 TOML schema doesn't carry confidence today; strip it
    // before emitting.  Future schema versions can extend the row.
    let entries: Vec<RenameEntry> = result.entries.into_iter().map(|r| r.entry).collect();
    write_toml_output(&entries, out)
}

/// Resolve a CLI `--language` string to a [`Language`] handle.
///
/// `auto` (the default) reads the minified file's extension and
/// dispatches via [`Language::from_extension`].  Any other value is
/// matched verbatim against [`Language::from_name`] so users can pass
/// `js`, `javascript`, `python`, etc.
///
/// Errors on unknown language names — the CLI surfaces this as a
/// non-zero exit rather than panicking.
fn resolve_language(language: &str, minified: &Path) -> Result<Language> {
    if language.eq_ignore_ascii_case("auto") {
        let ext = minified
            .extension()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                anyhow!(
                    "couldn't auto-detect language: {} has no extension; pass --language explicitly",
                    minified.display()
                )
            })?;
        Language::from_extension(ext).ok_or_else(|| {
            anyhow!(
                "couldn't auto-detect language from extension '.{}'; pass --language explicitly (supported: js, ts, python)",
                ext
            )
        })
    } else {
        Language::from_name(language).ok_or_else(|| {
            anyhow!(
                "unsupported --language '{}' (supported: js, ts, python)",
                language
            )
        })
    }
}

/// Write the produced TOML to `out` or stdout.  Shared between the
/// `from-sourcemap` and `infer` paths so they have identical
/// broken-pipe handling and error wrapping.
fn write_toml_output(entries: &[RenameEntry], out: Option<&Path>) -> Result<()> {
    let toml_text = to_toml(entries);
    match out {
        Some(path) => {
            fs::write(path, &toml_text)
                .with_context(|| format!("failed to write {}", path.display()))?;
        }
        None => {
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
