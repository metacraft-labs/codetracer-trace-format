//! Filter configuration façade: composes inline and file-based sources into a
//! resolved [`TraceFilterConfig`].
//!
//! Implements the composition order specified in
//! `codetracer-trace-format-spec/Trace-Filters.md` § 5: callers pass inline
//! filter contents first (typically the recorder-shipped builtin default),
//! then file paths in the order discovered/derived from auto-discovery, env
//! variable, and CLI arguments.

pub use crate::model::{
    ExecDirective, FilterMeta, FilterSource, FilterSummary, FilterSummaryEntry, IoConfig, IoStream, ScopeRule, TraceFilterConfig, ValueAction,
    ValuePattern,
};

use crate::error::FilterResult;
use crate::filter_invalid;
use crate::loader::ConfigAggregator;
use std::path::PathBuf;

impl TraceFilterConfig {
    /// Load and compose filters from the provided paths.
    ///
    /// Convenience entry point for callers that have no inline (builtin)
    /// filter contents. For the composition order described in spec § 5,
    /// prefer [`TraceFilterConfig::from_inline_and_paths`].
    pub fn from_paths(paths: &[PathBuf]) -> FilterResult<Self> {
        Self::from_inline_and_paths(&[], paths)
    }

    /// Load and compose filters from inline TOML sources combined with paths.
    ///
    /// Inline entries are ingested first in the order provided, followed by
    /// files. This matches the spec § 5 composition order when callers pass:
    /// 1. inline = builtin default (`("builtin-default", BUILTIN_TOML)`)
    /// 2. paths  = auto-discovery, env, CLI in that order
    pub fn from_inline_and_paths(inline: &[(&str, &str)], paths: &[PathBuf]) -> FilterResult<Self> {
        if inline.is_empty() && paths.is_empty() {
            return Err(filter_invalid!("no trace filter sources supplied"));
        }

        let mut aggregator = ConfigAggregator::default();
        for (label, contents) in inline {
            aggregator.ingest_inline(label, contents)?;
        }
        for path in paths {
            aggregator.ingest_file(path)?;
        }

        aggregator.finish()
    }
}
