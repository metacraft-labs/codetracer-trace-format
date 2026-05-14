//! Cross-language trace-filter implementation.
//!
//! This crate implements the trace-filter schema, selector grammar, composition,
//! and classifier algorithm specified in
//! `codetracer-trace-format-spec/Trace-Filters.md`. It is shared across
//! CodeTracer recorder backends so the rules of the trace-filter language are
//! defined once and enforced uniformly. Recorder-specific glue (mapping
//! the host runtime's per-scope identity into the recorder's cache and into a
//! [`ScopeQuery`]) lives in the recorder.
//!
//! ## Surface area
//!
//! * [`Selector`] / [`SelectorKind`] / [`MatchType`] — selector grammar (§ 4).
//! * [`TraceFilterConfig`] — fully resolved, composed configuration loaded
//!   from TOML files (§ 5 composition order).
//! * [`Classifier`] / [`ScopeQuery`] / [`ScopeResolution`] — pure classifier
//!   that maps a scope identifier (module name, file path, qualified object)
//!   to an execution decision and a value-policy. The classifier owns no
//!   per-scope cache — that responsibility belongs to the recorder, which
//!   must stash decisions in the host runtime's native per-scope slot per
//!   spec § 6 (Python `co_extra`, Nim VM `FileIndex.int32` array, etc.).
//! * [`ffi`] — C-FFI surface for non-Rust recorders (gated on the `ffi`
//!   feature). Wraps the above into `extern "C" fn` symbols.
//!
//! ## History
//!
//! The bulk of this code originated inside
//! `codetracer-python-recorder/src/trace_filter/` as the proto-spec.
//! TF-M6 extracted it into this shared crate after the cross-language spec
//! landed in `codetracer-trace-format-spec`.

pub mod config;
pub mod engine;
pub mod error;
pub mod loader;
pub mod model;
pub mod selector;
pub mod summary;

#[cfg(feature = "ffi")]
pub mod ffi;

pub use crate::config::{
    ExecDirective, FilterMeta, FilterSource, FilterSummary, FilterSummaryEntry, IoConfig, IoStream,
    ScopeRule, TraceFilterConfig, ValueAction, ValuePattern,
};
pub use crate::engine::{
    Classifier, CompiledValuePattern, ExecDecision, ScopeQuery, ScopeResolution, ValueKind,
    ValuePolicy,
};
pub use crate::error::{ErrorCode, FilterError, FilterResult};
pub use crate::selector::{MatchType, Selector, SelectorKind};

/// Maximum schema version this crate understands. Per spec § 11, recorders
/// SHOULD refuse to load filter files whose `[meta] version` is higher.
pub const MAX_SCHEMA_VERSION: u32 = 1;
