//! Self-contained error type for the trace-filter crate.
//!
//! Recorder integrations convert these at the boundary into the host
//! recorder's native error type (e.g. `recorder_errors::RecorderError` for
//! the Python recorder).  Keeping a local enum avoids forcing every consumer
//! of this crate to depend on a particular error facade.

use std::fmt;

/// Error code matching the recorder-facing taxonomy.  These values exist
/// independently of the host recorder error crate so this library is
/// dependency-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Generic invalid policy / configuration value.
    InvalidPolicyValue,
    /// IO failure while reading a filter file from disk.
    Io,
    /// Schema version newer than [`crate::MAX_SCHEMA_VERSION`].
    UnsupportedSchemaVersion,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::InvalidPolicyValue => f.write_str("ERR_INVALID_POLICY_VALUE"),
            ErrorCode::Io => f.write_str("ERR_IO"),
            ErrorCode::UnsupportedSchemaVersion => f.write_str("ERR_UNSUPPORTED_SCHEMA_VERSION"),
        }
    }
}

/// Error reported by the trace-filter crate.
#[derive(Debug, Clone, thiserror::Error)]
#[error("[{code}] {message}")]
pub struct FilterError {
    pub code: ErrorCode,
    pub message: String,
}

impl FilterError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        FilterError {
            code,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidPolicyValue, message)
    }
}

pub type FilterResult<T> = Result<T, FilterError>;

/// Convenience macro mirroring `recorder_errors::usage!`: build a
/// [`FilterError`] with formatted message.
#[macro_export]
macro_rules! filter_invalid {
    ($($arg:tt)*) => {
        $crate::error::FilterError::new(
            $crate::error::ErrorCode::InvalidPolicyValue,
            format!($($arg)*),
        )
    };
}

#[macro_export]
macro_rules! filter_io {
    ($($arg:tt)*) => {
        $crate::error::FilterError::new(
            $crate::error::ErrorCode::Io,
            format!($($arg)*),
        )
    };
}
