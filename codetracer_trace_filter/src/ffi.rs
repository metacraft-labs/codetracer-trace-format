//! C-FFI surface for non-Rust recorders.
//!
//! Exposes a minimal stable C ABI that lets a recorder written in any
//! language compile filter chains and classify scopes against them. Future
//! Ruby / EVM / JS recorders are expected consumers; the Python recorder
//! does not need this — it links the Rust API directly.
//!
//! All symbols are prefixed `ctf_` (CodeTracer Filter). All strings are
//! UTF-8, null-terminated. Pointer arguments may be null; the caller must
//! check return values.
//!
//! ## Memory ownership
//!
//! * `ctf_classifier_new_from_paths` allocates a [`Classifier`] on the heap
//!   and returns an opaque pointer. The caller MUST eventually free it via
//!   [`ctf_classifier_free`].
//! * `ctf_classify` allocates a [`ScopeResolution`] returned via an out
//!   pointer; the caller MUST free it via [`ctf_resolution_free`].
//! * Strings returned through getters (`ctf_resolution_module_name`,
//!   `ctf_resolution_object_name`, ...) are owned by the resolution; their
//!   lifetime is bound to the resolution pointer.
//! * `ctf_last_error_message` returns a thread-local owned string; it
//!   stays valid until the next FFI call on the same thread.

use crate::config::TraceFilterConfig;
use crate::engine::{Classifier, ExecDecision, ScopeQuery, ScopeResolution, ValueKind};
use crate::error::FilterError;
use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr, CString};
use std::path::PathBuf;
use std::ptr;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(err: &FilterError) {
    let formatted = format!("{}", err);
    let cstring = CString::new(formatted).unwrap_or_else(|_| CString::new("trace-filter error contained NUL byte").expect("static error message"));
    LAST_ERROR.with(|cell| cell.borrow_mut().replace(cstring));
}

fn clear_last_error() {
    LAST_ERROR.with(|cell| cell.borrow_mut().take());
}

/// Return a pointer to the last error message recorded on this thread.
/// The pointer is valid until the next FFI call on the same thread.
///
/// Returns `NULL` when no error is pending.
#[no_mangle]
pub extern "C" fn ctf_last_error_message() -> *const c_char {
    LAST_ERROR.with(|cell| match cell.borrow().as_ref() {
        Some(err) => err.as_ptr(),
        None => ptr::null(),
    })
}

/// Construct a classifier from a list of UTF-8 filter file paths.
///
/// `paths`/`n` provide the list. Files are loaded in order; later files
/// override earlier ones per spec § 5.  Returns NULL on error; call
/// [`ctf_last_error_message`] to retrieve the diagnostic.
///
/// # Safety
///
/// * `paths` MUST point to `n` valid null-terminated UTF-8 C strings.
/// * Each pointer in `paths` MUST remain valid for the duration of the
///   call (the function only borrows them).
#[no_mangle]
pub unsafe extern "C" fn ctf_classifier_new_from_paths(paths: *const *const c_char, n: usize) -> *mut Classifier {
    clear_last_error();
    if paths.is_null() && n != 0 {
        set_last_error(&FilterError::invalid("paths pointer is NULL but n > 0"));
        return ptr::null_mut();
    }

    let mut owned_paths: Vec<PathBuf> = Vec::with_capacity(n);
    for offset in 0..n {
        let raw = *paths.add(offset);
        if raw.is_null() {
            set_last_error(&FilterError::invalid(format!("paths[{offset}] is NULL")));
            return ptr::null_mut();
        }
        let cstr = CStr::from_ptr(raw);
        let s = match cstr.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error(&FilterError::invalid(format!("paths[{offset}] is not valid UTF-8")));
                return ptr::null_mut();
            }
        };
        owned_paths.push(PathBuf::from(s));
    }

    match TraceFilterConfig::from_paths(&owned_paths) {
        Ok(config) => Box::into_raw(Box::new(Classifier::new(config))),
        Err(err) => {
            set_last_error(&err);
            ptr::null_mut()
        }
    }
}

/// Free a classifier allocated by [`ctf_classifier_new_from_paths`].
///
/// # Safety
///
/// `classifier` MUST be a pointer obtained from
/// [`ctf_classifier_new_from_paths`], or NULL.
#[no_mangle]
pub unsafe extern "C" fn ctf_classifier_free(classifier: *mut Classifier) {
    if classifier.is_null() {
        return;
    }
    drop(Box::from_raw(classifier));
}

/// Classify a scope. On success, sets `*out_resolution` to a freshly
/// allocated [`ScopeResolution`] (the caller MUST free it via
/// [`ctf_resolution_free`]) and returns 0. On failure returns non-zero
/// and leaves `*out_resolution` NULL.
///
/// `qualname` and `module_hint` may be NULL.
///
/// # Safety
///
/// All non-NULL pointer arguments MUST point to valid null-terminated
/// UTF-8 strings.  `out_resolution` MUST point to writable storage for a
/// `*mut ScopeResolution`.
#[no_mangle]
pub unsafe extern "C" fn ctf_classify(
    classifier: *const Classifier,
    filename: *const c_char,
    qualname: *const c_char,
    module_hint: *const c_char,
    out_resolution: *mut *mut ScopeResolution,
) -> c_int {
    clear_last_error();
    if classifier.is_null() {
        set_last_error(&FilterError::invalid("classifier pointer is NULL"));
        return 1;
    }
    if filename.is_null() {
        set_last_error(&FilterError::invalid("filename is NULL"));
        return 2;
    }
    if out_resolution.is_null() {
        set_last_error(&FilterError::invalid("out_resolution is NULL"));
        return 3;
    }

    let filename_str = match CStr::from_ptr(filename).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error(&FilterError::invalid("filename is not valid UTF-8"));
            return 4;
        }
    };

    let qualname_str = if qualname.is_null() {
        None
    } else {
        match CStr::from_ptr(qualname).to_str() {
            Ok(s) => Some(s),
            Err(_) => {
                set_last_error(&FilterError::invalid("qualname is not valid UTF-8"));
                return 4;
            }
        }
    };

    let module_hint_str = if module_hint.is_null() {
        None
    } else {
        match CStr::from_ptr(module_hint).to_str() {
            Ok(s) => Some(s),
            Err(_) => {
                set_last_error(&FilterError::invalid("module_hint is not valid UTF-8"));
                return 4;
            }
        }
    };

    let mut query = ScopeQuery::new(filename_str);
    if let Some(qualname) = qualname_str {
        query = query.with_qualname(qualname);
    }
    if let Some(hint) = module_hint_str {
        query = query.with_module_hint(hint);
    }

    let resolution = (*classifier).classify(&query);
    *out_resolution = Box::into_raw(Box::new(resolution));
    0
}

/// Free a resolution previously returned by [`ctf_classify`].
///
/// # Safety
///
/// `resolution` MUST be a pointer obtained from [`ctf_classify`], or NULL.
#[no_mangle]
pub unsafe extern "C" fn ctf_resolution_free(resolution: *mut ScopeResolution) {
    if resolution.is_null() {
        return;
    }
    drop(Box::from_raw(resolution));
}

/// Get the execution decision: 0 = Trace, 1 = Skip.
///
/// # Safety
///
/// `resolution` MUST be a valid pointer from [`ctf_classify`].
#[no_mangle]
pub unsafe extern "C" fn ctf_resolution_exec(resolution: *const ScopeResolution) -> c_int {
    match (*resolution).exec() {
        ExecDecision::Trace => 0,
        ExecDecision::Skip => 1,
    }
}

/// Get the default value action: 0 = Allow, 1 = Redact, 2 = Drop.
///
/// # Safety
///
/// `resolution` MUST be a valid pointer from [`ctf_classify`].
#[no_mangle]
pub unsafe extern "C" fn ctf_resolution_default_value_action(resolution: *const ScopeResolution) -> c_int {
    use crate::model::ValueAction;
    match (*resolution).value_policy().default_action() {
        ValueAction::Allow => 0,
        ValueAction::Redact => 1,
        ValueAction::Drop => 2,
    }
}

/// Decide the action for a single value name inside the resolved scope.
/// `kind` uses the encoding from [`ValueKind::index`]: 0=Local, 1=Global,
/// 2=Arg, 3=Return, 4=Attr. Returns -1 on argument error, otherwise the
/// action encoding from [`ctf_resolution_default_value_action`].
///
/// # Safety
///
/// All pointer arguments MUST be valid; `name` MUST be UTF-8.
#[no_mangle]
pub unsafe extern "C" fn ctf_resolution_decide_value(resolution: *const ScopeResolution, kind: c_int, name: *const c_char) -> c_int {
    use crate::model::ValueAction;
    if resolution.is_null() || name.is_null() {
        return -1;
    }
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let value_kind = match kind {
        0 => ValueKind::Local,
        1 => ValueKind::Global,
        2 => ValueKind::Arg,
        3 => ValueKind::Return,
        4 => ValueKind::Attr,
        _ => return -1,
    };
    let action = (*resolution).value_policy().decide(value_kind, name_str);
    match action {
        ValueAction::Allow => 0,
        ValueAction::Redact => 1,
        ValueAction::Drop => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn ffi_round_trip_classifies_and_frees() {
        let temp = tempdir().unwrap();
        let codetracer = temp.path().join(".codetracer");
        fs::create_dir(&codetracer).unwrap();
        let filter = codetracer.join("filter.toml");
        fs::write(
            &filter,
            r#"
[meta]
name = "ffi"
version = 1

[scope]
default_exec = "trace"
default_value_action = "allow"

[[scope.rules]]
selector = "pkg:app.foo"
exec = "skip"
"#,
        )
        .unwrap();

        let filter_str = CString::new(filter.to_string_lossy().to_string()).unwrap();
        let paths: [*const c_char; 1] = [filter_str.as_ptr()];
        let classifier = unsafe { ctf_classifier_new_from_paths(paths.as_ptr(), 1) };
        assert!(!classifier.is_null(), "classifier construction failed");

        let app_file = temp.path().join("app").join("foo.py");
        fs::create_dir_all(app_file.parent().unwrap()).unwrap();
        fs::write(&app_file, "x = 1\n").unwrap();
        let filename = CString::new(app_file.to_string_lossy().to_string()).unwrap();
        let module = CString::new("app.foo").unwrap();

        let mut resolution_ptr: *mut ScopeResolution = ptr::null_mut();
        let rc = unsafe { ctf_classify(classifier, filename.as_ptr(), ptr::null(), module.as_ptr(), &mut resolution_ptr) };
        assert_eq!(rc, 0, "classify returned non-zero");
        assert!(!resolution_ptr.is_null());
        assert_eq!(unsafe { ctf_resolution_exec(resolution_ptr) }, 1, "expected Skip");

        unsafe { ctf_resolution_free(resolution_ptr) };
        unsafe { ctf_classifier_free(classifier) };
    }
}
