//! Keep generated C paths short enough for the native compiler.
use std::path::{Path, PathBuf};

// The existing short-cache design protects the C compiler's legacy 260-unit
// path handling. A 100-unit directory leaves 159 units for a separator and
// Nim's generated C filename (plus the terminating NUL). This is deliberately
// conservative, rather than a claim that longer paths always fail.
pub const MAX_CACHE_PATH: usize = 100;

fn path_units(path: &Path) -> usize {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        path.as_os_str().encode_wide().count()
    }
    #[cfg(not(windows))]
    {
        path.as_os_str().as_encoded_bytes().len()
    }
}

pub fn select(temp: &Path, fallback: &Path, key: u64) -> (PathBuf, bool) {
    let cache = |base: &Path| {
        // A relative TMPDIR still expands underneath the current directory;
        // measuring only its spelling would let a deep checkout evade the cap.
        let base = if base.is_absolute() {
            base.to_path_buf()
        } else {
            std::env::current_dir().expect("cannot resolve relative temporary directory").join(base)
        };
        base.join("ctnw").join(format!("{key:016x}"))
    };
    let preferred = cache(temp);
    if path_units(&preferred) <= MAX_CACHE_PATH {
        return (preferred, false);
    }
    let fallback = cache(fallback);
    assert!(
        path_units(&fallback) <= MAX_CACHE_PATH,
        "Nim FFI nimcache fallback {} exceeds the {}-unit path budget; set a shorter temporary directory",
        fallback.display(),
        MAX_CACHE_PATH,
    );
    (fallback, true)
}
