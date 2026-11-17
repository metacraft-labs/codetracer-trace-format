//! Match the foreign archive to Cargo's existing MSVC C-runtime context.
//! https://doc.rust-lang.org/reference/linkage.html#static-and-dynamic-c-runtimes
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

pub fn compiler_argument(msvc: bool, target_features: &str) -> Option<&'static str> {
    if !msvc {
        return None;
    }
    if target_features.split(',').any(|feature| feature == "crt-static") {
        Some("--passC:/MT")
    } else {
        Some("--passC:/MD")
    }
}

pub fn cache_identity(out_dir: &Path, source_hash: u64, runtime: Option<&str>) -> u64 {
    let mut hasher = DefaultHasher::new();
    out_dir.hash(&mut hasher);
    source_hash.hash(&mut hasher);
    // Leave GNU/POSIX's existing identity unchanged. MSVC objects from the
    // other runtime mode must never be reused, even for the same build unit.
    if let Some(argument) = runtime {
        argument.hash(&mut hasher);
    }
    hasher.finish()
}
