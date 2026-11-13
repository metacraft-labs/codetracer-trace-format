//! Exercise the production target-context selector and native cache key.
//! No compiler/provider is mocked; native linking is covered by the existing
//! archive test in both real Cargo runtime contexts (tests/msvc_runtime.ps1).
#[path = "../msvc_runtime.rs"]
mod msvc_runtime;
use std::path::Path;

#[test]
fn default_msvc_context_uses_the_dll_runtime() {
    assert_eq!(msvc_runtime::compiler_argument(true, "fxsr,sse,sse2"), Some("--passC:/MD"));
    assert_eq!(msvc_runtime::compiler_argument(true, ""), Some("--passC:/MD"));
}

#[test]
fn static_runtime_is_an_exact_feature_token() {
    for features in ["crt-static", "crt-static,sse2", "sse2,crt-static", "sse2,crt-static,fxsr"] {
        assert_eq!(msvc_runtime::compiler_argument(true, features), Some("--passC:/MT"));
    }
    for features in ["not-crt-static", "crt-static-extra", "crt-static=off"] {
        assert_eq!(msvc_runtime::compiler_argument(true, features), Some("--passC:/MD"));
    }
}

#[test]
fn other_abis_do_not_receive_msvc_flags() {
    assert_eq!(msvc_runtime::compiler_argument(false, "crt-static"), None);
    assert_eq!(msvc_runtime::compiler_argument(false, ""), None);
}

#[test]
fn runtime_modes_do_not_share_native_cache_identity() {
    let out = Path::new("same-build-unit");
    let dll = msvc_runtime::cache_identity(out, 17, Some("--passC:/MD"));
    let static_crt = msvc_runtime::cache_identity(out, 17, Some("--passC:/MT"));
    assert_ne!(dll, static_crt);
    assert_eq!(dll, msvc_runtime::cache_identity(out, 17, Some("--passC:/MD")));
    assert_ne!(dll, msvc_runtime::cache_identity(out, 18, Some("--passC:/MD")));
    assert_ne!(dll, msvc_runtime::cache_identity(Path::new("other-unit"), 17, Some("--passC:/MD")));
}
