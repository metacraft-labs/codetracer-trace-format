//! Integration tests for the §P7.4 `infer-llm` LLM-based inference
//! path.
//!
//! Each test exercises one specific contract from the §P7.4 spec:
//!
//! * `no_api_key_yields_skip_loud_message` — invokes the binary in a
//!   subprocess with both env vars unset, asserts the loud `SKIP`
//!   message goes to stdout and the exit code is 0.
//! * `infer_llm_with_mock_server` — points the library at a tiny
//!   hand-written TCP mock server returning a canned Messages API
//!   response, asserts the produced renames match what the mock
//!   returned.
//! * `malformed_json_response_errors` — mock returns text without a
//!   parseable JSON block; assert the library returns
//!   `InferLlmError::JsonParseError`.
//! * `http_error_propagates` — mock returns a 401 + error body; assert
//!   the library returns `InferLlmError::HttpError`.
//! * `min_confidence_filters` — mock returns three renames at
//!   confidences 0.9 / 0.6 / 0.3, threshold 0.7 → only the 0.9 one
//!   survives.
//! * `real_api_call_on_lodash_snippet` — skip-loud when no real API
//!   key is present; otherwise hit the live API and assert at least
//!   one rename came back.
//!
//! The mock server is a hand-written single-shot `std::net::TcpListener`
//! (~50 lines) rather than `wiremock` so we don't drag a tokio runtime
//! into this crate's dev-deps just for tests.  It accepts a single
//! request and responds with a fixed payload, then exits.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use ct_mapping_tools::{InferLlmError, InferLlmOptions, Language, infer_llm};

/// Tiny single-shot HTTP mock server.
///
/// Spawns a thread bound to an OS-assigned port, accepts one
/// connection, reads the entire request up to `\r\n\r\n`, then writes
/// `response_body` with the supplied `status_line` and a
/// `Content-Length` header.
///
/// Returns the base URL (e.g. `http://127.0.0.1:34567`) the test
/// should pass as `--api-base` so the library's `<api_base>/messages`
/// concatenation targets the mock.  Strips the trailing `/v1`-style
/// path bit (the library appends `/messages` directly).
fn spawn_mock(status_line: &'static str, response_body: &'static str) -> (String, Arc<AtomicBool>) {
    spawn_mock_with_handler(status_line, response_body, Arc::new(AtomicBool::new(false)))
}

fn spawn_mock_with_handler(
    status_line: &'static str,
    response_body: &'static str,
    saw_request: Arc<AtomicBool>,
) -> (String, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
    let port = listener.local_addr().expect("local_addr").port();
    let saw_request_clone = saw_request.clone();
    thread::spawn(move || {
        // We accept just one connection — the library makes exactly
        // one HTTP request per `infer_llm` call in v1.
        let (mut stream, _) = match listener.accept() {
            Ok(c) => c,
            Err(_) => return,
        };
        // Read the full HTTP request.  We don't bother parsing it
        // (the test asserts on the response, not the request shape) —
        // we just need to consume it so the client's write completes.
        let mut buf = [0u8; 8192];
        let mut total = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    total.extend_from_slice(&buf[..n]);
                    // Heuristic: HTTP request is complete once we've
                    // seen the header terminator AND consumed at
                    // least Content-Length bytes after it.  For the
                    // tests' tiny payloads, the request typically
                    // arrives in a single read; we break on the first
                    // `\r\n\r\n` we see and trust the body is whole.
                    if let Some(hdr_end) = total.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header_block = std::str::from_utf8(&total[..hdr_end])
                            .unwrap_or("");
                        let body_start = hdr_end + 4;
                        let content_length = header_block
                            .lines()
                            .find_map(|l| {
                                let l = l.to_ascii_lowercase();
                                let stripped = l.strip_prefix("content-length:")?;
                                stripped.trim().parse::<usize>().ok()
                            })
                            .unwrap_or(0);
                        if total.len() >= body_start + content_length {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        saw_request_clone.store(true, Ordering::SeqCst);
        let response = format!(
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
            status_line = status_line,
            len = response_body.len(),
            body = response_body
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    });
    (format!("http://127.0.0.1:{port}"), saw_request)
}

#[test]
fn no_api_key_yields_skip_loud_message() {
    // Spawn the binary as a subprocess with both env vars unset and
    // assert the CLI prints the loud `SKIP` line + exits 0.  Uses
    // `CARGO_BIN_EXE_<binary>` (cargo sets this for integration
    // tests) so we don't rely on `target/debug/...` path layout.
    let bin = env!("CARGO_BIN_EXE_ct-mapping-tools");
    let tmp = std::env::temp_dir().join("p74-no-key-min.js");
    std::fs::write(&tmp, "function a(b){return b;}").expect("write tmp");

    let output = std::process::Command::new(bin)
        .args(["infer-llm", tmp.to_str().unwrap()])
        .env_remove("CT_LLM_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .output()
        .expect("spawn ct-mapping-tools");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "expected exit 0 when no API key; got status {:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("SKIP infer-llm"),
        "expected `SKIP infer-llm` in stdout; got: {stdout}"
    );
    assert!(
        stdout.contains("CT_LLM_API_KEY") || stdout.contains("ANTHROPIC_API_KEY"),
        "expected env-var hint in skip message; got: {stdout}"
    );
}

#[test]
fn infer_llm_with_mock_server() {
    // Canned Anthropic Messages API response — one assistant text
    // block whose content is a JSON code fence with three renames.
    // Each rename has a self-rated confidence; the default threshold
    // (0.5) keeps the two strong ones, drops the 0.3.
    let body = r#"{
      "content": [
        {
          "type": "text",
          "text": "```json\n{\"renames\": [{\"from\": \"a\", \"to\": \"userId\", \"confidence\": 0.9, \"reasoning\": \"passed to authenticate\"}, {\"from\": \"b\", \"to\": \"callback\", \"confidence\": 0.75, \"reasoning\": \"invoked as a function\"}]}\n```"
        }
      ],
      "usage": {"input_tokens": 123, "output_tokens": 45}
    }"#;
    let (base, _) = spawn_mock("HTTP/1.1 200 OK", Box::leak(body.to_string().into_boxed_str()));

    let opts = InferLlmOptions {
        language: Language::JavaScript,
        file_name: Some("test.min.js".to_string()),
        model: "claude-haiku-4-5-20251001".to_string(),
        api_base: base,
        min_confidence: 0.5,
        max_bindings: 50,
    };
    let result = infer_llm("function a(b){return b();}", "fake-key", &opts)
        .expect("mock server call should succeed");

    let pairs: HashSet<(String, String)> = result
        .entries
        .iter()
        .map(|e| (e.entry.from.clone(), e.entry.to.clone()))
        .collect();
    let expected: HashSet<(String, String)> = [
        ("a".to_string(), "userId".to_string()),
        ("b".to_string(), "callback".to_string()),
    ]
    .into_iter()
    .collect();
    assert_eq!(pairs, expected, "mock-returned renames pass through");

    // Field-level checks: file + scope propagate to every row.
    for r in &result.entries {
        assert_eq!(r.entry.file, "test.min.js");
        assert_eq!(r.entry.scope, "global");
        assert!(r.confidence >= 0.5);
    }

    // Stats: 2 proposals, both survive the 0.5 default threshold,
    // usage tokens propagate from the mock's `usage` block.
    assert_eq!(result.stats.api_call_count, 1);
    assert_eq!(result.stats.bindings_proposed, 2);
    assert_eq!(result.stats.bindings_above_confidence, 2);
    assert_eq!(result.stats.total_tokens_in, 123);
    assert_eq!(result.stats.total_tokens_out, 45);
}

#[test]
fn malformed_json_response_errors() {
    // The mock returns a syntactically valid Messages API envelope
    // whose assistant text is plain prose — no JSON fence, no
    // parseable JSON.  The library must surface
    // `InferLlmError::JsonParseError` (NOT a panic, NOT a silent
    // empty result).
    let body = r#"{
      "content": [
        {"type": "text", "text": "Sorry, I cannot propose renames for this source."}
      ]
    }"#;
    let (base, _) = spawn_mock("HTTP/1.1 200 OK", Box::leak(body.to_string().into_boxed_str()));

    let opts = InferLlmOptions {
        api_base: base,
        ..InferLlmOptions::default()
    };
    let err = infer_llm("function a(){}", "fake-key", &opts).unwrap_err();
    match err {
        InferLlmError::JsonParseError(_) => {}
        other => panic!("expected JsonParseError, got {other:?}"),
    }
}

#[test]
fn http_error_propagates() {
    // Mock returns 401 Unauthorized with an error body.  The library
    // must turn this into `InferLlmError::HttpError` (NOT a
    // JsonParseError on the error body).
    let body = r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#;
    let (base, _) =
        spawn_mock("HTTP/1.1 401 Unauthorized", Box::leak(body.to_string().into_boxed_str()));

    let opts = InferLlmOptions {
        api_base: base,
        ..InferLlmOptions::default()
    };
    let err = infer_llm("function a(){}", "fake-key", &opts).unwrap_err();
    match err {
        InferLlmError::HttpError(msg) => {
            assert!(
                msg.contains("401"),
                "expected HTTP error message to mention the status code; got: {msg}"
            );
        }
        other => panic!("expected HttpError, got {other:?}"),
    }
}

#[test]
fn min_confidence_filters() {
    // Mock returns three renames with confidences 0.9 / 0.6 / 0.3.
    // With `min_confidence = 0.7` only the 0.9 row survives.  Locks
    // in the documented filtering contract.
    let body = r#"{
      "content": [
        {
          "type": "text",
          "text": "```json\n{\"renames\": [{\"from\": \"a\", \"to\": \"highConf\", \"confidence\": 0.9}, {\"from\": \"b\", \"to\": \"midConf\", \"confidence\": 0.6}, {\"from\": \"c\", \"to\": \"lowConf\", \"confidence\": 0.3}]}\n```"
        }
      ]
    }"#;
    let (base, _) = spawn_mock("HTTP/1.1 200 OK", Box::leak(body.to_string().into_boxed_str()));

    let opts = InferLlmOptions {
        api_base: base,
        min_confidence: 0.7,
        ..InferLlmOptions::default()
    };
    let result = infer_llm("function a(b,c){}", "fake-key", &opts).expect("mock call");

    let pairs: HashSet<(String, String)> = result
        .entries
        .iter()
        .map(|e| (e.entry.from.clone(), e.entry.to.clone()))
        .collect();
    let expected: HashSet<(String, String)> =
        [("a".to_string(), "highConf".to_string())].into_iter().collect();
    assert_eq!(
        pairs, expected,
        "only the high-confidence rename survives the 0.7 threshold"
    );

    // Stats: all three rows count as `proposed`, only one as `above_confidence`.
    assert_eq!(result.stats.bindings_proposed, 3);
    assert_eq!(result.stats.bindings_above_confidence, 1);
}

/// Hit the real Anthropic API when a key is configured; skip-loud
/// otherwise.  Keeps the test suite green in environments without
/// network or credentials while exercising the happy path when run
/// locally with a key in the env.
///
/// We don't assert on specific names — the model's output is
/// non-deterministic — only that at least one rename came back and
/// the call didn't error.
#[test]
fn real_api_call_on_lodash_snippet() {
    let api_key = std::env::var("CT_LLM_API_KEY")
        .ok()
        .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
        .filter(|k| !k.is_empty());
    let Some(api_key) = api_key else {
        eprintln!("SKIP real_api_call_on_lodash_snippet: no API key in env");
        return;
    };

    let minified = "function a(b,c){return b.map(function(d){return c(d);});}";
    let opts = InferLlmOptions {
        language: Language::JavaScript,
        file_name: Some("lodash-snippet.min.js".to_string()),
        ..InferLlmOptions::default()
    };
    let result = infer_llm(minified, &api_key, &opts).expect("live API call");
    assert!(
        !result.entries.is_empty(),
        "live model should have proposed at least one rename; got stats={:?}",
        result.stats
    );
    assert_eq!(result.stats.api_call_count, 1);
}
