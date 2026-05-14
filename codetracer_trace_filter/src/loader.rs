//! Trace filter configuration loader (TOML ingestion, aggregation).
//!
//! Implements the schema from
//! `codetracer-trace-format-spec/Trace-Filters.md` § 4 and the chain
//! composition rules from § 5.

use crate::error::FilterResult;
use crate::model::{
    ExecDirective, FilterMeta, FilterSource, IoConfig, IoStream, ScopeRule, TraceFilterConfig,
    ValueAction, ValuePattern,
};
use crate::selector::{MatchType, Selector, SelectorKind};
use crate::{filter_invalid, filter_io, MAX_SCHEMA_VERSION};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

/// Helper aggregating inline and file sources into a resolved configuration.
#[derive(Default)]
pub struct ConfigAggregator {
    default_exec: Option<ExecDirective>,
    default_value_action: Option<ValueAction>,
    default_value_source: Option<usize>,
    io: Option<IoConfig>,
    rules: Vec<ScopeRule>,
    sources: Vec<FilterSource>,
}

impl ConfigAggregator {
    /// Ingest a filter from the filesystem.
    pub fn ingest_file(&mut self, path: &Path) -> FilterResult<()> {
        let contents = fs::read_to_string(path).map_err(|err| {
            filter_io!("failed to read trace filter '{}': {}", path.display(), err)
        })?;

        self.ingest_source(path, &contents)
    }

    /// Ingest an inline filter (used for builtin defaults).
    pub fn ingest_inline(&mut self, label: &str, contents: &str) -> FilterResult<()> {
        let pseudo_path = PathBuf::from(format!("<inline:{label}>"));
        self.ingest_source(&pseudo_path, contents)
    }

    /// Finalise the aggregation, producing a resolved configuration.
    pub fn finish(self) -> FilterResult<TraceFilterConfig> {
        let default_exec = self.default_exec.ok_or_else(|| {
            filter_invalid!("composed filters never set 'scope.default_exec'")
        })?;
        let default_value_action = self.default_value_action.ok_or_else(|| {
            filter_invalid!("composed filters never set 'scope.default_value_action'")
        })?;
        let default_value_source = self
            .default_value_source
            .ok_or_else(|| filter_invalid!("failed to record source for 'scope.default_value_action'"))?;

        let io = self.io.unwrap_or_default();

        Ok(TraceFilterConfig {
            default_exec,
            default_value_action,
            default_value_source,
            io,
            rules: self.rules,
            sources: self.sources,
        })
    }

    fn ingest_source(&mut self, path: &Path, contents: &str) -> FilterResult<()> {
        let checksum = calculate_sha256(contents);
        let raw: RawFilterFile = toml::from_str(contents).map_err(|err| {
            filter_invalid!(
                "failed to parse trace filter '{}': {}",
                path.display(),
                err
            )
        })?;

        let project_root = detect_project_root(path);
        let source_index = self.sources.len();
        let meta = parse_meta(&raw.meta, path)?;
        self.sources.push(FilterSource {
            path: path.to_path_buf(),
            sha256: checksum,
            project_root: project_root.clone(),
            meta,
        });

        let defaults = resolve_defaults(
            &raw.scope,
            path,
            self.default_exec,
            self.default_value_action,
        )?;
        if let Some(exec) = defaults.exec {
            self.default_exec = Some(exec);
        }
        if let Some(value_action) = defaults.value_action {
            self.default_value_action = Some(value_action);
            self.default_value_source = Some(source_index);
        }

        if let Some(io) = parse_io(raw.io.as_ref(), path)? {
            self.io = Some(io);
        }

        let rules = parse_rules(
            raw.scope.rules.as_deref().unwrap_or_default(),
            path,
            &project_root,
            source_index,
        )?;
        self.rules.extend(rules);

        Ok(())
    }
}

pub fn calculate_sha256(contents: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(contents.as_bytes());
    let digest = hasher.finalize();
    format!("{:x}", digest)
}

pub fn detect_project_root(path: &Path) -> PathBuf {
    let mut current = path.parent();
    while let Some(dir) = current {
        if dir.file_name().and_then(|name| name.to_str()) == Some(".codetracer") {
            return dir
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| dir.to_path_buf());
        }
        current = dir.parent();
    }
    path.parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn parse_meta(raw: &RawMeta, path: &Path) -> FilterResult<FilterMeta> {
    if raw.name.trim().is_empty() {
        return Err(filter_invalid!(
            "'meta.name' must not be empty in '{}'",
            path.display()
        ));
    }

    if raw.version < 1 {
        return Err(filter_invalid!(
            "'meta.version' must be >= 1 in '{}'",
            path.display()
        ));
    }

    // Spec § 11: refuse to load filter files whose schema version exceeds
    // what this crate understands.  Forward-compat: a future v2 schema is
    // rejected here rather than misinterpreted under v1 semantics.
    if raw.version as u32 > MAX_SCHEMA_VERSION {
        return Err(crate::error::FilterError::new(
            crate::error::ErrorCode::UnsupportedSchemaVersion,
            format!(
                "trace filter '{}' declares 'meta.version = {}' but this recorder supports up to {} (per spec § 11)",
                path.display(),
                raw.version,
                MAX_SCHEMA_VERSION
            ),
        ));
    }

    let mut labels = Vec::new();
    let mut seen = HashSet::new();
    for label in &raw.labels {
        if seen.insert(label) {
            labels.push(label.clone());
        }
    }

    Ok(FilterMeta {
        name: raw.name.clone(),
        version: raw.version as u32,
        description: raw.description.clone(),
        labels,
    })
}

pub struct ResolvedDefaults {
    pub exec: Option<ExecDirective>,
    pub value_action: Option<ValueAction>,
}

pub fn resolve_defaults(
    scope: &RawScope,
    path: &Path,
    current_exec: Option<ExecDirective>,
    current_value_action: Option<ValueAction>,
) -> FilterResult<ResolvedDefaults> {
    let exec = parse_default_exec(&scope.default_exec, path, current_exec)?;
    let value_action =
        parse_default_value_action(&scope.default_value_action, path, current_value_action)?;
    Ok(ResolvedDefaults { exec, value_action })
}

pub fn parse_default_exec(
    token: &str,
    path: &Path,
    current_exec: Option<ExecDirective>,
) -> FilterResult<Option<ExecDirective>> {
    match token {
        "inherit" => {
            if current_exec.is_none() {
                return Err(filter_invalid!(
                    "'scope.default_exec' in '{}' cannot inherit without a previous filter",
                    path.display()
                ));
            }
            Ok(None)
        }
        _ => ExecDirective::parse(token)
            .ok_or_else(|| {
                filter_invalid!(
                    "unsupported value '{}' for 'scope.default_exec' in '{}'",
                    token,
                    path.display()
                )
            })
            .map(Some),
    }
}

pub fn parse_default_value_action(
    token: &str,
    path: &Path,
    current_value_action: Option<ValueAction>,
) -> FilterResult<Option<ValueAction>> {
    match token {
        "inherit" => {
            if current_value_action.is_none() {
                return Err(filter_invalid!(
                    "'scope.default_value_action' in '{}' cannot inherit without a previous filter",
                    path.display()
                ));
            }
            Ok(None)
        }
        _ => ValueAction::parse(token)
            .ok_or_else(|| {
                filter_invalid!(
                    "unsupported value '{}' for 'scope.default_value_action' in '{}'",
                    token,
                    path.display()
                )
            })
            .map(Some),
    }
}

pub fn parse_io(raw: Option<&RawIo>, path: &Path) -> FilterResult<Option<IoConfig>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let capture = raw.capture.unwrap_or(false);
    let streams = match raw.streams.as_ref() {
        Some(values) => {
            let mut parsed = Vec::new();
            let mut seen = HashSet::new();
            for value in values {
                let stream = IoStream::parse(value).ok_or_else(|| {
                    filter_invalid!(
                        "unsupported IO stream '{}' in '{}'",
                        value,
                        path.display()
                    )
                })?;
                if seen.insert(stream) {
                    parsed.push(stream);
                }
            }
            parsed
        }
        None => Vec::new(),
    };

    if capture && streams.is_empty() {
        return Err(filter_invalid!(
            "'io.streams' must be provided when 'io.capture = true' in '{}'",
            path.display()
        ));
    }
    if let Some(modes) = raw.modes.as_ref() {
        if !modes.is_empty() {
            return Err(filter_invalid!(
                "'io.modes' is reserved and must be empty in '{}'",
                path.display()
            ));
        }
    }

    Ok(Some(IoConfig { capture, streams }))
}

pub fn parse_rules(
    raw_rules: &[RawScopeRule],
    path: &Path,
    project_root: &Path,
    source_id: usize,
) -> FilterResult<Vec<ScopeRule>> {
    let mut rules = Vec::new();
    for (idx, raw_rule) in raw_rules.iter().enumerate() {
        let location = format!("{} scope.rules[{}]", path.display(), idx);
        let selector =
            Selector::parse(&raw_rule.selector, &SCOPE_SELECTOR_KINDS).map_err(|err| {
                filter_invalid!("invalid scope selector in {}: {}", location, err)
            })?;
        let selector = normalize_scope_selector(selector, project_root, &location)?;

        let exec = match raw_rule.exec.as_deref() {
            None | Some("inherit") => None,
            Some(value) => Some(ExecDirective::parse(value).ok_or_else(|| {
                filter_invalid!(
                    "unsupported value '{}' for 'exec' in {}",
                    value,
                    location
                )
            })?),
        };

        let value_default = match raw_rule.value_default.as_deref() {
            None | Some("inherit") => None,
            Some(value) => Some(ValueAction::parse(value).ok_or_else(|| {
                filter_invalid!(
                    "unsupported value '{}' for 'value_default' in {}",
                    value,
                    location
                )
            })?),
        };

        let mut value_patterns = Vec::new();
        if let Some(patterns) = raw_rule.value_patterns.as_ref() {
            for (pidx, pattern) in patterns.iter().enumerate() {
                let pattern_location = format!("{} value_patterns[{}]", location, pidx);
                let selector =
                    Selector::parse(&pattern.selector, &VALUE_SELECTOR_KINDS).map_err(|err| {
                        filter_invalid!(
                            "invalid value selector in {}: {}",
                            pattern_location,
                            err
                        )
                    })?;

                let action = ValueAction::parse(&pattern.action).ok_or_else(|| {
                    filter_invalid!(
                        "unsupported value '{}' for 'action' in {}",
                        pattern.action,
                        pattern_location
                    )
                })?;

                value_patterns.push(ValuePattern {
                    selector,
                    action,
                    reason: pattern.reason.clone(),
                    source_id,
                });
            }
        }

        rules.push(ScopeRule {
            selector,
            exec,
            value_default,
            value_patterns,
            reason: raw_rule.reason.clone(),
            source_id,
        });
    }
    Ok(rules)
}

pub fn normalize_scope_selector(
    selector: Selector,
    project_root: &Path,
    location: &str,
) -> FilterResult<Selector> {
    match selector.kind() {
        SelectorKind::File => {
            let pattern = selector.pattern();
            if pattern.starts_with("glob:") {
                let glob_pattern = &pattern["glob:".len()..];
                let normalized = normalize_glob_pattern(glob_pattern, project_root)?;
                rebuild_selector(selector.kind(), selector.match_type(), &normalized)
            } else {
                let path = Path::new(pattern);
                let normalized = normalize_file_selector(path, project_root, pattern, location)?;
                rebuild_selector(selector.kind(), selector.match_type(), &normalized)
            }
        }
        _ => Ok(selector),
    }
}

pub fn normalize_file_selector(
    path: &Path,
    project_root: &Path,
    pattern: &str,
    location: &str,
) -> FilterResult<String> {
    let path = if path.is_absolute() {
        path.strip_prefix(project_root)
            .map_err(|_| {
                filter_invalid!(
                    "file selector '{}' in {} must reside within project root '{}'",
                    pattern,
                    location,
                    project_root.display()
                )
            })?
            .to_path_buf()
    } else {
        path.to_path_buf()
    };

    let normalized = normalize_components(&path, pattern, location)?;
    Ok(pathbuf_to_posix(&normalized))
}

pub fn normalize_components(
    path: &Path,
    raw: &str,
    location: &str,
) -> FilterResult<PathBuf> {
    let mut normalised = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => continue,
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalised.pop() {
                    return Err(filter_invalid!(
                        "file selector '{}' in {} escapes the project root",
                        raw,
                        location
                    ));
                }
            }
            Component::Normal(part) => normalised.push(part),
        }
    }
    Ok(normalised)
}

pub fn normalize_glob_pattern(pattern: &str, project_root: &Path) -> FilterResult<String> {
    let mut replaced = pattern.replace('\\', "/");
    while replaced.starts_with("./") {
        replaced = replaced[2..].to_string();
    }

    let trimmed = replaced.trim_start_matches('/');
    let root = pathbuf_to_posix(project_root);
    if root.is_empty() {
        return Ok(trimmed.to_string());
    }

    let root_with_slash = format!("{}/", root);
    if trimmed.starts_with(&root_with_slash) {
        Ok(trimmed[root_with_slash.len()..].to_string())
    } else if trimmed == root {
        Ok(String::new())
    } else {
        Ok(trimmed.to_string())
    }
}

pub fn pathbuf_to_posix(path: &Path) -> String {
    let mut parts = Vec::new();
    for component in path.components() {
        if let Component::Normal(part) = component {
            parts.push(part.to_string_lossy());
        }
    }
    parts.join("/")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawFilterFile {
    pub meta: RawMeta,
    #[serde(default)]
    pub io: Option<RawIo>,
    pub scope: RawScope,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMeta {
    pub name: String,
    pub version: u32,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawIo {
    #[serde(default)]
    pub capture: Option<bool>,
    #[serde(default)]
    pub streams: Option<Vec<String>>,
    #[serde(default)]
    pub modes: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawScope {
    pub default_exec: String,
    pub default_value_action: String,
    #[serde(default)]
    pub rules: Option<Vec<RawScopeRule>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawScopeRule {
    pub selector: String,
    #[serde(default)]
    pub exec: Option<String>,
    #[serde(default)]
    pub value_default: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub value_patterns: Option<Vec<RawValuePattern>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawValuePattern {
    pub selector: String,
    pub action: String,
    #[serde(default)]
    pub reason: Option<String>,
}

const SCOPE_SELECTOR_KINDS: [SelectorKind; 3] = [
    SelectorKind::Package,
    SelectorKind::File,
    SelectorKind::Object,
];
const VALUE_SELECTOR_KINDS: [SelectorKind; 5] = [
    SelectorKind::Local,
    SelectorKind::Global,
    SelectorKind::Arg,
    SelectorKind::Return,
    SelectorKind::Attr,
];

fn rebuild_selector(
    kind: SelectorKind,
    match_type: MatchType,
    pattern: &str,
) -> FilterResult<Selector> {
    let raw = match match_type {
        MatchType::Glob => format!("{}:{}", kind.token(), pattern),
        MatchType::Regex => format!("{}:regex:{}", kind.token(), pattern),
        MatchType::Literal => format!("{}:literal:{}", kind.token(), pattern),
    };
    Selector::parse(&raw, &[kind])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use tempfile::tempdir;

    fn write_filter(body: &str) -> (tempfile::TempDir, PathBuf) {
        let temp = tempdir().expect("temp dir");
        let codetracer_dir = temp.path().join(".codetracer");
        fs::create_dir(&codetracer_dir).expect("create .codetracer");
        let path = codetracer_dir.join("filter.toml");
        fs::write(&path, body).expect("write filter");
        (temp, path)
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let body = r#"
[meta]
name = "future"
version = 2

[scope]
default_exec = "trace"
default_value_action = "allow"
"#;
        let (_tmp, path) = write_filter(body);
        let mut aggregator = ConfigAggregator::default();
        let err = aggregator
            .ingest_file(&path)
            .expect_err("v2 schema must be rejected");
        assert_eq!(err.code, ErrorCode::UnsupportedSchemaVersion);
    }

    #[test]
    fn accepts_v1_schema() {
        let body = r#"
[meta]
name = "ok"
version = 1

[scope]
default_exec = "trace"
default_value_action = "allow"
"#;
        let (_tmp, path) = write_filter(body);
        let mut aggregator = ConfigAggregator::default();
        aggregator.ingest_file(&path).expect("v1 should load");
        let config = aggregator.finish().expect("config");
        assert_eq!(config.default_exec(), ExecDirective::Trace);
        assert_eq!(config.default_value_action(), ValueAction::Allow);
    }

    #[test]
    fn rejects_unknown_action_name() {
        let body = r#"
[meta]
name = "bad"
version = 1

[scope]
default_exec = "trace"
default_value_action = "allow"

[[scope.rules]]
selector = "pkg:foo"
exec = "skipp"
"#;
        let (_tmp, path) = write_filter(body);
        let mut aggregator = ConfigAggregator::default();
        let err = aggregator
            .ingest_file(&path)
            .expect_err("unknown action must be rejected");
        assert_eq!(err.code, ErrorCode::InvalidPolicyValue);
    }
}
