//! Runtime classifier evaluating scope selectors and value policies.
//!
//! The engine is **pure**: it consumes a [`TraceFilterConfig`] plus a
//! [`ScopeQuery`] describing the scope being classified and returns a
//! [`ScopeResolution`]. It owns no per-scope cache — that responsibility
//! belongs to the recorder, which MUST stash the decision in the host
//! runtime's native per-scope slot (Python `co_extra`, Nim VM
//! `seq[PathId]` indexed by `FileIndex.int32`, etc.) per spec § 6.
//!
//! Keeping the engine free of host-runtime types means this crate compiles
//! anywhere and can be FFI'd into any recorder.

use crate::config::{ExecDirective, ScopeRule, TraceFilterConfig, ValueAction, ValuePattern};
use crate::model::FilterSummary;
use crate::selector::{Selector, SelectorKind};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

/// Final execution decision emitted by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecDecision {
    Trace,
    Skip,
}

impl From<ExecDirective> for ExecDecision {
    fn from(value: ExecDirective) -> Self {
        match value {
            ExecDirective::Trace => ExecDecision::Trace,
            ExecDirective::Skip => ExecDecision::Skip,
        }
    }
}

/// Kind of value inspected while deciding redaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Local,
    Global,
    Arg,
    Return,
    Attr,
}

impl ValueKind {
    fn selector_kind(self) -> SelectorKind {
        match self {
            ValueKind::Local => SelectorKind::Local,
            ValueKind::Global => SelectorKind::Global,
            ValueKind::Arg => SelectorKind::Arg,
            ValueKind::Return => SelectorKind::Return,
            ValueKind::Attr => SelectorKind::Attr,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            ValueKind::Local => "local",
            ValueKind::Global => "global",
            ValueKind::Arg => "argument",
            ValueKind::Return => "return",
            ValueKind::Attr => "attribute",
        }
    }

    pub fn index(self) -> usize {
        match self {
            ValueKind::Local => 0,
            ValueKind::Global => 1,
            ValueKind::Arg => 2,
            ValueKind::Return => 3,
            ValueKind::Attr => 4,
        }
    }

    pub const ALL: [ValueKind; 5] = [ValueKind::Local, ValueKind::Global, ValueKind::Arg, ValueKind::Return, ValueKind::Attr];
}

/// Value redaction policy resolved for a scope.
#[derive(Debug, Clone)]
pub struct ValuePolicy {
    default_action: ValueAction,
    patterns: Arc<[CompiledValuePattern]>,
}

impl ValuePolicy {
    fn new(default_action: ValueAction, patterns: Arc<[CompiledValuePattern]>) -> Self {
        ValuePolicy { default_action, patterns }
    }

    /// Default action applied when no selector matches.
    pub fn default_action(&self) -> ValueAction {
        self.default_action
    }

    /// Evaluate the policy for a value of `kind` with identifier `name`.
    pub fn decide(&self, kind: ValueKind, name: &str) -> ValueAction {
        let selector_kind = kind.selector_kind();
        for pattern in self.patterns.iter() {
            if pattern.selector.kind() == selector_kind && pattern.selector.matches(name) {
                return pattern.action;
            }
        }
        self.default_action
    }

    /// Expose rule metadata for debugging or telemetry.
    pub fn patterns(&self) -> &[CompiledValuePattern] {
        &self.patterns
    }
}

/// Query passed to [`Classifier::classify`].
///
/// Recorders fill in whichever fields the host runtime can supply cheaply:
/// * `filename` is the absolute path reported by the host runtime
///   (Python `co_filename`, Nim `FileIndex` source path, etc.).
/// * `qualname` is the qualified scope name within the file (Python
///   `co_qualname`, Nim proc name, etc.).
/// * `module_hint` is an optional module name (e.g. derived from
///   `sys.modules` or `__package__` for Python).  When provided the
///   classifier prefers it over filename-based derivation.
///
/// All fields except `filename` are optional; the classifier degrades
/// gracefully when they are missing.
#[derive(Debug, Clone, Default)]
pub struct ScopeQuery<'a> {
    pub filename: &'a str,
    pub qualname: Option<&'a str>,
    pub module_hint: Option<&'a str>,
}

impl<'a> ScopeQuery<'a> {
    pub fn new(filename: &'a str) -> Self {
        ScopeQuery {
            filename,
            qualname: None,
            module_hint: None,
        }
    }

    pub fn with_qualname(mut self, qualname: &'a str) -> Self {
        self.qualname = Some(qualname);
        self
    }

    pub fn with_module_hint(mut self, hint: &'a str) -> Self {
        self.module_hint = Some(hint);
        self
    }
}

/// Resolution emitted by the engine for a given scope.
#[derive(Debug, Clone)]
pub struct ScopeResolution {
    exec: ExecDecision,
    value_policy: Arc<ValuePolicy>,
    module_name: Option<String>,
    object_name: Option<String>,
    relative_path: Option<String>,
    absolute_path: Option<String>,
    matched_rule_index: Option<usize>,
    matched_rule_source: Option<usize>,
    matched_rule_reason: Option<String>,
}

impl ScopeResolution {
    /// Execution decision (trace vs skip).
    pub fn exec(&self) -> ExecDecision {
        self.exec
    }

    /// Value redaction policy derived for this scope.
    pub fn value_policy(&self) -> &ValuePolicy {
        &self.value_policy
    }

    /// Module name derived from the code object's filename (if any).
    pub fn module_name(&self) -> Option<&str> {
        self.module_name.as_deref()
    }

    /// Qualified object identifier (module + qualname when available).
    pub fn object_name(&self) -> Option<&str> {
        self.object_name.as_deref()
    }

    /// Project-relative POSIX path for the file containing the code object.
    pub fn relative_path(&self) -> Option<&str> {
        self.relative_path.as_deref()
    }

    /// Absolute POSIX path for the file containing the code object.
    pub fn absolute_path(&self) -> Option<&str> {
        self.absolute_path.as_deref()
    }

    /// Index within the flattened rule list that last matched this scope.
    pub fn matched_rule_index(&self) -> Option<usize> {
        self.matched_rule_index
    }

    /// Source identifier (filter file index) of the last matched rule.
    pub fn matched_rule_source(&self) -> Option<usize> {
        self.matched_rule_source
    }

    /// Reason string attached to the last matched rule, if present.
    pub fn matched_rule_reason(&self) -> Option<&str> {
        self.matched_rule_reason.as_deref()
    }
}

/// Pure classifier wrapping a compiled filter configuration.
///
/// Recorders typically wrap a [`Classifier`] in their own per-scope cache
/// (Python: `co_extra`; Nim VM: `seq[PathId]`; etc.) so the hot path stays
/// at one indirection per event per spec § 6.
pub struct Classifier {
    config: Arc<TraceFilterConfig>,
    default_exec: ExecDecision,
    default_value_action: ValueAction,
    default_value_source: usize,
    rules: Arc<[CompiledScopeRule]>,
}

impl Classifier {
    /// Construct the classifier from a fully resolved configuration.
    pub fn new(config: TraceFilterConfig) -> Self {
        let default_exec = config.default_exec().into();
        let default_value_action = config.default_value_action();
        let default_value_source = config.default_value_source();
        let rules = compile_rules(config.rules());

        Classifier {
            config: Arc::new(config),
            default_exec,
            default_value_action,
            default_value_source,
            rules,
        }
    }

    /// Convenience constructor: build the classifier from a single
    /// [`TraceFilterConfig`] wrapped in an [`Arc`] that may be shared.
    pub fn from_arc(config: Arc<TraceFilterConfig>) -> Self {
        Classifier::new((*config).clone())
    }

    /// Borrow the underlying compiled configuration.
    pub fn config(&self) -> &TraceFilterConfig {
        &self.config
    }

    /// Classify a single scope.  Pure; no IO, no shared mutable state.
    pub fn classify(&self, query: &ScopeQuery<'_>) -> ScopeResolution {
        let mut context = ScopeContext::derive(query.filename, self.config.sources());
        if let Some(qualname) = query.qualname {
            context.refresh_object_name(qualname);
        }

        // A caller-supplied module hint (e.g. Python `__module__` looked up
        // in `sys.modules`) wins over filename-based derivation whenever it
        // looks like a real dotted identifier.
        if let Some(hint) = query.module_hint {
            if is_valid_module_name(hint) {
                context.module_name = Some(hint.to_string());
                if let Some(qualname) = query.qualname {
                    context.refresh_object_name(qualname);
                }
            }
        }

        let mut exec = self.default_exec;
        let mut value_default = self.default_value_action;
        let mut value_default_source = self.default_value_source;
        let mut patterns: Arc<[CompiledValuePattern]> = Arc::from(Vec::new());
        let mut matched_rule_index = None;
        let mut matched_rule_source = context.source_id;
        let mut matched_rule_reason = None;

        for rule in self.rules.iter() {
            if rule.matches(&context) {
                if let Some(rule_exec) = rule.exec {
                    exec = rule_exec;
                }
                if let Some(rule_value) = rule.value_default {
                    value_default = rule_value;
                    value_default_source = rule.source_id;
                }
                patterns = rule.value_patterns.clone();
                matched_rule_index = Some(rule.index);
                matched_rule_source = Some(rule.source_id);
                matched_rule_reason = rule.reason.clone();
            }
        }

        // When the resolved default is `Drop`, filter out value patterns
        // that come from earlier sources than the one that set Drop — those
        // patterns can't possibly survive the broader Drop policy.
        let patterns = if value_default == ValueAction::Drop {
            if patterns.iter().all(|pattern| pattern.source_id >= value_default_source) {
                patterns
            } else {
                let filtered: Vec<CompiledValuePattern> = patterns
                    .iter()
                    .filter(|pattern| pattern.source_id >= value_default_source)
                    .cloned()
                    .collect();
                filtered.into()
            }
        } else {
            patterns
        };

        let value_policy = Arc::new(ValuePolicy::new(value_default, patterns));

        ScopeResolution {
            exec,
            value_policy,
            module_name: context.module_name,
            object_name: context.object_name,
            relative_path: context.relative_path,
            absolute_path: context.absolute_path,
            matched_rule_index,
            matched_rule_source,
            matched_rule_reason,
        }
    }

    /// Return a summary of the filters that produced this classifier.
    pub fn summary(&self) -> FilterSummary {
        self.config.summary()
    }
}

#[derive(Debug, Clone)]
struct CompiledScopeRule {
    selector: Selector,
    exec: Option<ExecDecision>,
    value_default: Option<ValueAction>,
    value_patterns: Arc<[CompiledValuePattern]>,
    reason: Option<String>,
    source_id: usize,
    index: usize,
}

impl CompiledScopeRule {
    fn matches(&self, context: &ScopeContext) -> bool {
        match self.selector.kind() {
            SelectorKind::Package => context
                .module_name
                .as_deref()
                .map(|module| self.selector.matches(module))
                .unwrap_or(false),
            SelectorKind::File => context
                .relative_path
                .as_deref()
                .map(|path| self.selector.matches(path))
                .or_else(|| context.absolute_path.as_deref().map(|path| self.selector.matches(path)))
                .unwrap_or(false),
            SelectorKind::Object => context
                .object_name
                .as_deref()
                .map(|object| self.selector.matches(object))
                .unwrap_or(false),
            _ => false,
        }
    }
}

/// A compiled value selector and its associated action.
#[derive(Debug, Clone)]
pub struct CompiledValuePattern {
    pub selector: Selector,
    pub action: ValueAction,
    pub reason: Option<String>,
    pub source_id: usize,
}

fn compile_rules(rules: &[ScopeRule]) -> Arc<[CompiledScopeRule]> {
    let compiled: Vec<CompiledScopeRule> = rules
        .iter()
        .enumerate()
        .map(|(index, rule)| CompiledScopeRule {
            selector: rule.selector.clone(),
            exec: rule.exec.map(ExecDecision::from),
            value_default: rule.value_default,
            value_patterns: compile_value_patterns(&rule.value_patterns),
            reason: rule.reason.clone(),
            source_id: rule.source_id,
            index,
        })
        .collect();
    compiled.into()
}

fn compile_value_patterns(patterns: &[ValuePattern]) -> Arc<[CompiledValuePattern]> {
    let compiled: Vec<CompiledValuePattern> = patterns
        .iter()
        .map(|pattern| CompiledValuePattern {
            selector: pattern.selector.clone(),
            action: pattern.action,
            reason: pattern.reason.clone(),
            source_id: pattern.source_id,
        })
        .collect();
    compiled.into()
}

#[derive(Debug)]
struct ScopeContext {
    module_name: Option<String>,
    object_name: Option<String>,
    relative_path: Option<String>,
    absolute_path: Option<String>,
    source_id: Option<usize>,
}

impl ScopeContext {
    fn derive(filename: &str, sources: &[crate::model::FilterSource]) -> Self {
        let absolute_path = normalise_to_posix(Path::new(filename));

        let mut best_match: Option<(usize, PathBuf)> = None;
        for (idx, source) in sources.iter().enumerate() {
            if let Ok(stripped) = Path::new(filename).strip_prefix(&source.project_root) {
                let stripped_owned = stripped.to_path_buf();
                let better = match &best_match {
                    Some((_, current)) => stripped_owned.components().count() < current.components().count(),
                    None => true,
                };
                if better {
                    best_match = Some((idx, stripped_owned));
                }
            }
        }

        let (source_id, relative_path) = best_match.map_or((None, None), |(idx, rel)| {
            let normalized = normalise_relative(rel);
            if normalized.is_empty() {
                (Some(idx), None)
            } else {
                (Some(idx), Some(normalized))
            }
        });

        let module_name = relative_path
            .as_deref()
            .and_then(module_from_relative)
            .filter(|name| is_valid_module_name(name));

        ScopeContext {
            module_name,
            object_name: None,
            relative_path,
            absolute_path,
            source_id,
        }
    }

    fn refresh_object_name(&mut self, qualname: &str) {
        self.object_name = match (self.module_name.as_ref(), qualname.is_empty()) {
            (Some(module), false) => Some(format!("{module}.{qualname}")),
            (Some(module), true) => Some(module.clone()),
            (None, false) => Some(qualname.to_string()),
            (None, true) => None,
        };
    }
}

fn normalise_relative(relative: PathBuf) -> String {
    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => components.push(part.to_string_lossy().to_string()),
            Component::CurDir => continue,
            Component::ParentDir => {
                if !components.is_empty() {
                    components.pop();
                }
            }
            _ => {}
        }
    }
    components.join("/")
}

/// Normalise an arbitrary path into a POSIX-style string.  Exposed here
/// because the classifier needs it both internally and through the FFI
/// bridge.
pub fn normalise_to_posix(path: &Path) -> Option<String> {
    if path.as_os_str().is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::Prefix(prefix) => parts.push(prefix.as_os_str().to_string_lossy().to_string()),
            Component::RootDir => parts.push(String::new()),
            Component::CurDir => continue,
            Component::ParentDir => parts.push("..".to_string()),
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

/// Convert a normalised relative path (e.g., `pkg/foo.py`) into a dotted
/// module name. Mirrors the helper that previously lived in the Python
/// recorder's `module_identity` module.
pub fn module_from_relative(relative: &str) -> Option<String> {
    let mut parts: Vec<&str> = relative.split('/').filter(|segment| !segment.is_empty()).collect();
    if parts.is_empty() {
        return None;
    }
    let last = parts.pop().expect("non-empty");
    if let Some(stem) = last.strip_suffix(".py") {
        if stem != "__init__" {
            parts.push(stem);
        }
    } else {
        parts.push(last);
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

/// Return true when the supplied module name is a dotted identifier.
pub fn is_valid_module_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .split('.')
            .all(|segment| !segment.is_empty() && segment.chars().all(is_identifier_char))
}

fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TraceFilterConfig;
    use std::fs;
    use tempfile::tempdir;

    fn config_for_body(body: &str) -> (tempfile::TempDir, TraceFilterConfig, PathBuf) {
        let temp = tempdir().expect("tempdir");
        let codetracer_dir = temp.path().join(".codetracer");
        fs::create_dir(&codetracer_dir).expect("create .codetracer");
        let filter_path = codetracer_dir.join("filter.toml");
        fs::write(
            &filter_path,
            format!(
                r#"
[meta]
name = "test"
version = 1

{}
"#,
                body.trim()
            ),
        )
        .expect("write filter");
        let config = TraceFilterConfig::from_paths(&[filter_path.clone()]).expect("config");
        let file_path = temp.path().join("app").join("foo.py");
        fs::create_dir_all(file_path.parent().unwrap()).expect("create app dir");
        fs::File::create(&file_path).expect("create file");
        (temp, config, file_path)
    }

    #[test]
    fn applies_pkg_rule_to_classified_scope() {
        let (_tmp, config, file_path) = config_for_body(
            r#"
[scope]
default_exec = "skip"
default_value_action = "redact"

[[scope.rules]]
selector = "pkg:app.foo"
exec = "trace"
value_default = "allow"

[[scope.rules.value_patterns]]
selector = "local:literal:user"
action = "allow"

[[scope.rules.value_patterns]]
selector = "arg:password"
action = "redact"
"#,
        );

        let classifier = Classifier::new(config);
        let filename = file_path.to_string_lossy().into_owned();
        let resolution = classifier.classify(&ScopeQuery::new(&filename).with_qualname("foo").with_module_hint("app.foo"));
        assert_eq!(resolution.exec(), ExecDecision::Trace);
        assert_eq!(resolution.module_name(), Some("app.foo"));
        assert_eq!(resolution.relative_path(), Some("app/foo.py"));

        let policy = resolution.value_policy();
        assert_eq!(policy.default_action(), ValueAction::Allow);
        assert_eq!(policy.decide(ValueKind::Local, "user"), ValueAction::Allow);
        assert_eq!(policy.decide(ValueKind::Arg, "password"), ValueAction::Redact);
    }

    #[test]
    fn file_selector_matches_relative_path() {
        let (_tmp, config, file_path) = config_for_body(
            r#"
[scope]
default_exec = "trace"
default_value_action = "allow"

[[scope.rules]]
selector = "file:app/foo.py"
exec = "skip"
"#,
        );

        let classifier = Classifier::new(config);
        let filename = file_path.to_string_lossy().into_owned();
        let resolution = classifier.classify(&ScopeQuery::new(&filename));
        assert_eq!(resolution.exec(), ExecDecision::Skip);
        assert_eq!(resolution.relative_path(), Some("app/foo.py"));
    }

    #[test]
    fn object_rule_overrides_package_rule() {
        let (_tmp, config, file_path) = config_for_body(
            r#"
[scope]
default_exec = "trace"
default_value_action = "allow"

[[scope.rules]]
selector = "pkg:app.foo"
exec = "skip"

[[scope.rules]]
selector = "obj:app.foo.bar"
exec = "trace"
value_default = "redact"
"#,
        );

        let classifier = Classifier::new(config);
        let filename = file_path.to_string_lossy().into_owned();
        let resolution = classifier.classify(&ScopeQuery::new(&filename).with_qualname("bar").with_module_hint("app.foo"));
        assert_eq!(resolution.exec(), ExecDecision::Trace);
        assert_eq!(resolution.matched_rule_index(), Some(1));
        assert_eq!(resolution.value_policy().default_action(), ValueAction::Redact);
    }

    #[test]
    fn module_from_relative_strips_init() {
        assert_eq!(module_from_relative("pkg/module/__init__.py").as_deref(), Some("pkg.module"));
        assert_eq!(module_from_relative("pkg/module/sub.py").as_deref(), Some("pkg.module.sub"));
    }
}
