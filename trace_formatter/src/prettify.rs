use regex::Regex;
use serde_json::Value;

pub fn prettify_value(root_value: Value, indent: &str, is_in_array: bool) -> String {
    let content = match root_value {
        Value::Array(elements) => {
            let new_indent = indent.to_string() + "  ";
            let parts: Vec<String> = elements
                .into_iter()
                .map(|el| prettify_value(el, new_indent.as_str(), true))
                .collect::<Vec<String>>();
            if parts.is_empty() {
                "[]".to_string()
            } else {
                let lines = parts.join(",\n");
                format!("[\n{lines}\n{indent}]")
            }
        }
        Value::Object(map) => {
            let parts: Vec<String> = map
                .into_iter()
                .map(|(k, v)| {
                    let head = format!("\"{k}\"");
                    let rest = prettify_value(v, indent, false);
                    format!("{head}: {rest}")
                })
                .collect();
            let json_object_string = parts.join(", ");
            format!("{{ {json_object_string} }}")
        }
        _ => root_value.to_string(),
    };
    let indent = if is_in_array { indent } else { "" };
    format!("{indent}{content}")
}

/// Replaces all absolute paths in the trace with relative paths
pub fn correct_path(pretty_json: &str) -> String {
    let re = Regex::new(r#"  \{ "Path": (?<abs_path>.*)(?<rel_path>/src/.*)"#);
    let result = re.map(|regex| {
        regex.replace_all(pretty_json, |caps: &regex::Captures| {
            format!("  {{ \"Path\": \"<relative-to-this>{}", &caps["rel_path"])
        })
    });
    result.map(|result_string| result_string.into_owned()).unwrap_or(pretty_json.to_string())
}
