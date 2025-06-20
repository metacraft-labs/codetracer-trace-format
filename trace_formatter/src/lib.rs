pub mod prettify;
pub mod read_write_json;

#[cfg(test)]
mod tests {
    use crate::prettify::correct_path;

    use super::*;
    fn generate_pretty_json(input_json: &str) -> String {
        let ser_json = serde_json::from_str(input_json).expect("Failed to parse the json input");
        let prettified_json: String = prettify::prettify_value(ser_json, "", false);
        let mut final_pretty_json: String = correct_path(&prettified_json);
        final_pretty_json.push('\n'); //this is done automatically when saving the json to a file
        final_pretty_json
    }

    #[test]
    fn test_single_json_object() {
        let input_json = r#"[{"Key":"val"}]"#;
        let expected = r#"[
  { "Key": "val" }
]
"#;
        let final_pretty_json = generate_pretty_json(input_json);
        assert_eq!(final_pretty_json, expected);
    }

    #[test]
    fn test_non_absolute_path_json() {
        let input_json = r#"[{"Path":"?"},{"Path":"src/dir/main.nr"}]"#;
        let expected = r#"[
  { "Path": "?" },
  { "Path": "src/dir/main.nr" }
]
"#;
        let final_pretty_json = generate_pretty_json(input_json);
        assert_eq!(final_pretty_json, expected);
    }

    #[test]
    fn test_absolute_path_json() {
        let input_json = r#"[{"Path":"?"},{"Path":"some/absolute/path/src/dir/main.nr"}]"#;
        let expected = r#"[
  { "Path": "?" },
  { "Path": "<relative-to-this>/src/dir/main.nr" }
]
"#;
        let final_pretty_json = generate_pretty_json(input_json);
        assert_eq!(final_pretty_json, expected);
    }

    #[test]
    fn test_basic_nested_array_json() {
        let input_json = r#"[{"arr":[{"nested_arr":[{"key":"val"}]}]}]"#;
        let expected = r#"[
  { "arr": [
    { "nested_arr": [
      { "key": "val" }
    ] }
  ] }
]
"#;
        let final_pretty_json = generate_pretty_json(input_json);
        assert_eq!(final_pretty_json, expected);
    }

    #[test]
    fn test_basic_nested_json_objects() {
        let input_json = r#"[{"key":{"inner_key1":"inner_value1","inner_key2":"inner_value2"}}]"#;
        let expected = r#"[
  { "key": { "inner_key1": "inner_value1", "inner_key2": "inner_value2" } }
]
"#;
        let final_pretty_json = generate_pretty_json(input_json);
        assert_eq!(final_pretty_json, expected);
    }

    #[test]
    fn test_arrays_nested_objects_full_json() {
        let input_json = r#"[{"a":"111"},{"b":[]},{"c":[{"arr":"arr1", "abb" : 1},"#.to_string()
            + r#"{"arr":"arr2","abb" : 2},{"arr":"arr3","abb" : 3}]},{"long":"a1","along1":"a2"},"#
            + r#"{ "Value": { "variable_id": 0, "value": { "kind": "Int", "i": 4, "type_id": 1 } } }]"#;

        let expected = r#"[
  { "a": "111" },
  { "b": [] },
  { "c": [
    { "arr": "arr1", "abb": 1 },
    { "arr": "arr2", "abb": 2 },
    { "arr": "arr3", "abb": 3 }
  ] },
  { "long": "a1", "along1": "a2" },
  { "Value": { "variable_id": 0, "value": { "kind": "Int", "i": 4, "type_id": 1 } } }
]
"#;
        let final_pretty_json = generate_pretty_json(&input_json);
        assert_eq!(final_pretty_json, expected);
    }
}
