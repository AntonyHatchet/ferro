use crate::models::MessageAttributeValue;
use std::collections::HashMap;

pub fn matches(
    filter_policy: &str,
    scope: &str,
    message_body: &str,
    message_attributes: &HashMap<String, MessageAttributeValue>,
) -> bool {
    let policy: serde_json::Value = match serde_json::from_str(filter_policy) {
        Ok(v) => v,
        Err(_) => return true,
    };

    let policy_obj = match policy.as_object() {
        Some(o) => o,
        None => return true,
    };

    if scope == "MessageBody" {
        let body: serde_json::Value = match serde_json::from_str(message_body) {
            Ok(v) => v,
            Err(_) => return true,
        };
        match_object(policy_obj, &body)
    } else {
        match_message_attributes(policy_obj, message_attributes)
    }
}

fn match_object(
    policy: &serde_json::Map<String, serde_json::Value>,
    target: &serde_json::Value,
) -> bool {
    let target_obj = match target.as_object() {
        Some(o) => o,
        None => return false,
    };

    for (key, condition) in policy {
        let field_value = match target_obj.get(key) {
            Some(v) => v,
            None => return false,
        };

        if let Some(arr) = condition.as_array() {
            if !value_in_array(field_value, arr) {
                return false;
            }
        } else if let Some(nested) = condition.as_object() {
            if !match_object(nested, field_value) {
                return false;
            }
        } else {
            return false;
        }
    }

    true
}

fn match_message_attributes(
    policy: &serde_json::Map<String, serde_json::Value>,
    attributes: &HashMap<String, MessageAttributeValue>,
) -> bool {
    for (key, condition) in policy {
        let attr = match attributes.get(key) {
            Some(a) => a,
            None => return false,
        };

        let attr_str = attr.string_value.as_deref().unwrap_or("");

        if let Some(arr) = condition.as_array() {
            if !arr.iter().any(|v| v.as_str().is_some_and(|s| s == attr_str)) {
                return false;
            }
        } else {
            return false;
        }
    }

    true
}

fn value_in_array(value: &serde_json::Value, allowed: &[serde_json::Value]) -> bool {
    match value {
        serde_json::Value::String(s) => allowed.iter().any(|v| v.as_str().is_some_and(|a| a == s)),
        serde_json::Value::Number(n) => allowed.iter().any(|v| v.as_f64() == n.as_f64()),
        serde_json::Value::Bool(b) => allowed.iter().any(|v| v.as_bool() == Some(*b)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_attrs(pairs: &[(&str, &str)]) -> HashMap<String, MessageAttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    MessageAttributeValue {
                        data_type: "String".to_string(),
                        string_value: Some(v.to_string()),
                        binary_value: None,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn message_attributes_exact_match() {
        let attrs = make_attrs(&[("eventType", "order_placed")]);
        let policy = r#"{"eventType":["order_placed","order_cancelled"]}"#;
        assert!(matches(policy, "MessageAttributes", "", &attrs));
    }

    #[test]
    fn message_attributes_no_match() {
        let attrs = make_attrs(&[("eventType", "order_shipped")]);
        let policy = r#"{"eventType":["order_placed","order_cancelled"]}"#;
        assert!(!matches(policy, "MessageAttributes", "", &attrs));
    }

    #[test]
    fn message_attributes_missing_key() {
        let attrs = make_attrs(&[("other", "value")]);
        let policy = r#"{"eventType":["order_placed"]}"#;
        assert!(!matches(policy, "MessageAttributes", "", &attrs));
    }

    #[test]
    fn message_attributes_multiple_conditions_all_must_match() {
        let attrs = make_attrs(&[("eventType", "order_placed"), ("region", "us-east-1")]);
        let policy = r#"{"eventType":["order_placed"],"region":["us-west-2"]}"#;
        assert!(!matches(policy, "MessageAttributes", "", &attrs));
    }

    #[test]
    fn message_body_nested_match() {
        let body = r#"{"body":{"resourceType":"chat","id":"123"}}"#;
        let policy = r#"{"body":{"resourceType":["chat"]}}"#;
        assert!(matches(policy, "MessageBody", body, &HashMap::new()));
    }

    #[test]
    fn message_body_nested_no_match() {
        let body = r#"{"body":{"resourceType":"email","id":"123"}}"#;
        let policy = r#"{"body":{"resourceType":["chat"]}}"#;
        assert!(!matches(policy, "MessageBody", body, &HashMap::new()));
    }

    #[test]
    fn message_body_top_level_match() {
        let body = r#"{"status":"active","name":"test"}"#;
        let policy = r#"{"status":["active","pending"]}"#;
        assert!(matches(policy, "MessageBody", body, &HashMap::new()));
    }

    #[test]
    fn message_body_numeric_match() {
        let body = r#"{"priority":1}"#;
        let policy = r#"{"priority":[1,2,3]}"#;
        assert!(matches(policy, "MessageBody", body, &HashMap::new()));
    }

    #[test]
    fn message_body_missing_field() {
        let body = r#"{"other":"value"}"#;
        let policy = r#"{"status":["active"]}"#;
        assert!(!matches(policy, "MessageBody", body, &HashMap::new()));
    }

    #[test]
    fn empty_policy_matches_everything() {
        let policy = "{}";
        assert!(matches(policy, "MessageAttributes", "", &HashMap::new()));
        assert!(matches(policy, "MessageBody", "{}", &HashMap::new()));
    }

    #[test]
    fn malformed_policy_json_fails_open() {
        assert!(matches("not json", "MessageAttributes", "", &HashMap::new()));
    }

    #[test]
    fn malformed_body_json_fails_open() {
        let policy = r#"{"key":["val"]}"#;
        assert!(matches(policy, "MessageBody", "not json", &HashMap::new()));
    }

    #[test]
    fn deeply_nested_body_match() {
        let body = r#"{"a":{"b":{"c":"target"}}}"#;
        let policy = r#"{"a":{"b":{"c":["target"]}}}"#;
        assert!(matches(policy, "MessageBody", body, &HashMap::new()));
    }
}
