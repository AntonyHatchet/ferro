use dashmap::DashMap;
use ls_store::Store;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn next_sequence_number() -> String {
    SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst).to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    pub arn: String,
    pub name: String,
    pub attributes: HashMap<String, String>,
    pub subscriptions: Vec<String>,
    pub data_protection_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnsSubscription {
    pub subscription_arn: String,
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
    pub owner: String,
    pub pending_confirmation: bool,
    pub raw_message_delivery: bool,
    pub filter_policy: Option<String>,
    pub filter_policy_scope: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnsMessage {
    pub message_id: String,
    pub topic_arn: String,
    pub message: String,
    pub subject: Option<String>,
    pub message_structure: Option<String>,
    pub message_attributes: HashMap<String, MessageAttributeValue>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAttributeValue {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct SnsStore {
    pub topics: DashMap<String, Topic>,
    pub subscriptions: DashMap<String, SnsSubscription>,
    pub subscription_tokens: DashMap<String, String>,
    pub tags: DashMap<String, HashMap<String, String>>,
    pub sms_messages: std::sync::Mutex<Vec<SnsMessage>>,
}

impl Store for SnsStore {
    fn service_name() -> &'static str {
        "sns"
    }
}

pub fn topic_arn(name: &str, region: &str, account_id: &str) -> String {
    format!("arn:aws:sns:{region}:{account_id}:{name}")
}

pub fn subscription_arn(topic_arn: &str, _protocol: &str) -> String {
    format!("{topic_arn}:{}", Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_arn_format() {
        let arn = topic_arn("my-topic", "us-east-1", "123456789012");
        assert_eq!(arn, "arn:aws:sns:us-east-1:123456789012:my-topic");
    }

    #[test]
    fn subscription_arn_contains_topic_arn() {
        let t_arn = "arn:aws:sns:us-east-1:123456789012:my-topic";
        let s_arn = subscription_arn(t_arn, "sqs");
        assert!(s_arn.starts_with(t_arn));
        assert!(s_arn.len() > t_arn.len());
    }

    #[test]
    fn sequence_numbers_are_unique() {
        let a = next_sequence_number();
        let b = next_sequence_number();
        assert_ne!(a, b);
        assert!(b.parse::<u64>().unwrap() > a.parse::<u64>().unwrap());
    }

    #[test]
    fn sns_store_defaults() {
        let store = SnsStore::default();
        assert!(store.topics.is_empty());
        assert!(store.subscriptions.is_empty());
    }

    #[test]
    fn topic_crud() {
        let store = SnsStore::default();
        let arn = topic_arn("events", "us-east-1", "000000000000");
        store.topics.insert(arn.clone(), Topic {
            arn: arn.clone(),
            name: "events".to_string(),
            attributes: HashMap::new(),
            subscriptions: Vec::new(),
            data_protection_policy: None,
        });
        assert_eq!(store.topics.len(), 1);
        assert!(store.topics.contains_key(&arn));

        store.topics.remove(&arn);
        assert!(store.topics.is_empty());
    }

    #[test]
    fn subscription_linked_to_topic() {
        let store = SnsStore::default();
        let t_arn = topic_arn("events", "us-east-1", "000000000000");
        let s_arn = subscription_arn(&t_arn, "sqs");
        store.subscriptions.insert(s_arn.clone(), SnsSubscription {
            subscription_arn: s_arn.clone(),
            topic_arn: t_arn.clone(),
            protocol: "sqs".to_string(),
            endpoint: "arn:aws:sqs:us-east-1:000:queue".to_string(),
            owner: "000000000000".to_string(),
            pending_confirmation: false,
            raw_message_delivery: false,
            filter_policy: None,
            filter_policy_scope: "MessageAttributes".to_string(),
            attributes: HashMap::new(),
        });
        let sub = store.subscriptions.get(&s_arn).unwrap();
        assert_eq!(sub.topic_arn, t_arn);
        assert_eq!(sub.protocol, "sqs");
    }
}
