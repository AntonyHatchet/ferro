use dashmap::DashMap;
use ls_store::{AccountRegionBundle, Store};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use uuid::Uuid;

pub const DEFAULT_VISIBILITY_TIMEOUT: u64 = 30;
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 262144;
pub const DEFAULT_MESSAGE_RETENTION: u64 = 345600;
pub const DEFAULT_RECEIVE_WAIT_TIME: u64 = 0;
pub const DEFAULT_DELAY_SECONDS: u64 = 0;
pub const DEDUPLICATION_INTERVAL_SEC: u64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsMessage {
    pub message_id: String,
    pub body: String,
    pub md5_of_body: String,
    pub attributes: HashMap<String, String>,
    pub message_attributes: HashMap<String, MessageAttribute>,
    pub md5_of_message_attributes: Option<String>,
    pub receipt_handles: HashSet<String>,
    pub receive_count: u32,
    pub first_received: Option<f64>,
    pub last_received: Option<f64>,
    pub visibility_deadline: Option<f64>,
    pub delay_until: Option<f64>,
    pub created: f64,
    pub deleted: bool,
    pub sequence_number: Option<String>,
    pub message_group_id: Option<String>,
    pub message_deduplication_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<Vec<u8>>,
}

impl SqsMessage {
    pub fn new(body: String, message_attributes: HashMap<String, MessageAttribute>) -> Self {
        let md5_of_body = format!("{:x}", md5::compute(body.as_bytes()));
        let md5_of_attrs = if message_attributes.is_empty() {
            None
        } else {
            Some(compute_message_attributes_md5(&message_attributes))
        };
        Self {
            message_id: Uuid::new_v4().to_string(),
            body,
            md5_of_body,
            attributes: HashMap::new(),
            message_attributes,
            md5_of_message_attributes: md5_of_attrs,
            receipt_handles: HashSet::new(),
            receive_count: 0,
            first_received: None,
            last_received: None,
            visibility_deadline: None,
            delay_until: None,
            created: now(),
            deleted: false,
            sequence_number: None,
            message_group_id: None,
            message_deduplication_id: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        if self.deleted {
            return false;
        }
        let now = now();
        if let Some(deadline) = self.visibility_deadline {
            if now < deadline {
                return false;
            }
        }
        if let Some(delay_until) = self.delay_until {
            if now < delay_until {
                return false;
            }
        }
        true
    }
}

fn compute_message_attributes_md5(attrs: &HashMap<String, MessageAttribute>) -> String {
    let mut data = Vec::new();
    let mut sorted_keys: Vec<&String> = attrs.keys().collect();
    sorted_keys.sort();
    for key in sorted_keys {
        if let Some(attr) = attrs.get(key) {
            data.extend_from_slice(key.as_bytes());
            data.extend_from_slice(attr.data_type.as_bytes());
            if let Some(ref sv) = attr.string_value {
                data.push(0x01);
                data.extend_from_slice(sv.as_bytes());
            }
            if let Some(ref bv) = attr.binary_value {
                data.push(0x02);
                data.extend_from_slice(bv);
            }
        }
    }
    format!("{:x}", md5::compute(&data))
}

#[derive(Debug)]
pub struct SqsQueue {
    pub name: String,
    pub region: String,
    pub account_id: String,
    pub arn: String,
    pub fifo: bool,
    pub attributes: HashMap<String, String>,
    pub tags: HashMap<String, String>,
    pub created_timestamp: f64,
    pub last_modified_timestamp: f64,
    pub messages: Mutex<VecDeque<SqsMessage>>,
    pub inflight: Mutex<Vec<SqsMessage>>,
    pub notify: Notify,
    pub dedup_cache: Mutex<HashMap<String, f64>>,
}

impl SqsQueue {
    pub fn new(
        name: String,
        region: String,
        account_id: String,
        attributes: Option<HashMap<String, String>>,
        tags: Option<HashMap<String, String>>,
    ) -> Self {
        let fifo = name.ends_with(".fifo");
        let now_ts = now();
        let partition = "aws";
        let arn = format!("arn:{partition}:sqs:{region}:{account_id}:{name}");

        let mut attrs = attributes.unwrap_or_default();
        attrs
            .entry("VisibilityTimeout".to_string())
            .or_insert_with(|| DEFAULT_VISIBILITY_TIMEOUT.to_string());
        attrs
            .entry("MaximumMessageSize".to_string())
            .or_insert_with(|| DEFAULT_MAX_MESSAGE_SIZE.to_string());
        attrs
            .entry("MessageRetentionPeriod".to_string())
            .or_insert_with(|| DEFAULT_MESSAGE_RETENTION.to_string());
        attrs
            .entry("DelaySeconds".to_string())
            .or_insert_with(|| DEFAULT_DELAY_SECONDS.to_string());
        attrs
            .entry("ReceiveMessageWaitTimeSeconds".to_string())
            .or_insert_with(|| DEFAULT_RECEIVE_WAIT_TIME.to_string());
        if fifo {
            attrs
                .entry("FifoQueue".to_string())
                .or_insert_with(|| "true".to_string());
            attrs
                .entry("ContentBasedDeduplication".to_string())
                .or_insert_with(|| "false".to_string());
        }

        Self {
            name,
            region,
            account_id,
            arn,
            fifo,
            attributes: attrs,
            tags: tags.unwrap_or_default(),
            created_timestamp: now_ts,
            last_modified_timestamp: now_ts,
            messages: Mutex::new(VecDeque::new()),
            inflight: Mutex::new(Vec::new()),
            notify: Notify::new(),
            dedup_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn url(&self, host: &str) -> String {
        format!("{host}/{}/{}", self.account_id, self.name)
    }

    pub fn visibility_timeout(&self) -> u64 {
        self.attributes
            .get("VisibilityTimeout")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_VISIBILITY_TIMEOUT)
    }

    pub fn delay_seconds(&self) -> u64 {
        self.attributes
            .get("DelaySeconds")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_DELAY_SECONDS)
    }

    pub fn max_message_size(&self) -> usize {
        self.attributes
            .get("MaximumMessageSize")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE)
    }

    pub fn wait_time_seconds(&self) -> u64 {
        self.attributes
            .get("ReceiveMessageWaitTimeSeconds")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_RECEIVE_WAIT_TIME)
    }

    pub fn redrive_policy(&self) -> Option<serde_json::Value> {
        self.attributes
            .get("RedrivePolicy")
            .and_then(|v| serde_json::from_str(v).ok())
    }

    pub fn max_receive_count(&self) -> Option<u32> {
        self.redrive_policy().and_then(|p| {
            p.get("maxReceiveCount")
                .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64().map(|n| n as u32)))
        })
    }

    pub fn dead_letter_target_arn(&self) -> Option<String> {
        self.redrive_policy().and_then(|p| {
            p.get("deadLetterTargetArn")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        })
    }

    pub fn put_message(&self, mut msg: SqsMessage, delay_seconds: Option<u64>) -> SqsMessage {
        let delay = delay_seconds.unwrap_or_else(|| self.delay_seconds());
        if delay > 0 {
            msg.delay_until = Some(now() + delay as f64);
        }

        msg.attributes.insert(
            "SentTimestamp".to_string(),
            (now() * 1000.0).round().to_string(),
        );
        msg.attributes
            .entry("ApproximateReceiveCount".to_string())
            .or_insert_with(|| "0".to_string());

        if self.fifo {
            if let Some(ref dedup_id) = msg.message_deduplication_id {
                let mut cache = self.dedup_cache.lock().unwrap();
                let cutoff = now() - DEDUPLICATION_INTERVAL_SEC as f64;
                cache.retain(|_, ts| *ts > cutoff);
                if cache.contains_key(dedup_id) {
                    return msg;
                }
                cache.insert(dedup_id.clone(), now());
            }
        }

        let result = msg.clone();
        self.messages.lock().unwrap().push_back(msg);
        self.notify.notify_waiters();
        result
    }

    pub fn receive_messages(
        &self,
        max_messages: usize,
        visibility_timeout: Option<u64>,
    ) -> Vec<(SqsMessage, String)> {
        let vt = visibility_timeout.unwrap_or_else(|| self.visibility_timeout());
        let mut msgs = self.messages.lock().unwrap();
        let mut inflight = self.inflight.lock().unwrap();

        let current = now();
        let mut expired: Vec<SqsMessage> = Vec::new();
        inflight.retain(|m| {
            if let Some(deadline) = m.visibility_deadline {
                if current >= deadline && !m.deleted {
                    expired.push(m.clone());
                    return false;
                }
            }
            !m.deleted
        });
        for m in expired {
            msgs.push_back(m);
        }

        let mut result = Vec::new();
        let mut remaining = VecDeque::new();

        while let Some(mut msg) = msgs.pop_front() {
            if result.len() >= max_messages {
                remaining.push_back(msg);
                continue;
            }
            if !msg.is_visible() {
                remaining.push_back(msg);
                continue;
            }

            msg.receive_count += 1;
            let count_str = msg.receive_count.to_string();
            msg.attributes
                .insert("ApproximateReceiveCount".to_string(), count_str);

            let received_ts = now();
            if msg.first_received.is_none() {
                msg.first_received = Some(received_ts);
                msg.attributes.insert(
                    "ApproximateFirstReceiveTimestamp".to_string(),
                    (received_ts * 1000.0).round().to_string(),
                );
            }
            msg.last_received = Some(received_ts);
            msg.visibility_deadline = Some(received_ts + vt as f64);

            let receipt = Uuid::new_v4().to_string();
            msg.receipt_handles.insert(receipt.clone());

            let out = msg.clone();
            inflight.push(msg);
            result.push((out, receipt));
        }

        *msgs = remaining;
        result
    }

    pub fn delete_message(&self, receipt_handle: &str) -> bool {
        let mut inflight = self.inflight.lock().unwrap();
        if let Some(pos) = inflight
            .iter()
            .position(|m| m.receipt_handles.contains(receipt_handle))
        {
            inflight[pos].deleted = true;
            inflight.remove(pos);
            return true;
        }

        let mut msgs = self.messages.lock().unwrap();
        if let Some(pos) = msgs
            .iter()
            .position(|m| m.receipt_handles.contains(receipt_handle))
        {
            msgs.remove(pos);
            return true;
        }
        false
    }

    pub fn change_visibility(
        &self,
        receipt_handle: &str,
        visibility_timeout: u64,
    ) -> Result<(), String> {
        let mut inflight = self.inflight.lock().unwrap();
        for msg in inflight.iter_mut() {
            if msg.receipt_handles.contains(receipt_handle) {
                if visibility_timeout == 0 {
                    msg.visibility_deadline = None;
                } else {
                    msg.visibility_deadline = Some(now() + visibility_timeout as f64);
                }
                return Ok(());
            }
        }
        Err("Message not inflight".to_string())
    }

    pub fn purge(&self) {
        self.messages.lock().unwrap().clear();
        self.inflight.lock().unwrap().clear();
    }

    pub fn approximate_message_count(&self) -> usize {
        self.messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.is_visible())
            .count()
    }

    pub fn approximate_not_visible(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    pub fn approximate_delayed(&self) -> usize {
        let now = now();
        self.messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| {
                if let Some(delay) = m.delay_until {
                    now < delay
                } else {
                    false
                }
            })
            .count()
    }

    pub fn get_attributes(&self, names: &[String]) -> HashMap<String, String> {
        let all = names.is_empty() || names.iter().any(|n| n == "All");
        let mut result = HashMap::new();

        if all || names.iter().any(|n| n == "QueueArn") {
            result.insert("QueueArn".to_string(), self.arn.clone());
        }
        if all || names.iter().any(|n| n == "CreatedTimestamp") {
            result.insert(
                "CreatedTimestamp".to_string(),
                (self.created_timestamp as u64).to_string(),
            );
        }
        if all || names.iter().any(|n| n == "LastModifiedTimestamp") {
            result.insert(
                "LastModifiedTimestamp".to_string(),
                (self.last_modified_timestamp as u64).to_string(),
            );
        }
        if all
            || names
                .iter()
                .any(|n| n == "ApproximateNumberOfMessages")
        {
            result.insert(
                "ApproximateNumberOfMessages".to_string(),
                self.approximate_message_count().to_string(),
            );
        }
        if all
            || names
                .iter()
                .any(|n| n == "ApproximateNumberOfMessagesNotVisible")
        {
            result.insert(
                "ApproximateNumberOfMessagesNotVisible".to_string(),
                self.approximate_not_visible().to_string(),
            );
        }
        if all
            || names
                .iter()
                .any(|n| n == "ApproximateNumberOfMessagesDelayed")
        {
            result.insert(
                "ApproximateNumberOfMessagesDelayed".to_string(),
                self.approximate_delayed().to_string(),
            );
        }
        for (k, v) in &self.attributes {
            if all || names.contains(k) {
                result.insert(k.clone(), v.clone());
            }
        }
        result
    }
}

#[derive(Debug, Default)]
pub struct SqsStore {
    pub queues: DashMap<String, Arc<SqsQueue>>,
    pub deleted: DashMap<String, f64>,
}

impl Store for SqsStore {
    fn service_name() -> &'static str {
        "sqs"
    }
}

pub fn sqs_stores() -> AccountRegionBundle<SqsStore> {
    AccountRegionBundle::new()
}

pub fn now() -> f64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_queue(name: &str) -> SqsQueue {
        SqsQueue::new(name.to_string(), "us-east-1".to_string(), "000000000000".to_string(), None, None)
    }

    fn make_msg(body: &str) -> SqsMessage {
        SqsMessage::new(body.to_string(), HashMap::new())
    }

    #[test]
    fn message_md5_is_computed() {
        let msg = make_msg("hello");
        assert_eq!(msg.md5_of_body, format!("{:x}", md5::compute(b"hello")));
    }

    #[test]
    fn message_starts_visible() {
        let msg = make_msg("test");
        assert!(msg.is_visible());
    }

    #[test]
    fn deleted_message_not_visible() {
        let mut msg = make_msg("test");
        msg.deleted = true;
        assert!(!msg.is_visible());
    }

    #[test]
    fn delayed_message_not_visible() {
        let mut msg = make_msg("test");
        msg.delay_until = Some(now() + 3600.0);
        assert!(!msg.is_visible());
    }

    #[test]
    fn queue_defaults() {
        let q = make_queue("test-queue");
        assert_eq!(q.name, "test-queue");
        assert_eq!(q.visibility_timeout(), DEFAULT_VISIBILITY_TIMEOUT);
        assert_eq!(q.delay_seconds(), DEFAULT_DELAY_SECONDS);
        assert_eq!(q.max_message_size(), DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(q.wait_time_seconds(), DEFAULT_RECEIVE_WAIT_TIME);
        assert!(!q.fifo);
        assert_eq!(q.arn, "arn:aws:sqs:us-east-1:000000000000:test-queue");
    }

    #[test]
    fn fifo_queue_detection() {
        let q = make_queue("orders.fifo");
        assert!(q.fifo);
        assert_eq!(q.attributes.get("FifoQueue").unwrap(), "true");
    }

    #[test]
    fn queue_url_format() {
        let q = make_queue("my-queue");
        assert_eq!(q.url("http://localhost:4566"), "http://localhost:4566/000000000000/my-queue");
    }

    #[test]
    fn put_and_receive_message() {
        let q = make_queue("test");
        let msg = make_msg("hello world");
        q.put_message(msg, None);
        assert_eq!(q.approximate_message_count(), 1);

        let received = q.receive_messages(1, None);
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0.body, "hello world");
        assert_eq!(received[0].0.receive_count, 1);

        assert_eq!(q.approximate_message_count(), 0);
        assert_eq!(q.approximate_not_visible(), 1);
    }

    #[test]
    fn receive_respects_max_messages() {
        let q = make_queue("test");
        for i in 0..5 {
            q.put_message(make_msg(&format!("msg-{i}")), None);
        }
        let received = q.receive_messages(2, None);
        assert_eq!(received.len(), 2);
        assert_eq!(q.approximate_message_count(), 3);
    }

    #[test]
    fn delete_message_removes_from_inflight() {
        let q = make_queue("test");
        q.put_message(make_msg("to-delete"), None);
        let received = q.receive_messages(1, None);
        let receipt = &received[0].1;

        assert!(q.delete_message(receipt));
        assert_eq!(q.approximate_not_visible(), 0);
    }

    #[test]
    fn purge_clears_all_messages() {
        let q = make_queue("test");
        for i in 0..10 {
            q.put_message(make_msg(&format!("msg-{i}")), None);
        }
        q.receive_messages(3, None);

        q.purge();
        assert_eq!(q.approximate_message_count(), 0);
        assert_eq!(q.approximate_not_visible(), 0);
    }

    #[test]
    fn change_visibility_extends_deadline() {
        let q = make_queue("test");
        q.put_message(make_msg("test"), None);
        let received = q.receive_messages(1, Some(1));
        let receipt = &received[0].1;

        q.change_visibility(receipt, 600).unwrap();

        let again = q.receive_messages(1, None);
        assert_eq!(again.len(), 0);
    }

    #[test]
    fn change_visibility_nonexistent_receipt_fails() {
        let q = make_queue("test");
        let result = q.change_visibility("bogus-receipt", 30);
        assert!(result.is_err());
    }

    #[test]
    fn fifo_dedup_prevents_duplicate() {
        let q = make_queue("test.fifo");
        let mut msg1 = make_msg("first");
        msg1.message_deduplication_id = Some("dedup-1".to_string());
        q.put_message(msg1, None);

        let mut msg2 = make_msg("second");
        msg2.message_deduplication_id = Some("dedup-1".to_string());
        q.put_message(msg2, None);

        assert_eq!(q.approximate_message_count(), 1);
    }

    #[test]
    fn get_attributes_all() {
        let q = make_queue("test");
        let attrs = q.get_attributes(&["All".to_string()]);
        assert!(attrs.contains_key("QueueArn"));
        assert!(attrs.contains_key("VisibilityTimeout"));
        assert!(attrs.contains_key("ApproximateNumberOfMessages"));
    }

    #[test]
    fn get_attributes_selective() {
        let q = make_queue("test");
        let attrs = q.get_attributes(&["QueueArn".to_string()]);
        assert!(attrs.contains_key("QueueArn"));
        assert!(!attrs.contains_key("ApproximateNumberOfMessages"));
    }

    #[test]
    fn delayed_messages_counted_correctly() {
        let q = make_queue("test");
        q.put_message(make_msg("delayed"), Some(300));
        assert_eq!(q.approximate_message_count(), 0);
        assert_eq!(q.approximate_delayed(), 1);
    }

    #[test]
    fn custom_attributes_applied() {
        let mut attrs = HashMap::new();
        attrs.insert("VisibilityTimeout".to_string(), "120".to_string());
        let q = SqsQueue::new(
            "custom".to_string(), "us-east-1".to_string(), "000000000000".to_string(),
            Some(attrs), None,
        );
        assert_eq!(q.visibility_timeout(), 120);
    }

    #[test]
    fn redrive_policy_parsed() {
        let mut attrs = HashMap::new();
        attrs.insert(
            "RedrivePolicy".to_string(),
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000:dlq","maxReceiveCount":"3"}"#.to_string(),
        );
        let q = SqsQueue::new("src".to_string(), "us-east-1".to_string(), "000000000000".to_string(), Some(attrs), None);
        assert_eq!(q.max_receive_count(), Some(3));
        assert_eq!(q.dead_letter_target_arn().unwrap(), "arn:aws:sqs:us-east-1:000:dlq");
    }
}
