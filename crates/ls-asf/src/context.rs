use axum::body::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub service_name: String,
    pub operation: String,
    pub protocol: String,
    pub region: String,
    pub account_id: String,
    pub request_id: String,
    pub method: String,
    pub uri: String,
    pub headers: HashMap<String, String>,
    pub query_params: HashMap<String, String>,
    pub body: Bytes,
}

impl RequestContext {
    pub fn new() -> Self {
        Self {
            service_name: String::new(),
            operation: String::new(),
            protocol: "query".to_string(),
            region: std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            account_id: "000000000000".to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            method: String::new(),
            uri: String::new(),
            headers: HashMap::new(),
            query_params: HashMap::new(),
            body: Bytes::new(),
        }
    }

    pub fn is_json_protocol(&self) -> bool {
        self.protocol == "json"
    }
}

impl Default for RequestContext {
    fn default() -> Self {
        Self::new()
    }
}
