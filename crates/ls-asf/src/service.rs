use crate::context::RequestContext;
use crate::error::ServiceException;
use axum::body::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ServiceResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
}

impl ServiceResponse {
    pub fn xml(status: u16, body: impl Into<String>) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "text/xml".to_string());
        Self {
            status,
            headers,
            body: Bytes::from(body.into()),
        }
    }

    pub fn json(status: u16, body: impl Into<String>) -> Self {
        let mut headers = HashMap::new();
        headers.insert(
            "Content-Type".to_string(),
            "application/x-amz-json-1.0".to_string(),
        );
        Self {
            status,
            headers,
            body: Bytes::from(body.into()),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

pub type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<ServiceResponse, ServiceException>> + Send>>;

pub trait ServiceHandler: Send + Sync + 'static {
    fn service_name(&self) -> &str;
    fn handle(&self, ctx: RequestContext, params: serde_json::Value) -> HandlerFuture;
}

pub struct ServiceRegistry {
    handlers: HashMap<String, Arc<dyn ServiceHandler>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, handler: Arc<dyn ServiceHandler>) {
        let name = handler.service_name().to_string();
        self.handlers.insert(name, handler);
    }

    pub fn get(&self, service_name: &str) -> Option<&Arc<dyn ServiceHandler>> {
        self.handlers.get(service_name)
    }

    pub fn service_names(&self) -> Vec<&str> {
        self.handlers.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}
