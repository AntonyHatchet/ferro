use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::IntoResponse,
    routing::{any, get},
    Router,
};
use bytes::Bytes;
use ls_asf::{
    context::RequestContext,
    parser,
    serializer,
    service::{ServiceRegistry, ServiceResponse},
    ServiceException,
};
use std::sync::Arc;

type AppState = Arc<ServiceRegistry>;

pub fn create_router(state: AppState, body_limit: Option<usize>) -> Router {
    let router = Router::new()
        .route("/_ferro/health", get(health_check))
        .route("/health", get(health_check))
        .route("/{*path}", any(handle_request))
        .route("/", any(handle_request));

    let router = match body_limit {
        Some(limit) => router.layer(DefaultBodyLimit::max(limit)),
        None => router.layer(DefaultBodyLimit::disable()),
    };

    router.with_state(state)
}

async fn health_check(State(registry): State<AppState>) -> impl IntoResponse {
    let services: serde_json::Map<String, serde_json::Value> = registry
        .service_names()
        .into_iter()
        .map(|name| {
            (
                name.to_string(),
                serde_json::Value::String("available".to_string()),
            )
        })
        .collect();

    let body = serde_json::json!({
        "services": services,
        "version": "ferro",
    });

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
}

async fn handle_request(
    State(registry): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut ctx = RequestContext::new();
    ctx.method = method.to_string();
    ctx.uri = uri.to_string();
    ctx.body = body;

    for (key, value) in &headers {
        if let Ok(v) = value.to_str() {
            ctx.headers.insert(key.to_string(), v.to_string());
        }
    }

    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                let k = urlencoding::decode(k).unwrap_or_default().to_string();
                let v = urlencoding::decode(v).unwrap_or_default().to_string();
                ctx.query_params.insert(k, v);
            } else if !pair.is_empty() {
                let k = urlencoding::decode(pair).unwrap_or_default().to_string();
                ctx.query_params.insert(k, String::new());
            }
        }
    }

    let origin = ctx
        .headers
        .get("origin")
        .or_else(|| ctx.headers.get("Origin"))
        .cloned();

    if method == Method::OPTIONS {
        tracing::debug!(target: "ferro::http", "{} {} -> CORS preflight", method, uri);
        return build_cors_preflight(origin.as_deref(), &ctx);
    }

    let credential = ctx
        .headers
        .get("authorization")
        .or_else(|| ctx.headers.get("Authorization"))
        .cloned()
        .or_else(|| ctx.query_params.get("X-Amz-Credential").map(|c| format!("Credential={c}")))
        .unwrap_or_default();

    if let Some(region) = extract_region_from_auth(&credential) {
        ctx.region = region;
    }
    if let Some(account) = extract_account_from_auth(&credential) {
        ctx.account_id = account;
    }

    let service_name = detect_service(&ctx);
    ctx.service_name = service_name.clone();

    let handler = match registry.get(&service_name) {
        Some(h) => h,
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("Service '{}' not found", service_name),
            )
                .into_response();
        }
    };

    let protocol = if service_name == "s3" {
        "rest-xml"
    } else {
        parser::detect_protocol(&ctx)
    };
    ctx.protocol = protocol.to_string();

    let (operation, params) = match protocol {
        "json" => parser::parse_sqs_json_request(&ctx),
        "query" => parser::parse_query_request(&ctx),
        "rest-xml" => {
            let (op, _) = parser::parse_rest_xml_request(&ctx);
            let s3_params = parser::extract_s3_params(&ctx);
            (op, serde_json::Value::Object(s3_params))
        }
        _ => parser::parse_query_request(&ctx),
    };

    ctx.operation = operation;

    let request_id = ctx.request_id.clone();
    let op_for_error = ctx.operation.clone();

    let path = uri.path();
    let started = std::time::Instant::now();

    let mut response = match handler.handle(ctx, params).await {
        Ok(resp) => {
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            log_request(&service_name, &method, path, &op_for_error, resp.status, None, ms);
            build_response(resp, &request_id)
        }
        Err(ref err) => {
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            log_request(&service_name, &method, path, &op_for_error, err.status_code, Some(&err.code), ms);
            build_error_response(err, protocol, &request_id, &service_name, &op_for_error)
        }
    };

    if origin.is_some() {
        inject_cors_headers(response.headers_mut(), origin.as_deref());
    }

    response
}

fn detect_service(ctx: &RequestContext) -> String {
    let host = ctx
        .headers
        .get("host")
        .or_else(|| ctx.headers.get("Host"))
        .cloned()
        .unwrap_or_default();

    if host.contains("s3") || host.ends_with(".s3.localhost.localstack.cloud")
        || host.ends_with(".s3.localhost.ferro.cloud")
    {
        return "s3".to_string();
    }

    let auth = ctx
        .headers
        .get("authorization")
        .or_else(|| ctx.headers.get("Authorization"))
        .cloned()
        .unwrap_or_default();

    if let Some(svc) = extract_service_from_auth(&auth) {
        return svc;
    }

    let content_type = ctx
        .headers
        .get("content-type")
        .or_else(|| ctx.headers.get("Content-Type"))
        .cloned()
        .unwrap_or_default();

    if content_type.contains("x-amz-json") {
        let target = ctx
            .headers
            .get("x-amz-target")
            .or_else(|| ctx.headers.get("X-Amz-Target"))
            .cloned()
            .unwrap_or_default();
        if target.contains("SQS") || target.starts_with("AmazonSQS") {
            return "sqs".to_string();
        }
    }

    let body_str = String::from_utf8_lossy(&ctx.body);
    if body_str.contains("Action=") {
        if ctx.uri.contains("sns") || host.contains("sns") {
            return "sns".to_string();
        }
        if ctx.uri.contains("sqs") || host.contains("sqs") || ctx.uri.contains("queue") {
            return "sqs".to_string();
        }
    }

    let path = ctx.uri.split('?').next().unwrap_or("/");
    if path.starts_with("/_aws/sqs") || path.contains("/queue/") {
        return "sqs".to_string();
    }
    if path.starts_with("/_aws/sns") {
        return "sns".to_string();
    }

    "s3".to_string()
}

fn extract_service_from_auth(auth: &str) -> Option<String> {
    let credential = auth.split("Credential=").nth(1)?;
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() >= 4 {
        let svc = parts[3];
        match svc {
            "sqs" => Some("sqs".to_string()),
            "sns" => Some("sns".to_string()),
            "s3" => Some("s3".to_string()),
            _ => None,
        }
    } else {
        None
    }
}

fn extract_region_from_auth(auth: &str) -> Option<String> {
    let credential = auth.split("Credential=").nth(1)?;
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() >= 3 {
        Some(parts[2].to_string())
    } else {
        None
    }
}

fn extract_account_from_auth(auth: &str) -> Option<String> {
    let credential = auth.split("Credential=").nth(1)?;
    let access_key = credential.split('/').next()?;
    if access_key.len() >= 12 && access_key.chars().all(|c| c.is_ascii_digit()) {
        Some(access_key[..12].to_string())
    } else {
        None
    }
}

macro_rules! log_to_target {
    ($target:expr, $is_err:expr, $($arg:tt)*) => {
        if $is_err {
            tracing::warn!(target: $target, $($arg)*);
        } else {
            tracing::info!(target: $target, $($arg)*);
        }
    };
}

fn log_request(service: &str, method: &Method, path: &str, op: &str, status: u16, error_code: Option<&str>, ms: f64) {
    let is_err = error_code.is_some();
    let detail = match error_code {
        Some(code) => format!("{} {} {}.{} -> {} {}  ({:.1}ms)", method, path, service, op, status, code, ms),
        None => format!("{} {} {}.{} -> {}  ({:.1}ms)", method, path, service, op, status, ms),
    };
    match service {
        "s3" => log_to_target!("ferro::s3", is_err, "{}", detail),
        "sqs" => log_to_target!("ferro::sqs", is_err, "{}", detail),
        "sns" => log_to_target!("ferro::sns", is_err, "{}", detail),
        _ => log_to_target!("ferro::http", is_err, "{}", detail),
    }
}

fn build_cors_preflight(origin: Option<&str>, ctx: &RequestContext) -> axum::response::Response {
    let origin = origin.unwrap_or("*");
    let request_method = ctx
        .headers
        .get("access-control-request-method")
        .or_else(|| ctx.headers.get("Access-Control-Request-Method"))
        .cloned()
        .unwrap_or_else(|| "GET, PUT, POST, DELETE, HEAD".to_string());
    let request_headers = ctx
        .headers
        .get("access-control-request-headers")
        .or_else(|| ctx.headers.get("Access-Control-Request-Headers"))
        .cloned()
        .unwrap_or_else(|| "*".to_string());

    axum::response::Response::builder()
        .status(200)
        .header("Access-Control-Allow-Origin", origin)
        .header("Access-Control-Allow-Methods", request_method)
        .header("Access-Control-Allow-Headers", request_headers)
        .header("Access-Control-Expose-Headers", "ETag, x-amz-request-id, x-amz-version-id, x-amz-delete-marker, x-amz-server-side-encryption")
        .header("Access-Control-Max-Age", "3600")
        .header("Access-Control-Allow-Credentials", "true")
        .body(Body::empty())
        .unwrap()
}

fn inject_cors_headers(headers: &mut axum::http::HeaderMap, origin: Option<&str>) {
    let origin = origin.unwrap_or("*");
    headers.insert("Access-Control-Allow-Origin", origin.parse().unwrap());
    headers.insert("Access-Control-Expose-Headers", "ETag, x-amz-request-id, x-amz-version-id, x-amz-delete-marker, x-amz-server-side-encryption".parse().unwrap());
    headers.insert("Access-Control-Allow-Credentials", "true".parse().unwrap());
}

fn build_response(resp: ServiceResponse, request_id: &str) -> axum::response::Response {
    let mut builder = axum::response::Response::builder()
        .status(resp.status)
        .header("x-amzn-requestid", request_id)
        .header("x-amz-request-id", request_id);

    for (k, v) in &resp.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }

    builder.body(Body::from(resp.body)).unwrap()
}

fn build_error_response(
    err: &ServiceException,
    protocol: &str,
    request_id: &str,
    _service: &str,
    _operation: &str,
) -> axum::response::Response {
    let (content_type, body) = match protocol {
        "json" => (
            "application/x-amz-json-1.0",
            serializer::serialize_json_error(err, request_id),
        ),
        "rest-xml" => (
            "text/xml",
            serializer::serialize_rest_xml_error(err, request_id),
        ),
        _ => (
            "text/xml",
            serializer::serialize_query_error(err, request_id),
        ),
    };

    axum::response::Response::builder()
        .status(err.status_code)
        .header("Content-Type", content_type)
        .header("x-amzn-requestid", request_id)
        .header("x-amz-request-id", request_id)
        .body(Body::from(body))
        .unwrap()
}
