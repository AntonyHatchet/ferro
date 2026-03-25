use ls_asf::context::RequestContext;
use ls_asf::service::ServiceRegistry;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Deserialize, Default)]
pub struct InitConfig {
    #[serde(default)]
    pub sqs: Vec<SqsQueueInit>,
    #[serde(default)]
    pub sns: Vec<SnsTopicInit>,
    #[serde(default)]
    pub s3: Vec<S3BucketInit>,
}

#[derive(Debug, Deserialize)]
pub struct SqsQueueInit {
    pub name: String,
    #[serde(default)]
    pub attributes: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub tags: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct SnsTopicInit {
    pub name: String,
    #[serde(default)]
    pub attributes: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub subscriptions: Vec<SnsSubscriptionInit>,
}

#[derive(Debug, Deserialize)]
pub struct SnsSubscriptionInit {
    pub protocol: String,
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct S3BucketInit {
    pub name: String,
    #[serde(default)]
    pub seed_dir: Option<String>,
    #[serde(default)]
    pub versioning: bool,
}

pub fn data_dir() -> PathBuf {
    std::env::var("FERRO_DATA_DIR")
        .or_else(|_| std::env::var("FERROSTACK_DATA_DIR"))
        .or_else(|_| std::env::var("LOCALSTACK_DATA_DIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/lib/ferro"))
}

pub fn init_scripts_root() -> PathBuf {
    std::env::var("FERRO_INIT_DIR")
        .or_else(|_| std::env::var("FERROSTACK_INIT_DIR"))
        .or_else(|_| std::env::var("LOCALSTACK_INIT_DIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            for candidate in &["/etc/ferro/init", "/etc/ferrostack/init", "/etc/localstack/init"] {
                let p = PathBuf::from(candidate);
                if p.exists() {
                    return p;
                }
            }
            PathBuf::from("./init")
        })
}

pub fn ensure_dirs() {
    let dir = data_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        tracing::warn!("Could not create data dir {}: {e}", dir.display());
    }

    let init_root = init_scripts_root();
    for sub in &["boot.d", "start.d", "ready.d", "shutdown.d"] {
        let p = init_root.join(sub);
        if let Err(e) = std::fs::create_dir_all(&p) {
            tracing::debug!("Could not create init dir {}: {e}", p.display());
        }
    }

    tracing::info!("Data dir: {}", dir.display());
    tracing::info!("Init scripts dir: {}", init_root.display());
}

pub async fn run_init_config(registry: &Arc<ServiceRegistry>) {
    let config_path = resolve_config_path();
    let config = match config_path {
        Some(p) => match load_config(&p) {
            Ok(c) => {
                tracing::info!("Loaded init config from {}", p.display());
                c
            }
            Err(e) => {
                tracing::error!("Failed to load init config {}: {e}", p.display());
                return;
            }
        },
        None => {
            tracing::debug!("No init config found, skipping declarative init");
            return;
        }
    };

    let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let account =
        std::env::var("AWS_ACCOUNT_ID").unwrap_or_else(|_| "000000000000".to_string());

    create_sqs_queues(registry, &config.sqs, &region, &account).await;
    create_sns_topics(registry, &config.sns, &region, &account).await;
    create_s3_buckets(registry, &config.s3, &region, &account).await;
}

fn resolve_config_path() -> Option<PathBuf> {
    for var in &["FERRO_INIT_CONFIG", "FERROSTACK_INIT_CONFIG", "LOCALSTACK_INIT_CONFIG"] {
        if let Ok(p) = std::env::var(var) {
            let path = PathBuf::from(p);
            if path.exists() {
                return Some(path);
            }
        }
    }

    let candidates = [
        init_scripts_root().join("init.json"),
        PathBuf::from("./init.json"),
        PathBuf::from("/etc/ferro/init.json"),
        PathBuf::from("/etc/ferrostack/init.json"),
        PathBuf::from("/etc/localstack/init.json"),
    ];

    for p in &candidates {
        if p.exists() {
            return Some(p.clone());
        }
    }
    None
}

fn load_config(path: &Path) -> Result<InitConfig, String> {
    let content = std::fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
    serde_json::from_str(&content).map_err(|e| format!("parse: {e}"))
}

async fn create_sqs_queues(
    registry: &Arc<ServiceRegistry>,
    queues: &[SqsQueueInit],
    region: &str,
    account: &str,
) {
    let handler = match registry.get("sqs") {
        Some(h) => h,
        None => return,
    };

    for q in queues {
        let mut ctx = RequestContext::new();
        ctx.service_name = "sqs".to_string();
        ctx.operation = "CreateQueue".to_string();
        ctx.region = region.to_string();
        ctx.account_id = account.to_string();

        let mut params = serde_json::Map::new();
        params.insert(
            "QueueName".to_string(),
            serde_json::Value::String(q.name.clone()),
        );
        if !q.attributes.is_empty() {
            let attrs: serde_json::Map<String, serde_json::Value> = q
                .attributes
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            params.insert("Attributes".to_string(), serde_json::Value::Object(attrs));
        }
        if !q.tags.is_empty() {
            let tags: serde_json::Map<String, serde_json::Value> = q
                .tags
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            params.insert("Tags".to_string(), serde_json::Value::Object(tags));
        }

        match handler
            .handle(ctx, serde_json::Value::Object(params))
            .await
        {
            Ok(_) => tracing::info!("  [sqs] Created queue: {}", q.name),
            Err(e) => tracing::error!("  [sqs] Failed to create queue {}: {e}", q.name),
        }
    }
}

async fn create_sns_topics(
    registry: &Arc<ServiceRegistry>,
    topics: &[SnsTopicInit],
    region: &str,
    account: &str,
) {
    let handler = match registry.get("sns") {
        Some(h) => h,
        None => return,
    };

    for t in topics {
        let mut ctx = RequestContext::new();
        ctx.service_name = "sns".to_string();
        ctx.operation = "CreateTopic".to_string();
        ctx.region = region.to_string();
        ctx.account_id = account.to_string();

        let mut params = serde_json::Map::new();
        params.insert(
            "Name".to_string(),
            serde_json::Value::String(t.name.clone()),
        );
        if !t.attributes.is_empty() {
            let attrs: serde_json::Map<String, serde_json::Value> = t
                .attributes
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            params.insert("Attributes".to_string(), serde_json::Value::Object(attrs));
        }

        match handler
            .handle(ctx, serde_json::Value::Object(params))
            .await
        {
            Ok(_) => tracing::info!("  [sns] Created topic: {}", t.name),
            Err(e) => tracing::error!("  [sns] Failed to create topic {}: {e}", t.name),
        }

        let topic_arn = format!("arn:aws:sns:{region}:{account}:{}", t.name);
        for sub in &t.subscriptions {
            let mut sub_ctx = RequestContext::new();
            sub_ctx.service_name = "sns".to_string();
            sub_ctx.operation = "Subscribe".to_string();
            sub_ctx.region = region.to_string();
            sub_ctx.account_id = account.to_string();

            let mut sub_params = serde_json::Map::new();
            sub_params.insert(
                "TopicArn".to_string(),
                serde_json::Value::String(topic_arn.clone()),
            );
            sub_params.insert(
                "Protocol".to_string(),
                serde_json::Value::String(sub.protocol.clone()),
            );
            sub_params.insert(
                "Endpoint".to_string(),
                serde_json::Value::String(sub.endpoint.clone()),
            );

            match handler
                .handle(sub_ctx, serde_json::Value::Object(sub_params))
                .await
            {
                Ok(_) => tracing::info!(
                    "  [sns] Subscribed {} -> {}:{}",
                    t.name,
                    sub.protocol,
                    sub.endpoint
                ),
                Err(e) => tracing::error!("  [sns] Failed to subscribe to {}: {e}", t.name),
            }
        }
    }
}

async fn create_s3_buckets(
    registry: &Arc<ServiceRegistry>,
    buckets: &[S3BucketInit],
    region: &str,
    account: &str,
) {
    let handler = match registry.get("s3") {
        Some(h) => h,
        None => return,
    };

    for b in buckets {
        let mut ctx = RequestContext::new();
        ctx.service_name = "s3".to_string();
        ctx.operation = "CreateBucket".to_string();
        ctx.region = region.to_string();
        ctx.account_id = account.to_string();
        ctx.uri = format!("/{}", b.name);
        ctx.method = "PUT".to_string();

        let params = serde_json::json!({ "Bucket": &b.name });

        match handler.handle(ctx, params).await {
            Ok(_) => tracing::info!("  [s3] Created bucket: {}", b.name),
            Err(e) => tracing::error!("  [s3] Failed to create bucket {}: {e}", b.name),
        }

        if b.versioning {
            let mut v_ctx = RequestContext::new();
            v_ctx.service_name = "s3".to_string();
            v_ctx.operation = "PutBucketVersioning".to_string();
            v_ctx.region = region.to_string();
            v_ctx.account_id = account.to_string();
            v_ctx.uri = format!("/{}?versioning", b.name);
            v_ctx.body =
                axum::body::Bytes::from("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>");

            let params = serde_json::json!({ "Bucket": &b.name });
            let _ = handler.handle(v_ctx, params).await;
            tracing::info!("  [s3] Enabled versioning on: {}", b.name);
        }

        if let Some(ref seed_dir) = b.seed_dir {
            seed_bucket(registry, &b.name, seed_dir, region, account).await;
        }
    }
}

async fn seed_bucket(
    registry: &Arc<ServiceRegistry>,
    bucket: &str,
    seed_dir: &str,
    region: &str,
    account: &str,
) {
    let handler = match registry.get("s3") {
        Some(h) => h,
        None => return,
    };

    let seed_path = Path::new(seed_dir);
    if !seed_path.is_dir() {
        tracing::warn!("  [s3] Seed dir does not exist: {seed_dir}");
        return;
    }

    let walker = walkdir(seed_path);
    for entry in walker {
        let rel = entry
            .strip_prefix(seed_path)
            .unwrap_or(&entry)
            .to_string_lossy()
            .to_string();
        if rel.is_empty() {
            continue;
        }

        let data = match std::fs::read(&entry) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("  [s3] Could not read {}: {e}", entry.display());
                continue;
            }
        };

        let content_type = guess_content_type(&rel);

        let mut ctx = RequestContext::new();
        ctx.service_name = "s3".to_string();
        ctx.operation = "PutObject".to_string();
        ctx.region = region.to_string();
        ctx.account_id = account.to_string();
        ctx.uri = format!("/{bucket}/{rel}");
        ctx.method = "PUT".to_string();
        ctx.body = axum::body::Bytes::from(data);
        ctx.headers
            .insert("content-type".to_string(), content_type);

        let params = serde_json::json!({ "Bucket": bucket, "Key": &rel });
        match handler.handle(ctx, params).await {
            Ok(_) => tracing::info!("  [s3] Seeded {bucket}/{rel}"),
            Err(e) => tracing::error!("  [s3] Failed to seed {bucket}/{rel}: {e}"),
        }
    }
}

fn walkdir(root: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                result.push(path);
            } else if path.is_dir() {
                result.extend(walkdir(&path));
            }
        }
    }
    result.sort();
    result
}

fn guess_content_type(filename: &str) -> String {
    let ext = filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" => "application/javascript",
        "json" => "application/json",
        "xml" => "application/xml",
        "txt" => "text/plain",
        "csv" => "text/csv",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" | "gzip" => "application/gzip",
        "tar" => "application/x-tar",
        "yaml" | "yml" => "application/x-yaml",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
    .to_string()
}

pub fn run_ready_scripts() {
    let root = init_scripts_root();
    let ready_dir = root.join("ready.d");

    if !ready_dir.is_dir() {
        tracing::debug!("No ready.d directory at {}", ready_dir.display());
        return;
    }

    let mut scripts: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&ready_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                scripts.push(path);
            }
        }
    }
    scripts.sort();

    if scripts.is_empty() {
        tracing::debug!("No scripts in {}", ready_dir.display());
        return;
    }

    tracing::info!("Running {} ready.d scripts...", scripts.len());
    for script in &scripts {
        run_script(script);
    }
}

fn run_script(path: &Path) {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    let path_str = path.display().to_string();
    tracing::info!("  Running: {path_str}");

    let result = match ext {
        "sh" | "bash" => std::process::Command::new("bash")
            .arg(path)
            .env(
                "AWS_ENDPOINT_URL",
                format!(
                    "http://localhost:{}",
                    std::env::var("GATEWAY_LISTEN")
                        .ok()
                        .and_then(|s| s.split(':').last().map(|p| p.to_string()))
                        .unwrap_or_else(|| "4566".to_string())
                ),
            )
            .env("AWS_DEFAULT_REGION", std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string()))
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .status(),
        "py" | "python" => std::process::Command::new("python3")
            .arg(path)
            .env(
                "AWS_ENDPOINT_URL",
                format!(
                    "http://localhost:{}",
                    std::env::var("GATEWAY_LISTEN")
                        .ok()
                        .and_then(|s| s.split(':').last().map(|p| p.to_string()))
                        .unwrap_or_else(|| "4566".to_string())
                ),
            )
            .status(),
        _ => {
            if is_executable(path) {
                std::process::Command::new(path)
                    .env(
                        "AWS_ENDPOINT_URL",
                        format!(
                            "http://localhost:{}",
                            std::env::var("GATEWAY_LISTEN")
                                .ok()
                                .and_then(|s| s.split(':').last().map(|p| p.to_string()))
                                .unwrap_or_else(|| "4566".to_string())
                        ),
                    )
                    .status()
            } else {
                tracing::warn!("  Skipping {path_str}: unknown extension and not executable");
                return;
            }
        }
    };

    match result {
        Ok(status) if status.success() => {
            tracing::info!("  Completed: {path_str}");
        }
        Ok(status) => {
            tracing::error!("  Failed: {path_str} (exit code: {:?})", status.code());
        }
        Err(e) => {
            tracing::error!("  Error running {path_str}: {e}");
        }
    }
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(_path: &Path) -> bool {
    false
}
