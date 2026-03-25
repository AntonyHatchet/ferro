use crate::context::RequestContext;
use std::collections::HashMap;

pub fn parse_query_request(ctx: &RequestContext) -> (String, serde_json::Value) {
    let params = if ctx.body.is_empty() {
        parse_query_string(&ctx.uri)
    } else {
        parse_query_string(&String::from_utf8_lossy(&ctx.body))
    };

    let action = params
        .get("Action")
        .cloned()
        .unwrap_or_default();

    let mut map = serde_json::Map::new();
    for (k, v) in &params {
        if k == "Action" || k == "Version" {
            continue;
        }
        map.insert(k.clone(), serde_json::Value::String(v.clone()));
    }

    (action, serde_json::Value::Object(map))
}

pub fn parse_rest_xml_request(ctx: &RequestContext) -> (String, serde_json::Value) {
    let operation = resolve_s3_operation(ctx);
    let mut params = serde_json::Map::new();

    for (k, v) in &ctx.query_params {
        params.insert(k.clone(), serde_json::Value::String(v.clone()));
    }

    for (k, v) in &ctx.headers {
        let key = k.to_lowercase();
        if key.starts_with("x-amz-") || key == "content-type" || key == "range" {
            params.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
    }

    if !ctx.body.is_empty() {
        if let Ok(body_str) = std::str::from_utf8(&ctx.body) {
            if body_str.trim_start().starts_with('<') {
                if let Ok(parsed) = parse_xml_body(body_str) {
                    for (k, v) in parsed {
                        params.insert(k, v);
                    }
                }
            } else {
                params.insert(
                    "_body".to_string(),
                    serde_json::Value::String(body_str.to_string()),
                );
            }
        }
    }

    (operation, serde_json::Value::Object(params))
}

fn resolve_s3_operation(ctx: &RequestContext) -> String {
    let path = ctx.uri.split('?').next().unwrap_or("/");
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    let has_bucket = !parts.is_empty();
    let has_key = parts.len() > 1;

    let qp = &ctx.query_params;

    match ctx.method.as_str() {
        "PUT" => {
            if qp.contains_key("tagging") {
                if has_key {
                    return "PutObjectTagging".to_string();
                }
                return "PutBucketTagging".to_string();
            }
            if qp.contains_key("acl") {
                if has_key {
                    return "PutObjectAcl".to_string();
                }
                return "PutBucketAcl".to_string();
            }
            if qp.contains_key("versioning") {
                return "PutBucketVersioning".to_string();
            }
            if qp.contains_key("cors") {
                return "PutBucketCors".to_string();
            }
            if qp.contains_key("lifecycle") {
                return "PutBucketLifecycleConfiguration".to_string();
            }
            if qp.contains_key("policy") {
                return "PutBucketPolicy".to_string();
            }
            if qp.contains_key("notification") {
                return "PutBucketNotificationConfiguration".to_string();
            }
            if qp.contains_key("encryption") {
                return "PutBucketEncryption".to_string();
            }
            if qp.contains_key("logging") {
                return "PutBucketLogging".to_string();
            }
            if qp.contains_key("website") {
                return "PutBucketWebsite".to_string();
            }
            if qp.contains_key("replication") {
                return "PutBucketReplication".to_string();
            }
            if qp.contains_key("object-lock") {
                return "PutObjectLockConfiguration".to_string();
            }
            if qp.contains_key("legal-hold") {
                return "PutObjectLegalHold".to_string();
            }
            if qp.contains_key("retention") {
                return "PutObjectRetention".to_string();
            }
            if qp.contains_key("accelerate") {
                return "PutBucketAccelerateConfiguration".to_string();
            }
            if qp.contains_key("intelligent-tiering") {
                return "PutBucketIntelligentTieringConfiguration".to_string();
            }
            if qp.contains_key("inventory") {
                return "PutBucketInventoryConfiguration".to_string();
            }
            if qp.contains_key("metrics") {
                return "PutBucketMetricsConfiguration".to_string();
            }
            if qp.contains_key("analytics") {
                return "PutBucketAnalyticsConfiguration".to_string();
            }
            if qp.contains_key("ownershipControls") {
                return "PutBucketOwnershipControls".to_string();
            }
            if qp.contains_key("publicAccessBlock") {
                return "PutPublicAccessBlock".to_string();
            }
            if ctx.headers.contains_key("x-amz-copy-source") {
                if qp.contains_key("partNumber") && qp.contains_key("uploadId") {
                    return "UploadPartCopy".to_string();
                }
                return "CopyObject".to_string();
            }
            if qp.contains_key("uploadId") && qp.contains_key("partNumber") {
                return "UploadPart".to_string();
            }
            if has_key {
                return "PutObject".to_string();
            }
            "CreateBucket".to_string()
        }
        "GET" => {
            if !has_bucket {
                return "ListBuckets".to_string();
            }
            if qp.contains_key("tagging") {
                if has_key {
                    return "GetObjectTagging".to_string();
                }
                return "GetBucketTagging".to_string();
            }
            if qp.contains_key("acl") {
                if has_key {
                    return "GetObjectAcl".to_string();
                }
                return "GetBucketAcl".to_string();
            }
            if qp.contains_key("versioning") {
                return "GetBucketVersioning".to_string();
            }
            if qp.contains_key("cors") {
                return "GetBucketCors".to_string();
            }
            if qp.contains_key("lifecycle") {
                return "GetBucketLifecycleConfiguration".to_string();
            }
            if qp.contains_key("policy") {
                return "GetBucketPolicy".to_string();
            }
            if qp.contains_key("policyStatus") {
                return "GetBucketPolicyStatus".to_string();
            }
            if qp.contains_key("notification") {
                return "GetBucketNotificationConfiguration".to_string();
            }
            if qp.contains_key("encryption") {
                return "GetBucketEncryption".to_string();
            }
            if qp.contains_key("logging") {
                return "GetBucketLogging".to_string();
            }
            if qp.contains_key("website") {
                return "GetBucketWebsite".to_string();
            }
            if qp.contains_key("location") {
                return "GetBucketLocation".to_string();
            }
            if qp.contains_key("replication") {
                return "GetBucketReplication".to_string();
            }
            if qp.contains_key("object-lock") {
                return "GetObjectLockConfiguration".to_string();
            }
            if qp.contains_key("legal-hold") {
                return "GetObjectLegalHold".to_string();
            }
            if qp.contains_key("retention") {
                return "GetObjectRetention".to_string();
            }
            if qp.contains_key("accelerate") {
                return "GetBucketAccelerateConfiguration".to_string();
            }
            if qp.contains_key("ownershipControls") {
                return "GetBucketOwnershipControls".to_string();
            }
            if qp.contains_key("publicAccessBlock") {
                return "GetPublicAccessBlock".to_string();
            }
            if qp.contains_key("versions") {
                return "ListObjectVersions".to_string();
            }
            if qp.contains_key("uploads") {
                return "ListMultipartUploads".to_string();
            }
            if qp.contains_key("uploadId") {
                return "ListParts".to_string();
            }
            if qp.contains_key("attributes") {
                return "GetObjectAttributes".to_string();
            }
            if has_key {
                return "GetObject".to_string();
            }
            if qp.get("list-type").map(|v| v.as_str()) == Some("2") {
                return "ListObjectsV2".to_string();
            }
            "ListObjects".to_string()
        }
        "HEAD" => {
            if has_key {
                return "HeadObject".to_string();
            }
            "HeadBucket".to_string()
        }
        "DELETE" => {
            if qp.contains_key("tagging") {
                if has_key {
                    return "DeleteObjectTagging".to_string();
                }
                return "DeleteBucketTagging".to_string();
            }
            if qp.contains_key("cors") {
                return "DeleteBucketCors".to_string();
            }
            if qp.contains_key("lifecycle") {
                return "DeleteBucketLifecycle".to_string();
            }
            if qp.contains_key("policy") {
                return "DeleteBucketPolicy".to_string();
            }
            if qp.contains_key("encryption") {
                return "DeleteBucketEncryption".to_string();
            }
            if qp.contains_key("website") {
                return "DeleteBucketWebsite".to_string();
            }
            if qp.contains_key("replication") {
                return "DeleteBucketReplication".to_string();
            }
            if qp.contains_key("ownershipControls") {
                return "DeleteBucketOwnershipControls".to_string();
            }
            if qp.contains_key("publicAccessBlock") {
                return "DeletePublicAccessBlock".to_string();
            }
            if qp.contains_key("uploadId") {
                return "AbortMultipartUpload".to_string();
            }
            if has_key {
                return "DeleteObject".to_string();
            }
            "DeleteBucket".to_string()
        }
        "POST" => {
            if qp.contains_key("delete") {
                return "DeleteObjects".to_string();
            }
            if qp.contains_key("uploads") {
                return "CreateMultipartUpload".to_string();
            }
            if qp.contains_key("uploadId") {
                return "CompleteMultipartUpload".to_string();
            }
            if qp.contains_key("restore") {
                return "RestoreObject".to_string();
            }
            if qp.contains_key("select") && qp.get("select").map(|v| v.as_str()) == Some("") {
                return "SelectObjectContent".to_string();
            }
            "PutObject".to_string()
        }
        _ => "UnknownOperation".to_string(),
    }
}

fn parse_query_string(input: &str) -> HashMap<String, String> {
    let query = if let Some(pos) = input.find('?') {
        &input[pos + 1..]
    } else if input.contains('=') {
        input
    } else {
        return HashMap::new();
    };

    let mut map = HashMap::new();
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            let key = urlencoding::decode(key).unwrap_or_default().to_string();
            let value = urlencoding::decode(value).unwrap_or_default().to_string();
            map.insert(key, value);
        } else if !pair.is_empty() {
            let key = urlencoding::decode(pair).unwrap_or_default().to_string();
            map.insert(key, String::new());
        }
    }
    map
}

fn parse_xml_body(xml: &str) -> Result<Vec<(String, serde_json::Value)>, String> {
    let mut result = Vec::new();
    let trimmed = xml.trim();
    if trimmed.is_empty() {
        return Ok(result);
    }
    result.push((
        "_raw_body".to_string(),
        serde_json::Value::String(trimmed.to_string()),
    ));
    Ok(result)
}

pub fn extract_bucket_and_key(uri: &str) -> (Option<String>, Option<String>) {
    let path = uri.split('?').next().unwrap_or("/");
    let decoded = urlencoding::decode(path).unwrap_or_default();
    let parts: Vec<&str> = decoded.split('/').filter(|s| !s.is_empty()).collect();
    match parts.len() {
        0 => (None, None),
        1 => (Some(parts[0].to_string()), None),
        _ => {
            let bucket = parts[0].to_string();
            let key = parts[1..].join("/");
            (Some(bucket), Some(key))
        }
    }
}

pub fn extract_s3_params(ctx: &RequestContext) -> serde_json::Map<String, serde_json::Value> {
    let mut params = serde_json::Map::new();
    let (bucket, key) = extract_bucket_and_key(&ctx.uri);

    if let Some(b) = bucket {
        params.insert("Bucket".to_string(), serde_json::Value::String(b));
    }
    if let Some(k) = key {
        params.insert("Key".to_string(), serde_json::Value::String(k));
    }
    for (k, v) in &ctx.query_params {
        params.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    for (k, v) in &ctx.headers {
        let lk = k.to_lowercase();
        if lk.starts_with("x-amz-") || lk == "content-type" || lk == "range"
            || lk == "content-md5" || lk == "content-encoding" || lk == "content-disposition"
            || lk == "content-language" || lk == "cache-control" || lk == "expires"
            || lk == "if-match" || lk == "if-none-match" || lk == "if-modified-since"
            || lk == "if-unmodified-since"
        {
            params.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
    }

    params
}

pub fn parse_sqs_json_request(ctx: &RequestContext) -> (String, serde_json::Value) {
    let target = ctx
        .headers
        .get("x-amz-target")
        .or_else(|| ctx.headers.get("X-Amz-Target"))
        .cloned()
        .unwrap_or_default();

    let operation = target
        .split('.')
        .last()
        .unwrap_or(&target)
        .to_string();

    let params: serde_json::Value = if ctx.body.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice(&ctx.body).unwrap_or(serde_json::Value::Object(serde_json::Map::new()))
    };

    (operation, params)
}

pub fn detect_protocol(ctx: &RequestContext) -> &'static str {
    let content_type = ctx
        .headers
        .get("content-type")
        .or_else(|| ctx.headers.get("Content-Type"))
        .map(|s| s.as_str())
        .unwrap_or("");

    if content_type.contains("x-amz-json") || ctx.headers.contains_key("x-amz-target")
        || ctx.headers.contains_key("X-Amz-Target")
    {
        return "json";
    }
    if content_type == "application/x-www-form-urlencoded"
        || ctx.body.starts_with(b"Action=")
    {
        return "query";
    }
    "rest-xml"
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;

    #[test]
    fn parse_query_string_basic() {
        let result = parse_query_string("Action=CreateQueue&QueueName=test");
        assert_eq!(result.get("Action").unwrap(), "CreateQueue");
        assert_eq!(result.get("QueueName").unwrap(), "test");
    }

    #[test]
    fn parse_query_string_url_encoded() {
        let result = parse_query_string("Key=hello%20world&Other=a%26b");
        assert_eq!(result.get("Key").unwrap(), "hello world");
        assert_eq!(result.get("Other").unwrap(), "a&b");
    }

    #[test]
    fn parse_query_string_empty() {
        let result = parse_query_string("");
        assert!(result.is_empty());
    }

    #[test]
    fn parse_query_string_with_question_mark() {
        let result = parse_query_string("/path?versioning");
        assert!(result.contains_key("versioning"));
    }

    #[test]
    fn extract_bucket_and_key_nested() {
        let (b, k) = extract_bucket_and_key("/my-bucket/some/key.txt");
        assert_eq!(b.unwrap(), "my-bucket");
        assert_eq!(k.unwrap(), "some/key.txt");
    }

    #[test]
    fn extract_bucket_only() {
        let (b, k) = extract_bucket_and_key("/my-bucket");
        assert_eq!(b.unwrap(), "my-bucket");
        assert!(k.is_none());
    }

    #[test]
    fn extract_root_path() {
        let (b, k) = extract_bucket_and_key("/");
        assert!(b.is_none());
        assert!(k.is_none());
    }

    #[test]
    fn extract_bucket_with_query_string() {
        let (b, k) = extract_bucket_and_key("/my-bucket?versioning");
        assert_eq!(b.unwrap(), "my-bucket");
        assert!(k.is_none());
    }

    fn make_ctx(method: &str, uri: &str) -> RequestContext {
        let mut ctx = RequestContext::new();
        ctx.method = method.to_string();
        ctx.uri = uri.to_string();
        ctx
    }

    fn make_ctx_with_qp(method: &str, uri: &str, qp: Vec<(&str, &str)>) -> RequestContext {
        let mut ctx = make_ctx(method, uri);
        for (k, v) in qp {
            ctx.query_params.insert(k.to_string(), v.to_string());
        }
        ctx
    }

    #[test]
    fn s3_operation_put_bucket() {
        let ctx = make_ctx("PUT", "/my-bucket");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "CreateBucket");
    }

    #[test]
    fn s3_operation_put_object() {
        let ctx = make_ctx("PUT", "/my-bucket/key.txt");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "PutObject");
    }

    #[test]
    fn s3_operation_get_object() {
        let ctx = make_ctx("GET", "/my-bucket/key.txt");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "GetObject");
    }

    #[test]
    fn s3_operation_list_buckets() {
        let ctx = make_ctx("GET", "/");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "ListBuckets");
    }

    #[test]
    fn s3_operation_list_objects_v2() {
        let ctx = make_ctx_with_qp("GET", "/my-bucket", vec![("list-type", "2")]);
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "ListObjectsV2");
    }

    #[test]
    fn s3_operation_list_objects_v1() {
        let ctx = make_ctx("GET", "/my-bucket");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "ListObjects");
    }

    #[test]
    fn s3_operation_delete_object() {
        let ctx = make_ctx("DELETE", "/my-bucket/key.txt");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "DeleteObject");
    }

    #[test]
    fn s3_operation_delete_bucket() {
        let ctx = make_ctx("DELETE", "/my-bucket");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "DeleteBucket");
    }

    #[test]
    fn s3_operation_head_object() {
        let ctx = make_ctx("HEAD", "/my-bucket/key.txt");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "HeadObject");
    }

    #[test]
    fn s3_operation_head_bucket() {
        let ctx = make_ctx("HEAD", "/my-bucket");
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "HeadBucket");
    }

    #[test]
    fn s3_operation_versioning() {
        let ctx = make_ctx_with_qp("PUT", "/my-bucket", vec![("versioning", "")]);
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "PutBucketVersioning");
    }

    #[test]
    fn s3_operation_multipart_create() {
        let ctx = make_ctx_with_qp("POST", "/my-bucket/key.txt", vec![("uploads", "")]);
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "CreateMultipartUpload");
    }

    #[test]
    fn s3_operation_copy_object() {
        let mut ctx = make_ctx("PUT", "/my-bucket/key.txt");
        ctx.headers.insert("x-amz-copy-source".to_string(), "/src-bucket/src-key".to_string());
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "CopyObject");
    }

    #[test]
    fn s3_operation_delete_objects_batch() {
        let ctx = make_ctx_with_qp("POST", "/my-bucket", vec![("delete", "")]);
        let (op, _) = parse_rest_xml_request(&ctx);
        assert_eq!(op, "DeleteObjects");
    }

    #[test]
    fn detect_protocol_query() {
        let mut ctx = RequestContext::new();
        ctx.headers.insert("content-type".to_string(), "application/x-www-form-urlencoded".to_string());
        assert_eq!(detect_protocol(&ctx), "query");
    }

    #[test]
    fn detect_protocol_json() {
        let mut ctx = RequestContext::new();
        ctx.headers.insert("x-amz-target".to_string(), "AmazonSQS.SendMessage".to_string());
        assert_eq!(detect_protocol(&ctx), "json");
    }

    #[test]
    fn detect_protocol_rest_xml_default() {
        let ctx = RequestContext::new();
        assert_eq!(detect_protocol(&ctx), "rest-xml");
    }

    #[test]
    fn detect_protocol_body_action() {
        let mut ctx = RequestContext::new();
        ctx.body = Bytes::from("Action=CreateQueue&QueueName=test");
        assert_eq!(detect_protocol(&ctx), "query");
    }

    #[test]
    fn parse_sqs_json_request_extracts_operation() {
        let mut ctx = RequestContext::new();
        ctx.headers.insert("x-amz-target".to_string(), "AmazonSQS.SendMessage".to_string());
        ctx.body = Bytes::from(r#"{"QueueUrl":"http://localhost/q","MessageBody":"hi"}"#);
        let (op, params) = parse_sqs_json_request(&ctx);
        assert_eq!(op, "SendMessage");
        assert_eq!(params.get("MessageBody").unwrap().as_str().unwrap(), "hi");
    }

    #[test]
    fn parse_query_request_extracts_action() {
        let mut ctx = RequestContext::new();
        ctx.body = Bytes::from("Action=CreateQueue&QueueName=my-queue&Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=60");
        let (action, params) = parse_query_request(&ctx);
        assert_eq!(action, "CreateQueue");
        assert_eq!(params.get("QueueName").unwrap().as_str().unwrap(), "my-queue");
    }

    #[test]
    fn extract_s3_params_includes_bucket_key_and_headers() {
        let mut ctx = RequestContext::new();
        ctx.uri = "/my-bucket/my-key.txt".to_string();
        ctx.headers.insert("x-amz-acl".to_string(), "public-read".to_string());
        ctx.headers.insert("user-agent".to_string(), "test".to_string());
        let params = extract_s3_params(&ctx);
        assert_eq!(params.get("Bucket").unwrap().as_str().unwrap(), "my-bucket");
        assert_eq!(params.get("Key").unwrap().as_str().unwrap(), "my-key.txt");
        assert!(params.contains_key("x-amz-acl"));
        assert!(!params.contains_key("user-agent"));
    }
}
