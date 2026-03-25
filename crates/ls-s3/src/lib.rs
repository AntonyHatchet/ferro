pub mod models;
pub mod provider;

pub use provider::S3Service;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ls_asf::context::RequestContext;
    use ls_asf::service::ServiceHandler;

    fn ctx(operation: &str, bucket: &str) -> RequestContext {
        let mut c = RequestContext::new();
        c.service_name = "s3".to_string();
        c.operation = operation.to_string();
        c.region = "us-east-1".to_string();
        c.account_id = "000000000000".to_string();
        c.uri = format!("/{bucket}");
        c.method = "PUT".to_string();
        c
    }

    async fn create_bucket(svc: &S3Service, name: &str) {
        let c = ctx("CreateBucket", name);
        let params = serde_json::json!({ "Bucket": name });
        svc.handle(c, params).await.unwrap();
    }

    #[tokio::test]
    async fn put_and_get_cors() {
        let svc = S3Service::new();
        create_bucket(&svc, "cors-test").await;

        let cors_xml = r#"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedHeader>*</AllowedHeader><MaxAgeSeconds>3600</MaxAgeSeconds></CORSRule></CORSConfiguration>"#;

        let mut put_ctx = ctx("PutBucketCors", "cors-test");
        put_ctx.body = Bytes::from(cors_xml);
        let params = serde_json::json!({ "Bucket": "cors-test" });
        let resp = svc.handle(put_ctx, params).await.unwrap();
        assert_eq!(resp.status, 200);

        let get_ctx = ctx("GetBucketCors", "cors-test");
        let params = serde_json::json!({ "Bucket": "cors-test" });
        let resp = svc.handle(get_ctx, params).await.unwrap();
        assert_eq!(resp.status, 200);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<AllowedOrigin>*</AllowedOrigin>"));
        assert!(body.contains("<AllowedMethod>GET</AllowedMethod>"));
        assert!(body.contains("<AllowedMethod>PUT</AllowedMethod>"));
        assert!(body.contains("<AllowedHeader>*</AllowedHeader>"));
        assert!(body.contains("<MaxAgeSeconds>3600</MaxAgeSeconds>"));
    }

    #[tokio::test]
    async fn get_cors_default_returns_empty_config() {
        let svc = S3Service::new();
        create_bucket(&svc, "no-cors").await;

        let get_ctx = ctx("GetBucketCors", "no-cors");
        let params = serde_json::json!({ "Bucket": "no-cors" });
        let resp = svc.handle(get_ctx, params).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<CORSConfiguration/>"));
    }

    #[tokio::test]
    async fn delete_cors() {
        let svc = S3Service::new();
        create_bucket(&svc, "del-cors").await;

        let cors_xml = "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
        let mut put_ctx = ctx("PutBucketCors", "del-cors");
        put_ctx.body = Bytes::from(cors_xml);
        svc.handle(put_ctx, serde_json::json!({ "Bucket": "del-cors" })).await.unwrap();

        let del_ctx = ctx("DeleteBucketCors", "del-cors");
        let resp = svc.handle(del_ctx, serde_json::json!({ "Bucket": "del-cors" })).await.unwrap();
        assert_eq!(resp.status, 204);

        let get_ctx = ctx("GetBucketCors", "del-cors");
        let resp = svc.handle(get_ctx, serde_json::json!({ "Bucket": "del-cors" })).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<CORSConfiguration/>"));
    }

    #[tokio::test]
    async fn cors_multiple_rules() {
        let svc = S3Service::new();
        create_bucket(&svc, "multi-cors").await;

        let cors_xml = concat!(
            "<CORSConfiguration>",
            "<CORSRule><AllowedOrigin>https://app.example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedHeader>Authorization</AllowedHeader></CORSRule>",
            "<CORSRule><AllowedOrigin>https://admin.example.com</AllowedOrigin><AllowedMethod>PUT</AllowedMethod><AllowedMethod>DELETE</AllowedMethod><AllowedHeader>*</AllowedHeader><ExposeHeader>ETag</ExposeHeader></CORSRule>",
            "</CORSConfiguration>"
        );

        let mut put_ctx = ctx("PutBucketCors", "multi-cors");
        put_ctx.body = Bytes::from(cors_xml);
        svc.handle(put_ctx, serde_json::json!({ "Bucket": "multi-cors" })).await.unwrap();

        let get_ctx = ctx("GetBucketCors", "multi-cors");
        let resp = svc.handle(get_ctx, serde_json::json!({ "Bucket": "multi-cors" })).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("https://app.example.com"));
        assert!(body.contains("https://admin.example.com"));
        assert!(body.contains("<ExposeHeader>ETag</ExposeHeader>"));
    }

    #[tokio::test]
    async fn cors_on_nonexistent_bucket_errors() {
        let svc = S3Service::new();
        let get_ctx = ctx("GetBucketCors", "ghost");
        let err = svc.handle(get_ctx, serde_json::json!({ "Bucket": "ghost" })).await.unwrap_err();
        assert_eq!(err.status_code, 404);
        assert_eq!(err.code, "NoSuchBucket");
    }
}
