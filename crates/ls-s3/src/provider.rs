use crate::models::*;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use ls_asf::context::RequestContext;
use ls_asf::error::ServiceException;
use ls_asf::parser;
use ls_asf::service::{HandlerFuture, ServiceHandler, ServiceResponse};
use ls_store::AccountRegionBundle;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct S3Service {
    stores: AccountRegionBundle<S3Store>,
}

impl S3Service {
    pub fn new() -> Self {
        Self {
            stores: AccountRegionBundle::new(),
        }
    }

    fn get_store(&self, account_id: &str, region: &str) -> Arc<S3Store> {
        self.stores.get(account_id, region)
    }

    fn require_bucket(
        &self,
        ctx: &RequestContext,
        bucket_name: &str,
    ) -> Result<Arc<S3Bucket>, ServiceException> {
        let store = self.get_store(&ctx.account_id, &ctx.region);
        store
            .buckets
            .get(bucket_name)
            .map(|e| e.value().clone())
            .ok_or_else(|| {
                ServiceException::new("NoSuchBucket", format!("The specified bucket does not exist: {bucket_name}"), 404)
            })
    }
}

impl ServiceHandler for S3Service {
    fn service_name(&self) -> &str {
        "s3"
    }

    fn handle(&self, ctx: RequestContext, params: serde_json::Value) -> HandlerFuture {
        let result = self.dispatch(ctx, params);
        Box::pin(async move { result })
    }
}

impl S3Service {
    fn dispatch(
        &self,
        ctx: RequestContext,
        params: serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        match ctx.operation.as_str() {
            "ListBuckets" => self.list_buckets(&ctx),
            "CreateBucket" => self.create_bucket(&ctx, &params),
            "DeleteBucket" => self.delete_bucket(&ctx, &params),
            "HeadBucket" => self.head_bucket(&ctx, &params),
            "PutObject" => self.put_object(&ctx, &params),
            "GetObject" => self.get_object(&ctx, &params),
            "HeadObject" => self.head_object(&ctx, &params),
            "DeleteObject" => self.delete_object(&ctx, &params),
            "DeleteObjects" => self.delete_objects(&ctx, &params),
            "CopyObject" => self.copy_object(&ctx, &params),
            "ListObjects" => self.list_objects(&ctx, &params),
            "ListObjectsV2" => self.list_objects_v2(&ctx, &params),
            "ListObjectVersions" => self.list_object_versions(&ctx, &params),
            "CreateMultipartUpload" => self.create_multipart_upload(&ctx, &params),
            "UploadPart" => self.upload_part(&ctx, &params),
            "CompleteMultipartUpload" => self.complete_multipart_upload(&ctx, &params),
            "AbortMultipartUpload" => self.abort_multipart_upload(&ctx, &params),
            "ListMultipartUploads" => self.list_multipart_uploads(&ctx, &params),
            "ListParts" => self.list_parts(&ctx, &params),
            "PutBucketVersioning" => self.put_bucket_versioning(&ctx, &params),
            "GetBucketVersioning" => self.get_bucket_versioning(&ctx, &params),
            "PutBucketPolicy" => self.put_bucket_policy(&ctx, &params),
            "GetBucketPolicy" => self.get_bucket_policy(&ctx, &params),
            "DeleteBucketPolicy" => self.delete_bucket_policy(&ctx, &params),
            "GetBucketPolicyStatus" => self.get_bucket_policy_status(&ctx, &params),
            "PutBucketCors" => self.put_bucket_cors(&ctx, &params),
            "GetBucketCors" => self.get_bucket_cors(&ctx, &params),
            "DeleteBucketCors" => self.delete_bucket_cors(&ctx, &params),
            "PutBucketLifecycleConfiguration" => self.put_bucket_lifecycle(&ctx, &params),
            "GetBucketLifecycleConfiguration" => self.get_bucket_lifecycle(&ctx, &params),
            "DeleteBucketLifecycle" => self.delete_bucket_lifecycle(&ctx, &params),
            "PutBucketNotificationConfiguration" => self.put_bucket_notification(&ctx, &params),
            "GetBucketNotificationConfiguration" => self.get_bucket_notification(&ctx, &params),
            "PutBucketEncryption" => self.put_bucket_encryption(&ctx, &params),
            "GetBucketEncryption" => self.get_bucket_encryption(&ctx, &params),
            "DeleteBucketEncryption" => self.delete_bucket_encryption(&ctx, &params),
            "PutBucketTagging" => self.put_bucket_tagging(&ctx, &params),
            "GetBucketTagging" => self.get_bucket_tagging(&ctx, &params),
            "DeleteBucketTagging" => self.delete_bucket_tagging(&ctx, &params),
            "PutObjectTagging" => self.put_object_tagging(&ctx, &params),
            "GetObjectTagging" => self.get_object_tagging(&ctx, &params),
            "DeleteObjectTagging" => self.delete_object_tagging(&ctx, &params),
            "PutBucketAcl" => self.put_bucket_acl(&ctx, &params),
            "GetBucketAcl" => self.get_bucket_acl(&ctx, &params),
            "PutObjectAcl" => self.put_object_acl(&ctx, &params),
            "GetObjectAcl" => self.get_object_acl(&ctx, &params),
            "PutBucketLogging" => self.put_bucket_logging(&ctx, &params),
            "GetBucketLogging" => self.get_bucket_logging(&ctx, &params),
            "PutBucketWebsite" => self.put_bucket_website(&ctx, &params),
            "GetBucketWebsite" => self.get_bucket_website(&ctx, &params),
            "DeleteBucketWebsite" => self.delete_bucket_website(&ctx, &params),
            "GetBucketLocation" => self.get_bucket_location(&ctx, &params),
            "PutBucketReplication" => self.put_bucket_replication(&ctx, &params),
            "GetBucketReplication" => self.get_bucket_replication(&ctx, &params),
            "DeleteBucketReplication" => self.delete_bucket_replication(&ctx, &params),
            "PutObjectLockConfiguration" => self.put_object_lock_config(&ctx, &params),
            "GetObjectLockConfiguration" => self.get_object_lock_config(&ctx, &params),
            "PutObjectLegalHold" => self.put_object_legal_hold(&ctx, &params),
            "GetObjectLegalHold" => self.get_object_legal_hold(&ctx, &params),
            "PutObjectRetention" => self.put_object_retention(&ctx, &params),
            "GetObjectRetention" => self.get_object_retention(&ctx, &params),
            "PutPublicAccessBlock" => self.put_public_access_block(&ctx, &params),
            "GetPublicAccessBlock" => self.get_public_access_block(&ctx, &params),
            "DeletePublicAccessBlock" => self.delete_public_access_block(&ctx, &params),
            "PutBucketAccelerateConfiguration" => self.put_bucket_accelerate(&ctx, &params),
            "GetBucketAccelerateConfiguration" => self.get_bucket_accelerate(&ctx, &params),
            "PutBucketOwnershipControls" => self.put_bucket_ownership(&ctx, &params),
            "GetBucketOwnershipControls" => self.get_bucket_ownership(&ctx, &params),
            "DeleteBucketOwnershipControls" => self.delete_bucket_ownership(&ctx, &params),
            "GetObjectAttributes" => self.get_object_attributes(&ctx, &params),
            "UploadPartCopy" => self.upload_part_copy(&ctx, &params),
            "RestoreObject" => self.restore_object(&ctx, &params),
            "PutBucketIntelligentTieringConfiguration" | "PutBucketInventoryConfiguration"
            | "PutBucketMetricsConfiguration" | "PutBucketAnalyticsConfiguration" => {
                Ok(ServiceResponse::xml(200, ""))
            }
            op => Err(ServiceException::new(
                "NotImplemented",
                format!("Operation {op} not implemented."),
                501,
            )),
        }
    }

    fn list_buckets(&self, ctx: &RequestContext) -> Result<ServiceResponse, ServiceException> {
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
        xml.push_str(&format!("<Owner><ID>{}</ID><DisplayName>ferro</DisplayName></Owner>", ctx.account_id));
        xml.push_str("<Buckets>");
        for entry in store.buckets.iter() {
            let b: &Arc<S3Bucket> = entry.value();
            xml.push_str(&format!(
                "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
                b.name,
                b.creation_date.to_rfc3339()
            ));
        }
        xml.push_str("</Buckets></ListAllMyBucketsResult>");
        Ok(ServiceResponse::xml(200, xml))
    }

    fn create_bucket(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let (bucket_name, _) = parser::extract_bucket_and_key(&ctx.uri);
        let bucket_name = bucket_name.ok_or_else(|| {
            ServiceException::new("InvalidBucketName", "Bucket name is required.", 400)
        })?;
        let store = self.get_store(&ctx.account_id, &ctx.region);
        if store.buckets.contains_key(&bucket_name) {
            return Err(ServiceException::new(
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded.",
                409,
            ));
        }
        let bucket = Arc::new(S3Bucket::new(
            bucket_name.clone(),
            ctx.region.clone(),
            ctx.account_id.clone(),
        ));
        store.buckets.insert(bucket_name.clone(), bucket);
        Ok(ServiceResponse::xml(200, "")
            .with_header("Location", format!("/{bucket_name}")))
    }

    fn delete_bucket(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        if !bucket.objects.is_empty() {
            return Err(ServiceException::new(
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty.",
                409,
            ));
        }
        let store = self.get_store(&ctx.account_id, &ctx.region);
        store.buckets.remove(&bucket_name);
        Ok(ServiceResponse::xml(204, ""))
    }

    fn head_bucket(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        self.require_bucket(ctx, &bucket_name)?;
        Ok(ServiceResponse::xml(200, "")
            .with_header("x-amz-bucket-region", ctx.region.clone()))
    }

    fn put_object(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let key = get_key(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;

        let content_type = ctx
            .headers
            .get("content-type")
            .or_else(|| ctx.headers.get("Content-Type"))
            .cloned()
            .unwrap_or_else(|| "application/octet-stream".to_string());

        let obj = S3Object::new(key, ctx.body.clone(), content_type);
        let etag = obj.etag.clone();
        bucket.put_object(obj);

        Ok(ServiceResponse::xml(200, "")
            .with_header("ETag", format!("\"{etag}\"")))
    }

    fn get_object(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let key = get_key(ctx, params)?;
        let version_id = param_str(params, "versionId");
        let bucket = self.require_bucket(ctx, &bucket_name)?;

        let obj = bucket
            .get_object(&key, version_id.as_deref())
            .ok_or_else(|| {
                ServiceException::new("NoSuchKey", format!("The specified key does not exist: {key}"), 404)
            })?;

        let mut resp = ServiceResponse {
            status: 200,
            headers: HashMap::new(),
            body: obj.data.clone(),
        };
        resp.headers.insert("Content-Type".to_string(), obj.content_type.clone());
        resp.headers.insert("ETag".to_string(), format!("\"{}\"", obj.etag));
        resp.headers.insert("Content-Length".to_string(), obj.content_length.to_string());
        resp.headers.insert("Last-Modified".to_string(), http_date(&obj.last_modified));
        if let Some(ref vid) = obj.version_id {
            resp.headers.insert("x-amz-version-id".to_string(), vid.clone());
        }
        if let Some(ref ce) = obj.content_encoding {
            resp.headers.insert("Content-Encoding".to_string(), ce.clone());
        }
        if let Some(ref cd) = obj.content_disposition {
            resp.headers.insert("Content-Disposition".to_string(), cd.clone());
        }
        for (k, v) in &obj.metadata {
            resp.headers.insert(format!("x-amz-meta-{k}"), v.clone());
        }
        Ok(resp)
    }

    fn head_object(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let key = get_key(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let obj = bucket.get_object(&key, None).ok_or_else(|| {
            ServiceException::new("NoSuchKey", "The specified key does not exist.", 404)
        })?;

        let mut resp = ServiceResponse::xml(200, "");
        resp.headers.insert("Content-Type".to_string(), obj.content_type.clone());
        resp.headers.insert("ETag".to_string(), format!("\"{}\"", obj.etag));
        resp.headers.insert("Content-Length".to_string(), obj.content_length.to_string());
        resp.headers.insert("Last-Modified".to_string(), http_date(&obj.last_modified));
        if let Some(ref vid) = obj.version_id {
            resp.headers.insert("x-amz-version-id".to_string(), vid.clone());
        }
        Ok(resp)
    }

    fn delete_object(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let key = get_key(ctx, params)?;
        let version_id = param_str(params, "versionId");
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let result = bucket.delete_object(&key, version_id.as_deref());
        let mut resp = ServiceResponse::xml(204, "");
        if let Some(ref obj) = result {
            if obj.delete_marker {
                resp.headers.insert("x-amz-delete-marker".to_string(), "true".to_string());
            }
            if let Some(ref vid) = obj.version_id {
                resp.headers.insert("x-amz-version-id".to_string(), vid.clone());
            }
        }
        Ok(resp)
    }

    fn delete_objects(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);

        let body_str = std::str::from_utf8(&ctx.body).unwrap_or("");
        for line in body_str.split("<Key>").skip(1) {
            if let Some(end) = line.find("</Key>") {
                let key = &line[..end];
                bucket.delete_object(key, None);
                xml.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
            }
        }
        xml.push_str("</DeleteResult>");
        Ok(ServiceResponse::xml(200, xml))
    }

    fn copy_object(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let dest_bucket = get_bucket(ctx, params)?;
        let dest_key = get_key(ctx, params)?;
        let copy_source = ctx
            .headers
            .get("x-amz-copy-source")
            .or_else(|| ctx.headers.get("X-Amz-Copy-Source"))
            .cloned()
            .unwrap_or_default();
        let decoded = urlencoding::decode(&copy_source).unwrap_or_default();
        let parts: Vec<&str> = decoded.trim_start_matches('/').splitn(2, '/').collect();
        if parts.len() < 2 {
            return Err(ServiceException::new("InvalidArgument", "Invalid copy source.", 400));
        }
        let src_bucket_name = parts[0];
        let src_key = parts[1];
        let src_bucket = self.require_bucket(ctx, src_bucket_name)?;
        let obj = src_bucket.get_object(src_key, None).ok_or_else(|| {
            ServiceException::new("NoSuchKey", "The specified key does not exist.", 404)
        })?;

        let dest_b = self.require_bucket(ctx, &dest_bucket)?;
        let new_obj = S3Object::new(dest_key, obj.data.clone(), obj.content_type.clone());
        let etag = new_obj.etag.clone();
        let last_mod = new_obj.last_modified;
        dest_b.put_object(new_obj);

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"{etag}"</ETag><LastModified>{}</LastModified></CopyObjectResult>"#,
            last_mod.to_rfc3339()
        );
        Ok(ServiceResponse::xml(200, xml))
    }

    fn list_objects(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let prefix = param_str(params, "prefix").or_else(|| param_str(params, "Prefix"));
        let delimiter = param_str(params, "delimiter").or_else(|| param_str(params, "Delimiter"));
        let max_keys = param_str(params, "max-keys")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000);

        let (objects, common_prefixes) = bucket.list_objects(prefix.as_deref(), delimiter.as_deref(), max_keys);
        let mut xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{bucket_name}</Name><MaxKeys>{max_keys}</MaxKeys><IsTruncated>false</IsTruncated>"#
        );
        if let Some(ref p) = prefix {
            xml.push_str(&format!("<Prefix>{p}</Prefix>"));
        }
        for obj in &objects {
            xml.push_str(&format!(
                "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>{}</StorageClass></Contents>",
                xml_escape(&obj.key), obj.last_modified.to_rfc3339(), obj.etag, obj.content_length, obj.storage_class
            ));
        }
        for cp in &common_prefixes {
            xml.push_str(&format!("<CommonPrefixes><Prefix>{cp}</Prefix></CommonPrefixes>"));
        }
        xml.push_str("</ListBucketResult>");
        Ok(ServiceResponse::xml(200, xml))
    }

    fn list_objects_v2(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let prefix = param_str(params, "prefix").or_else(|| param_str(params, "Prefix"));
        let delimiter = param_str(params, "delimiter").or_else(|| param_str(params, "Delimiter"));
        let max_keys = param_str(params, "max-keys")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000);

        let (objects, common_prefixes) = bucket.list_objects(prefix.as_deref(), delimiter.as_deref(), max_keys);
        let count = objects.len();
        let mut xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{bucket_name}</Name><MaxKeys>{max_keys}</MaxKeys><KeyCount>{count}</KeyCount><IsTruncated>false</IsTruncated>"#
        );
        if let Some(ref p) = prefix {
            xml.push_str(&format!("<Prefix>{p}</Prefix>"));
        }
        for obj in &objects {
            xml.push_str(&format!(
                "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>{}</StorageClass></Contents>",
                xml_escape(&obj.key), obj.last_modified.to_rfc3339(), obj.etag, obj.content_length, obj.storage_class
            ));
        }
        for cp in &common_prefixes {
            xml.push_str(&format!("<CommonPrefixes><Prefix>{cp}</Prefix></CommonPrefixes>"));
        }
        xml.push_str("</ListBucketResult>");
        Ok(ServiceResponse::xml(200, xml))
    }

    fn list_object_versions(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let mut xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{bucket_name}</Name><IsTruncated>false</IsTruncated>"#
        );
        for entry in bucket.objects.iter() {
            let versions: &Vec<S3Object> = entry.value();
            for obj in versions.iter() {
                let vid = obj.version_id.as_deref().unwrap_or("null");
                if obj.delete_marker {
                    xml.push_str(&format!(
                        "<DeleteMarker><Key>{}</Key><VersionId>{vid}</VersionId><IsLatest>true</IsLatest><LastModified>{}</LastModified></DeleteMarker>",
                        xml_escape(&obj.key), obj.last_modified.to_rfc3339()
                    ));
                } else {
                    xml.push_str(&format!(
                        "<Version><Key>{}</Key><VersionId>{vid}</VersionId><IsLatest>true</IsLatest><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>{}</StorageClass></Version>",
                        xml_escape(&obj.key), obj.last_modified.to_rfc3339(), obj.etag, obj.content_length, obj.storage_class
                    ));
                }
            }
        }
        xml.push_str("</ListVersionsResult>");
        Ok(ServiceResponse::xml(200, xml))
    }

    fn create_multipart_upload(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let key = get_key(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let upload_id = Uuid::new_v4().to_string();
        let content_type = ctx.headers.get("content-type").or_else(|| ctx.headers.get("Content-Type")).cloned().unwrap_or_else(|| "application/octet-stream".to_string());
        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            key: key.clone(),
            parts: HashMap::new(),
            content_type,
            metadata: HashMap::new(),
            initiated: Utc::now(),
        };
        bucket.multipart_uploads.insert(upload_id.clone(), upload);
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{bucket_name}</Bucket><Key>{}</Key><UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>"#,
            xml_escape(&key)
        );
        Ok(ServiceResponse::xml(200, xml))
    }

    fn upload_part(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let upload_id = param_str(params, "uploadId").unwrap_or_default();
        let part_number: u32 = param_str(params, "partNumber")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let etag = format!("{:x}", md5::compute(&ctx.body));

        if let Some(mut upload) = bucket.multipart_uploads.get_mut(&upload_id) {
            let part = MultipartPart {
                part_number,
                data: ctx.body.clone(),
                etag: etag.clone(),
                last_modified: Utc::now(),
                size: ctx.body.len(),
            };
            upload.parts.insert(part_number, part);
        } else {
            return Err(ServiceException::new("NoSuchUpload", "The specified upload does not exist.", 404));
        }
        Ok(ServiceResponse::xml(200, "").with_header("ETag", format!("\"{etag}\"")))
    }

    fn complete_multipart_upload(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let upload_id = param_str(params, "uploadId").unwrap_or_default();

        let upload = bucket
            .multipart_uploads
            .remove(&upload_id)
            .map(|(_, u)| u)
            .ok_or_else(|| {
                ServiceException::new("NoSuchUpload", "The specified upload does not exist.", 404)
            })?;

        let mut parts: Vec<(u32, &MultipartPart)> = upload.parts.iter().map(|(k, v)| (*k, v)).collect();
        parts.sort_by_key(|(k, _)| *k);

        let mut combined = Vec::new();
        for (_, part) in &parts {
            combined.extend_from_slice(&part.data);
        }
        let data = Bytes::from(combined);
        let obj = S3Object::new(upload.key.clone(), data, upload.content_type.clone());
        let etag = obj.etag.clone();
        bucket.put_object(obj);

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{bucket_name}</Bucket><Key>{}</Key><ETag>"{etag}"</ETag></CompleteMultipartUploadResult>"#,
            xml_escape(&upload.key)
        );
        Ok(ServiceResponse::xml(200, xml))
    }

    fn abort_multipart_upload(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let upload_id = param_str(params, "uploadId").unwrap_or_default();
        bucket.multipart_uploads.remove(&upload_id);
        Ok(ServiceResponse::xml(204, ""))
    }

    fn list_multipart_uploads(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let mut xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{bucket_name}</Bucket><IsTruncated>false</IsTruncated>"#
        );
        for entry in bucket.multipart_uploads.iter() {
            let u: &MultipartUpload = entry.value();
            xml.push_str(&format!(
                "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated></Upload>",
                xml_escape(&u.key), u.upload_id, u.initiated.to_rfc3339()
            ));
        }
        xml.push_str("</ListMultipartUploadsResult>");
        Ok(ServiceResponse::xml(200, xml))
    }

    fn list_parts(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let upload_id = param_str(params, "uploadId").unwrap_or_default();
        let upload = bucket.multipart_uploads.get(&upload_id).ok_or_else(|| {
            ServiceException::new("NoSuchUpload", "The specified upload does not exist.", 404)
        })?;
        let mut xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{bucket_name}</Bucket><Key>{}</Key><UploadId>{upload_id}</UploadId><IsTruncated>false</IsTruncated>"#,
            xml_escape(&upload.key)
        );
        let mut parts: Vec<&MultipartPart> = upload.parts.values().collect();
        parts.sort_by_key(|p| p.part_number);
        for p in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>\"{}\"</ETag><Size>{}</Size><LastModified>{}</LastModified></Part>",
                p.part_number, p.etag, p.size, p.last_modified.to_rfc3339()
            ));
        }
        xml.push_str("</ListPartsResult>");
        Ok(ServiceResponse::xml(200, xml))
    }

    // Bucket config operations — simple store/retrieve pattern
    fn put_bucket_versioning(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let body_str = std::str::from_utf8(&ctx.body).unwrap_or("");
        let status = if body_str.contains("<Status>Enabled</Status>") {
            VersioningStatus::Enabled
        } else if body_str.contains("<Status>Suspended</Status>") {
            VersioningStatus::Suspended
        } else {
            VersioningStatus::Disabled
        };
        *bucket.versioning.lock().unwrap() = status;
        Ok(ServiceResponse::xml(200, ""))
    }

    fn get_bucket_versioning(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let status = *bucket.versioning.lock().unwrap();
        let status_str = match status {
            VersioningStatus::Enabled => "<Status>Enabled</Status>",
            VersioningStatus::Suspended => "<Status>Suspended</Status>",
            VersioningStatus::Disabled => "",
        };
        let xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{status_str}</VersioningConfiguration>"#);
        Ok(ServiceResponse::xml(200, xml))
    }

    fn put_bucket_policy(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.policy.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(204, ""))
    }
    fn get_bucket_policy(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let policy = bucket.policy.lock().unwrap().clone().unwrap_or_default();
        Ok(ServiceResponse::json(200, policy))
    }
    fn delete_bucket_policy(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.policy.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }
    fn get_bucket_policy_status(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        Ok(ServiceResponse::xml(200, r#"<?xml version="1.0" encoding="UTF-8"?><PolicyStatus><IsPublic>false</IsPublic></PolicyStatus>"#))
    }

    fn put_bucket_cors(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.cors_rules.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_cors(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let cors = bucket.cors_rules.lock().unwrap().clone().unwrap_or_else(|| r#"<?xml version="1.0" encoding="UTF-8"?><CORSConfiguration/>"#.to_string());
        Ok(ServiceResponse::xml(200, cors))
    }
    fn delete_bucket_cors(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.cors_rules.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }

    fn put_bucket_lifecycle(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.lifecycle_rules.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_lifecycle(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let lc = bucket.lifecycle_rules.lock().unwrap().clone().unwrap_or_else(|| r#"<?xml version="1.0" encoding="UTF-8"?><LifecycleConfiguration/>"#.to_string());
        Ok(ServiceResponse::xml(200, lc))
    }
    fn delete_bucket_lifecycle(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.lifecycle_rules.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }

    fn put_bucket_notification(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.notification_config.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_notification(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let nc = bucket.notification_config.lock().unwrap().clone().unwrap_or_else(|| r#"<?xml version="1.0" encoding="UTF-8"?><NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#.to_string());
        Ok(ServiceResponse::xml(200, nc))
    }

    fn put_bucket_encryption(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.encryption_config.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_encryption(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let enc = bucket.encryption_config.lock().unwrap().clone().unwrap_or_else(|| r#"<?xml version="1.0" encoding="UTF-8"?><ServerSideEncryptionConfiguration/>"#.to_string());
        Ok(ServiceResponse::xml(200, enc))
    }
    fn delete_bucket_encryption(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.encryption_config.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }

    fn put_bucket_tagging(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let body = std::str::from_utf8(&ctx.body).unwrap_or("");
        bucket.tagging.clear();
        for chunk in body.split("<Tag>").skip(1) {
            let key = chunk.split("<Key>").nth(1).and_then(|s| s.split("</Key>").next()).unwrap_or("");
            let val = chunk.split("<Value>").nth(1).and_then(|s| s.split("</Value>").next()).unwrap_or("");
            bucket.tagging.insert(key.to_string(), val.to_string());
        }
        Ok(ServiceResponse::xml(204, ""))
    }
    fn get_bucket_tagging(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet>"#);
        for entry in bucket.tagging.iter() {
            let tag_val: &String = entry.value();
            xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                entry.key(),
                tag_val
            ));
        }
        xml.push_str("</TagSet></Tagging>");
        Ok(ServiceResponse::xml(200, xml))
    }
    fn delete_bucket_tagging(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        bucket.tagging.clear();
        Ok(ServiceResponse::xml(204, ""))
    }

    fn put_object_tagging(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> { Ok(ServiceResponse::xml(200, "")) }
    fn get_object_tagging(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        Ok(ServiceResponse::xml(200, r#"<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet/></Tagging>"#))
    }
    fn delete_object_tagging(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> { Ok(ServiceResponse::xml(204, "")) }

    fn put_bucket_acl(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.acl.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_acl(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy><Owner><ID>{}</ID><DisplayName>ferro</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>{}</ID><DisplayName>ferro</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>"#, ctx.account_id, ctx.account_id);
        Ok(ServiceResponse::xml(200, xml))
    }
    fn put_object_acl(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> { Ok(ServiceResponse::xml(200, "")) }
    fn get_object_acl(&self, ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy><Owner><ID>{}</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>{}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>"#, ctx.account_id, ctx.account_id);
        Ok(ServiceResponse::xml(200, xml))
    }

    fn put_bucket_logging(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.logging_config.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_logging(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        Ok(ServiceResponse::xml(200, r#"<?xml version="1.0" encoding="UTF-8"?><BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01"/>"#))
    }

    fn put_bucket_website(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.website_config.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_website(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let ws = bucket.website_config.lock().unwrap().clone().ok_or_else(|| {
            ServiceException::new("NoSuchWebsiteConfiguration", "The website configuration does not exist.", 404)
        })?;
        Ok(ServiceResponse::xml(200, ws))
    }
    fn delete_bucket_website(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.website_config.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }

    fn get_bucket_location(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let loc = if bucket.region == "us-east-1" { String::new() } else { format!("<LocationConstraint>{}</LocationConstraint>", bucket.region) };
        Ok(ServiceResponse::xml(200, format!(r#"<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{loc}</LocationConstraint>"#)))
    }

    fn put_bucket_replication(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.replication_config.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_replication(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let rc = bucket.replication_config.lock().unwrap().clone().ok_or_else(|| {
            ServiceException::new("ReplicationConfigurationNotFoundError", "The replication configuration was not found.", 404)
        })?;
        Ok(ServiceResponse::xml(200, rc))
    }
    fn delete_bucket_replication(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.replication_config.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }

    fn put_object_lock_config(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.object_lock_config.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_object_lock_config(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let olc = bucket.object_lock_config.lock().unwrap().clone().unwrap_or_else(|| r#"<?xml version="1.0" encoding="UTF-8"?><ObjectLockConfiguration/>"#.to_string());
        Ok(ServiceResponse::xml(200, olc))
    }

    fn put_object_legal_hold(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> { Ok(ServiceResponse::xml(200, "")) }
    fn get_object_legal_hold(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> { Ok(ServiceResponse::xml(200, r#"<?xml version="1.0" encoding="UTF-8"?><LegalHold><Status>OFF</Status></LegalHold>"#)) }
    fn put_object_retention(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> { Ok(ServiceResponse::xml(200, "")) }
    fn get_object_retention(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> { Ok(ServiceResponse::xml(200, r#"<?xml version="1.0" encoding="UTF-8"?><Retention/>"#)) }

    fn put_public_access_block(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let body = std::str::from_utf8(&ctx.body).unwrap_or("");
        let pab = PublicAccessBlock {
            block_public_acls: body.contains("<BlockPublicAcls>true</BlockPublicAcls>"),
            block_public_policy: body.contains("<BlockPublicPolicy>true</BlockPublicPolicy>"),
            ignore_public_acls: body.contains("<IgnorePublicAcls>true</IgnorePublicAcls>"),
            restrict_public_buckets: body.contains("<RestrictPublicBuckets>true</RestrictPublicBuckets>"),
        };
        *bucket.public_access_block.lock().unwrap() = Some(pab);
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_public_access_block(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let pab = bucket.public_access_block.lock().unwrap();
        let xml = if let Some(ref p) = *pab {
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><PublicAccessBlockConfiguration><BlockPublicAcls>{}</BlockPublicAcls><IgnorePublicAcls>{}</IgnorePublicAcls><BlockPublicPolicy>{}</BlockPublicPolicy><RestrictPublicBuckets>{}</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#,
                p.block_public_acls, p.ignore_public_acls, p.block_public_policy, p.restrict_public_buckets)
        } else {
            return Err(ServiceException::new("NoSuchPublicAccessBlockConfiguration", "The public access block configuration was not found.", 404));
        };
        Ok(ServiceResponse::xml(200, xml))
    }
    fn delete_public_access_block(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.public_access_block.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }

    fn put_bucket_accelerate(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.accelerate_config.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_accelerate(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        Ok(ServiceResponse::xml(200, r#"<?xml version="1.0" encoding="UTF-8"?><AccelerateConfiguration/>"#))
    }

    fn put_bucket_ownership(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.ownership_controls.lock().unwrap() = Some(String::from_utf8_lossy(&ctx.body).to_string());
        Ok(ServiceResponse::xml(200, ""))
    }
    fn get_bucket_ownership(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        let oc = bucket.ownership_controls.lock().unwrap().clone().ok_or_else(|| {
            ServiceException::new("OwnershipControlsNotFoundError", "The ownership controls were not found.", 404)
        })?;
        Ok(ServiceResponse::xml(200, oc))
    }
    fn delete_bucket_ownership(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket = self.require_bucket(ctx, &get_bucket(ctx, params)?)?;
        *bucket.ownership_controls.lock().unwrap() = None;
        Ok(ServiceResponse::xml(204, ""))
    }

    fn get_object_attributes(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let key = get_key(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let obj = bucket.get_object(&key, None).ok_or_else(|| {
            ServiceException::new("NoSuchKey", "The specified key does not exist.", 404)
        })?;
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><GetObjectAttributesResponse><ETag>{}</ETag><ObjectSize>{}</ObjectSize><StorageClass>{}</StorageClass></GetObjectAttributesResponse>"#,
            obj.etag, obj.content_length, obj.storage_class
        );
        Ok(ServiceResponse::xml(200, xml))
    }

    fn upload_part_copy(&self, ctx: &RequestContext, params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        let bucket_name = get_bucket(ctx, params)?;
        let bucket = self.require_bucket(ctx, &bucket_name)?;
        let upload_id = param_str(params, "uploadId").unwrap_or_default();
        let part_number: u32 = param_str(params, "partNumber").and_then(|s| s.parse().ok()).unwrap_or(1);

        let copy_source = ctx.headers.get("x-amz-copy-source").or_else(|| ctx.headers.get("X-Amz-Copy-Source")).cloned().unwrap_or_default();
        let decoded = urlencoding::decode(&copy_source).unwrap_or_default();
        let parts: Vec<&str> = decoded.trim_start_matches('/').splitn(2, '/').collect();
        if parts.len() < 2 {
            return Err(ServiceException::new("InvalidArgument", "Invalid copy source.", 400));
        }
        let src_bucket = self.require_bucket(ctx, parts[0])?;
        let src_obj = src_bucket.get_object(parts[1], None).ok_or_else(|| {
            ServiceException::new("NoSuchKey", "Source key does not exist.", 404)
        })?;

        let etag = format!("{:x}", md5::compute(&src_obj.data));
        if let Some(mut upload) = bucket.multipart_uploads.get_mut(&upload_id) {
            upload.parts.insert(part_number, MultipartPart {
                part_number, data: src_obj.data.clone(), etag: etag.clone(), last_modified: Utc::now(), size: src_obj.data.len(),
            });
        }
        let xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?><CopyPartResult><ETag>"{etag}"</ETag><LastModified>{}</LastModified></CopyPartResult>"#, Utc::now().to_rfc3339());
        Ok(ServiceResponse::xml(200, xml))
    }

    fn restore_object(&self, _ctx: &RequestContext, _params: &serde_json::Value) -> Result<ServiceResponse, ServiceException> {
        Ok(ServiceResponse::xml(202, ""))
    }
}

fn get_bucket(ctx: &RequestContext, params: &serde_json::Value) -> Result<String, ServiceException> {
    param_str(params, "Bucket")
        .or_else(|| {
            let (b, _) = parser::extract_bucket_and_key(&ctx.uri);
            b
        })
        .ok_or_else(|| ServiceException::new("InvalidBucketName", "Bucket name is required.", 400))
}

fn get_key(ctx: &RequestContext, params: &serde_json::Value) -> Result<String, ServiceException> {
    param_str(params, "Key")
        .or_else(|| {
            let (_, k) = parser::extract_bucket_and_key(&ctx.uri);
            k
        })
        .ok_or_else(|| ServiceException::new("InvalidArgument", "Object key is required.", 400))
}

fn param_str(params: &serde_json::Value, key: &str) -> Option<String> {
    params.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn http_date(dt: &DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
