use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use ls_store::Store;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub data: Bytes,
    pub etag: String,
    pub content_type: String,
    pub content_length: usize,
    pub last_modified: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub version_id: Option<String>,
    pub delete_marker: bool,
    pub storage_class: String,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
    pub checksum_sha256: Option<String>,
    pub checksum_crc32: Option<String>,
    pub server_side_encryption: Option<String>,
    pub sse_kms_key_id: Option<String>,
}

impl S3Object {
    pub fn new(key: String, data: Bytes, content_type: String) -> Self {
        let etag = format!("{:x}", md5::compute(&data));
        let content_length = data.len();
        Self {
            key,
            data,
            etag,
            content_type,
            content_length,
            last_modified: Utc::now(),
            metadata: HashMap::new(),
            version_id: None,
            delete_marker: false,
            storage_class: "STANDARD".to_string(),
            content_encoding: None,
            content_disposition: None,
            content_language: None,
            cache_control: None,
            expires: None,
            checksum_sha256: None,
            checksum_crc32: None,
            server_side_encryption: None,
            sse_kms_key_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub key: String,
    pub parts: HashMap<u32, MultipartPart>,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    pub initiated: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MultipartPart {
    pub part_number: u32,
    pub data: Bytes,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub size: usize,
}

#[derive(Debug)]
pub struct S3Bucket {
    pub name: String,
    pub region: String,
    pub account_id: String,
    pub creation_date: DateTime<Utc>,
    pub objects: DashMap<String, Vec<S3Object>>,
    pub versioning: Mutex<VersioningStatus>,
    pub policy: Mutex<Option<String>>,
    pub cors_rules: Mutex<Option<String>>,
    pub lifecycle_rules: Mutex<Option<String>>,
    pub notification_config: Mutex<Option<String>>,
    pub encryption_config: Mutex<Option<String>>,
    pub logging_config: Mutex<Option<String>>,
    pub website_config: Mutex<Option<String>>,
    pub tagging: DashMap<String, String>,
    pub acl: Mutex<Option<String>>,
    pub multipart_uploads: DashMap<String, MultipartUpload>,
    pub public_access_block: Mutex<Option<PublicAccessBlock>>,
    pub object_lock_enabled: bool,
    pub object_lock_config: Mutex<Option<String>>,
    pub replication_config: Mutex<Option<String>>,
    pub ownership_controls: Mutex<Option<String>>,
    pub accelerate_config: Mutex<Option<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersioningStatus {
    Disabled,
    Enabled,
    Suspended,
}

#[derive(Debug, Clone)]
pub struct PublicAccessBlock {
    pub block_public_acls: bool,
    pub block_public_policy: bool,
    pub ignore_public_acls: bool,
    pub restrict_public_buckets: bool,
}

impl S3Bucket {
    pub fn new(name: String, region: String, account_id: String) -> Self {
        Self {
            name,
            region,
            account_id,
            creation_date: Utc::now(),
            objects: DashMap::new(),
            versioning: Mutex::new(VersioningStatus::Disabled),
            policy: Mutex::new(None),
            cors_rules: Mutex::new(None),
            lifecycle_rules: Mutex::new(None),
            notification_config: Mutex::new(None),
            encryption_config: Mutex::new(None),
            logging_config: Mutex::new(None),
            website_config: Mutex::new(None),
            tagging: DashMap::new(),
            acl: Mutex::new(None),
            multipart_uploads: DashMap::new(),
            public_access_block: Mutex::new(None),
            object_lock_enabled: false,
            object_lock_config: Mutex::new(None),
            replication_config: Mutex::new(None),
            ownership_controls: Mutex::new(None),
            accelerate_config: Mutex::new(None),
        }
    }

    pub fn put_object(&self, obj: S3Object) {
        let key = obj.key.clone();
        let versioning = *self.versioning.lock().unwrap();
        match versioning {
            VersioningStatus::Enabled => {
                let mut obj = obj;
                obj.version_id = Some(Uuid::new_v4().to_string());
                self.objects
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(obj);
            }
            _ => {
                self.objects.insert(key, vec![obj]);
            }
        }
    }

    pub fn get_object(&self, key: &str, version_id: Option<&str>) -> Option<S3Object> {
        let versions = self.objects.get(key)?;
        if let Some(vid) = version_id {
            versions
                .iter()
                .find(|o| o.version_id.as_deref() == Some(vid) && !o.delete_marker)
                .cloned()
        } else {
            versions.last().filter(|o| !o.delete_marker).cloned()
        }
    }

    pub fn delete_object(&self, key: &str, version_id: Option<&str>) -> Option<S3Object> {
        let versioning = *self.versioning.lock().unwrap();
        match versioning {
            VersioningStatus::Enabled => {
                if let Some(vid) = version_id {
                    if let Some(mut versions) = self.objects.get_mut(key) {
                        if let Some(pos) = versions
                            .iter()
                            .position(|o| o.version_id.as_deref() == Some(vid))
                        {
                            return Some(versions.remove(pos));
                        }
                    }
                    None
                } else {
                    let marker = S3Object {
                        key: key.to_string(),
                        data: Bytes::new(),
                        etag: String::new(),
                        content_type: String::new(),
                        content_length: 0,
                        last_modified: Utc::now(),
                        metadata: HashMap::new(),
                        version_id: Some(Uuid::new_v4().to_string()),
                        delete_marker: true,
                        storage_class: "STANDARD".to_string(),
                        content_encoding: None,
                        content_disposition: None,
                        content_language: None,
                        cache_control: None,
                        expires: None,
                        checksum_sha256: None,
                        checksum_crc32: None,
                        server_side_encryption: None,
                        sse_kms_key_id: None,
                    };
                    self.objects
                        .entry(key.to_string())
                        .or_insert_with(Vec::new)
                        .push(marker.clone());
                    Some(marker)
                }
            }
            _ => {
                self.objects.remove(key).map(|(_, mut v)| v.pop()).flatten()
            }
        }
    }

    pub fn list_objects(&self, prefix: Option<&str>, delimiter: Option<&str>, max_keys: usize) -> (Vec<S3Object>, Vec<String>) {
        let mut objects = Vec::new();
        let mut common_prefixes: std::collections::HashSet<String> = std::collections::HashSet::new();

        for entry in self.objects.iter() {
            let key = entry.key();
            if let Some(p) = prefix {
                if !key.starts_with(p) {
                    continue;
                }
            }
            if let Some(versions) = self.objects.get(key) {
                if let Some(obj) = versions.last() {
                    if obj.delete_marker {
                        continue;
                    }
                    if let Some(d) = delimiter {
                        let suffix = if let Some(p) = prefix {
                            key.strip_prefix(p).unwrap_or(key)
                        } else {
                            key.as_str()
                        };
                        if let Some(pos) = suffix.find(d) {
                            let cp = if let Some(p) = prefix {
                                format!("{}{}", p, &suffix[..=pos])
                            } else {
                                suffix[..=pos].to_string()
                            };
                            common_prefixes.insert(cp);
                            continue;
                        }
                    }
                    objects.push(obj.clone());
                }
            }
        }
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        objects.truncate(max_keys);
        let mut cps: Vec<String> = common_prefixes.into_iter().collect();
        cps.sort();
        (objects, cps)
    }
}

#[derive(Debug, Default)]
pub struct S3Store {
    pub buckets: DashMap<String, Arc<S3Bucket>>,
}

impl Store for S3Store {
    fn service_name() -> &'static str {
        "s3"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bucket(name: &str) -> S3Bucket {
        S3Bucket::new(name.to_string(), "us-east-1".to_string(), "000000000000".to_string())
    }

    fn make_object(key: &str, data: &[u8]) -> S3Object {
        S3Object::new(key.to_string(), Bytes::from(data.to_vec()), "application/octet-stream".to_string())
    }

    #[test]
    fn object_etag_is_md5() {
        let obj = make_object("key", b"hello");
        assert_eq!(obj.etag, format!("{:x}", md5::compute(b"hello")));
    }

    #[test]
    fn object_content_length() {
        let obj = make_object("key", b"12345");
        assert_eq!(obj.content_length, 5);
    }

    #[test]
    fn bucket_put_and_get_object() {
        let b = make_bucket("test");
        b.put_object(make_object("file.txt", b"data"));
        let obj = b.get_object("file.txt", None).unwrap();
        assert_eq!(obj.data, Bytes::from("data"));
        assert_eq!(obj.key, "file.txt");
    }

    #[test]
    fn bucket_get_nonexistent_returns_none() {
        let b = make_bucket("test");
        assert!(b.get_object("missing", None).is_none());
    }

    #[test]
    fn bucket_put_overwrites_without_versioning() {
        let b = make_bucket("test");
        b.put_object(make_object("f.txt", b"v1"));
        b.put_object(make_object("f.txt", b"v2"));
        let obj = b.get_object("f.txt", None).unwrap();
        assert_eq!(obj.data, Bytes::from("v2"));
    }

    #[test]
    fn bucket_delete_object() {
        let b = make_bucket("test");
        b.put_object(make_object("f.txt", b"data"));
        b.delete_object("f.txt", None);
        assert!(b.get_object("f.txt", None).is_none());
    }

    #[test]
    fn bucket_versioning_enabled() {
        let b = make_bucket("test");
        *b.versioning.lock().unwrap() = VersioningStatus::Enabled;

        b.put_object(make_object("f.txt", b"v1"));
        b.put_object(make_object("f.txt", b"v2"));

        let latest = b.get_object("f.txt", None).unwrap();
        assert_eq!(latest.data, Bytes::from("v2"));
        assert!(latest.version_id.is_some());

        let versions = b.objects.get("f.txt").unwrap();
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn bucket_versioning_delete_creates_marker() {
        let b = make_bucket("test");
        *b.versioning.lock().unwrap() = VersioningStatus::Enabled;
        b.put_object(make_object("f.txt", b"data"));

        let result = b.delete_object("f.txt", None);
        assert!(result.is_some());
        assert!(result.unwrap().delete_marker);

        assert!(b.get_object("f.txt", None).is_none());
    }

    #[test]
    fn list_objects_basic() {
        let b = make_bucket("test");
        b.put_object(make_object("a.txt", b"1"));
        b.put_object(make_object("b.txt", b"2"));
        b.put_object(make_object("c.txt", b"3"));

        let (objs, cps) = b.list_objects(None, None, 1000);
        assert_eq!(objs.len(), 3);
        assert!(cps.is_empty());
        assert_eq!(objs[0].key, "a.txt");
        assert_eq!(objs[1].key, "b.txt");
        assert_eq!(objs[2].key, "c.txt");
    }

    #[test]
    fn list_objects_with_prefix() {
        let b = make_bucket("test");
        b.put_object(make_object("docs/a.txt", b"1"));
        b.put_object(make_object("docs/b.txt", b"2"));
        b.put_object(make_object("images/c.png", b"3"));

        let (objs, _) = b.list_objects(Some("docs/"), None, 1000);
        assert_eq!(objs.len(), 2);
        assert!(objs.iter().all(|o| o.key.starts_with("docs/")));
    }

    #[test]
    fn list_objects_with_delimiter() {
        let b = make_bucket("test");
        b.put_object(make_object("docs/a.txt", b"1"));
        b.put_object(make_object("docs/b.txt", b"2"));
        b.put_object(make_object("root.txt", b"3"));

        let (objs, cps) = b.list_objects(None, Some("/"), 1000);
        assert_eq!(objs.len(), 1);
        assert_eq!(objs[0].key, "root.txt");
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0], "docs/");
    }

    #[test]
    fn list_objects_max_keys() {
        let b = make_bucket("test");
        for i in 0..10 {
            b.put_object(make_object(&format!("file-{i:02}.txt"), b"data"));
        }
        let (objs, _) = b.list_objects(None, None, 3);
        assert_eq!(objs.len(), 3);
    }

    #[test]
    fn multipart_upload_lifecycle() {
        let b = make_bucket("test");
        let upload_id = "upload-123".to_string();
        let mut parts = HashMap::new();
        parts.insert(1, MultipartPart {
            part_number: 1,
            data: Bytes::from("part1"),
            etag: "etag1".to_string(),
            last_modified: Utc::now(),
            size: 5,
        });
        parts.insert(2, MultipartPart {
            part_number: 2,
            data: Bytes::from("part2"),
            etag: "etag2".to_string(),
            last_modified: Utc::now(),
            size: 5,
        });
        b.multipart_uploads.insert(upload_id.clone(), MultipartUpload {
            upload_id: upload_id.clone(),
            key: "big-file.bin".to_string(),
            parts,
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
            initiated: Utc::now(),
        });

        assert_eq!(b.multipart_uploads.len(), 1);
        assert_eq!(b.multipart_uploads.get(&upload_id).unwrap().parts.len(), 2);

        b.multipart_uploads.remove(&upload_id);
        assert!(b.multipart_uploads.is_empty());
    }

    #[test]
    fn bucket_tagging() {
        let b = make_bucket("test");
        b.tagging.insert("env".to_string(), "local".to_string());
        b.tagging.insert("project".to_string(), "demo".to_string());
        assert_eq!(b.tagging.len(), 2);
        assert_eq!(b.tagging.get("env").unwrap().value(), "local");
    }

    #[test]
    fn bucket_config_store_and_retrieve() {
        let b = make_bucket("test");
        assert!(b.policy.lock().unwrap().is_none());
        *b.policy.lock().unwrap() = Some(r#"{"Version":"2012-10-17"}"#.to_string());
        assert!(b.policy.lock().unwrap().is_some());
    }
}
