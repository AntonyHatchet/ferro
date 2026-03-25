pub mod models;
pub mod provider;

pub use provider::SqsService;

#[cfg(test)]
mod tests {
    use super::*;
    use ls_asf::context::RequestContext;
    use ls_asf::service::ServiceHandler;

    fn ctx(operation: &str) -> RequestContext {
        let mut c = RequestContext::new();
        c.service_name = "sqs".to_string();
        c.operation = operation.to_string();
        c.region = "us-east-1".to_string();
        c.account_id = "000000000000".to_string();
        c.headers.insert("host".to_string(), "localhost:4566".to_string());
        c
    }

    #[tokio::test]
    async fn create_queue_and_list() {
        let svc = SqsService::new();
        let params = serde_json::json!({"QueueName": "orders"});
        let resp = svc.handle(ctx("CreateQueue"), params).await.unwrap();
        assert_eq!(resp.status, 200);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("orders"));

        let resp = svc.handle(ctx("ListQueues"), serde_json::json!({})).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("orders"));
    }

    #[tokio::test]
    async fn send_and_receive() {
        let svc = SqsService::new();
        svc.handle(ctx("CreateQueue"), serde_json::json!({"QueueName": "q"})).await.unwrap();

        let params = serde_json::json!({
            "QueueUrl": "http://localhost:4566/000000000000/q",
            "MessageBody": "test-body"
        });
        let resp = svc.handle(ctx("SendMessage"), params).await.unwrap();
        assert_eq!(resp.status, 200);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<MessageId>"));

        let params = serde_json::json!({
            "QueueUrl": "http://localhost:4566/000000000000/q",
            "MaxNumberOfMessages": "1"
        });
        let resp = svc.handle(ctx("ReceiveMessage"), params).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("test-body"));
    }

    #[tokio::test]
    async fn delete_queue() {
        let svc = SqsService::new();
        svc.handle(ctx("CreateQueue"), serde_json::json!({"QueueName": "to-delete"})).await.unwrap();

        let params = serde_json::json!({"QueueUrl": "http://localhost:4566/000000000000/to-delete"});
        svc.handle(ctx("DeleteQueue"), params).await.unwrap();

        let resp = svc.handle(ctx("ListQueues"), serde_json::json!({})).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains("to-delete"));
    }

    #[tokio::test]
    async fn purge_queue() {
        let svc = SqsService::new();
        svc.handle(ctx("CreateQueue"), serde_json::json!({"QueueName": "pq"})).await.unwrap();

        let url = "http://localhost:4566/000000000000/pq";
        for _ in 0..3 {
            svc.handle(ctx("SendMessage"), serde_json::json!({"QueueUrl": url, "MessageBody": "x"})).await.unwrap();
        }

        svc.handle(ctx("PurgeQueue"), serde_json::json!({"QueueUrl": url})).await.unwrap();

        let resp = svc.handle(ctx("ReceiveMessage"), serde_json::json!({"QueueUrl": url})).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains("<Message>"));
    }

    #[tokio::test]
    async fn nonexistent_queue_error() {
        let svc = SqsService::new();
        let params = serde_json::json!({"QueueUrl": "http://localhost/000/nope", "MessageBody": "x"});
        let err = svc.handle(ctx("SendMessage"), params).await.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.code.contains("NonExistentQueue"));
    }

    #[tokio::test]
    async fn get_queue_attributes_returns_arn() {
        let svc = SqsService::new();
        svc.handle(ctx("CreateQueue"), serde_json::json!({"QueueName": "attr-q"})).await.unwrap();

        let resp = svc.handle(
            ctx("GetQueueAttributes"),
            serde_json::json!({"QueueUrl": "http://localhost:4566/000000000000/attr-q"}),
        ).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("QueueArn"));
        assert!(body.contains("arn:aws:sqs:us-east-1:000000000000:attr-q"));
    }
}
