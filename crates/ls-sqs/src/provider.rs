use crate::models::*;
use dashmap::mapref::one::Ref;
use ls_asf::context::RequestContext;
use ls_asf::error::ServiceException;
use ls_asf::serializer;
use ls_asf::service::{HandlerFuture, ServiceHandler, ServiceResponse};
use ls_store::AccountRegionBundle;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const MAX_LONG_POLL_SECONDS: u64 = 20;

pub struct SqsService {
    stores: AccountRegionBundle<SqsStore>,
}

impl SqsService {
    pub fn new() -> Self {
        Self {
            stores: AccountRegionBundle::new(),
        }
    }

    fn get_store(&self, account_id: &str, region: &str) -> Arc<SqsStore> {
        self.stores.get(account_id, region)
    }

    fn host_url(&self, ctx: &RequestContext) -> String {
        let host = ctx
            .headers
            .get("host")
            .or_else(|| ctx.headers.get("Host"))
            .cloned()
            .unwrap_or_else(|| "localhost:4566".to_string());
        format!("http://{host}")
    }

    fn resolve_queue(
        &self,
        ctx: &RequestContext,
        queue_url: Option<&str>,
        queue_name: Option<&str>,
    ) -> Result<Arc<SqsQueue>, ServiceException> {
        let store = self.get_store(&ctx.account_id, &ctx.region);

        if let Some(name) = queue_name {
            let entry: Option<Ref<String, Arc<SqsQueue>>> = store.queues.get(name);
            return entry
                .map(|q| q.value().clone())
                .ok_or_else(|| {
                    ServiceException::new(
                        "AWS.SimpleQueueService.NonExistentQueue",
                        "The specified queue does not exist.",
                        400,
                    )
                    .with_sender_fault()
                });
        }

        if let Some(url) = queue_url {
            let name = url.split('/').last().unwrap_or("");
            let entry: Option<Ref<String, Arc<SqsQueue>>> = store.queues.get(name);
            return entry
                .map(|q| q.value().clone())
                .ok_or_else(|| {
                    ServiceException::new(
                        "AWS.SimpleQueueService.NonExistentQueue",
                        "The specified queue does not exist.",
                        400,
                    )
                    .with_sender_fault()
                });
        }

        Err(ServiceException::new(
            "MissingParameter",
            "A queue URL or name is required.",
            400,
        ))
    }
}

impl ServiceHandler for SqsService {
    fn service_name(&self) -> &str {
        "sqs"
    }

    fn handle(&self, ctx: RequestContext, params: serde_json::Value) -> HandlerFuture {
        if ctx.operation == "ReceiveMessage" {
            match self.prepare_long_poll(&ctx, &params) {
                Ok(poll) => Box::pin(poll.execute(ctx)),
                Err(e) => Box::pin(async move { Err(e) }),
            }
        } else {
            let result = self.dispatch(ctx, params);
            Box::pin(async move { result })
        }
    }
}

impl SqsService {
    fn dispatch(
        &self,
        ctx: RequestContext,
        params: serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        match ctx.operation.as_str() {
            "CreateQueue" => self.create_queue(&ctx, &params),
            "DeleteQueue" => self.delete_queue(&ctx, &params),
            "GetQueueUrl" => self.get_queue_url(&ctx, &params),
            "GetQueueAttributes" => self.get_queue_attributes(&ctx, &params),
            "SetQueueAttributes" => self.set_queue_attributes(&ctx, &params),
            "ListQueues" => self.list_queues(&ctx, &params),
            "SendMessage" => self.send_message(&ctx, &params),
            "SendMessageBatch" => self.send_message_batch(&ctx, &params),
            "ReceiveMessage" => self.receive_message(&ctx, &params),
            "DeleteMessage" => self.delete_message(&ctx, &params),
            "DeleteMessageBatch" => self.delete_message_batch(&ctx, &params),
            "PurgeQueue" => self.purge_queue(&ctx, &params),
            "ChangeMessageVisibility" => self.change_message_visibility(&ctx, &params),
            "ChangeMessageVisibilityBatch" => self.change_message_visibility_batch(&ctx, &params),
            "TagQueue" => self.tag_queue(&ctx, &params),
            "UntagQueue" => self.untag_queue(&ctx, &params),
            "ListQueueTags" => self.list_queue_tags(&ctx, &params),
            "AddPermission" => self.add_permission(&ctx, &params),
            "RemovePermission" => self.remove_permission(&ctx, &params),
            "ListDeadLetterSourceQueues" => self.list_dead_letter_source_queues(&ctx, &params),
            "StartMessageMoveTask" => self.start_message_move_task(&ctx, &params),
            "CancelMessageMoveTask" => self.cancel_message_move_task(&ctx, &params),
            "ListMessageMoveTasks" => self.list_message_move_tasks(&ctx, &params),
            op => Err(ServiceException::new(
                "InvalidAction",
                format!("The action {op} is not valid for this endpoint."),
                400,
            )),
        }
    }

    fn create_queue(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_name = param_str(params, "QueueName").ok_or_else(|| {
            ServiceException::new("MissingParameter", "QueueName is required.", 400)
        })?;

        let attributes = extract_attributes(params);
        let tags = extract_tags(params);
        let store = self.get_store(&ctx.account_id, &ctx.region);

        if let Some(existing) = store.queues.get(&queue_name) {
            let url = existing.value().url(&self.host_url(ctx));
            return Ok(sqs_response(
                ctx,
                &format!("    <QueueUrl>{url}</QueueUrl>"),
                serde_json::json!({"QueueUrl": url}),
            ));
        }

        let queue = Arc::new(SqsQueue::new(
            queue_name.clone(),
            ctx.region.clone(),
            ctx.account_id.clone(),
            if attributes.is_empty() {
                None
            } else {
                Some(attributes)
            },
            if tags.is_empty() { None } else { Some(tags) },
        ));
        let url = queue.url(&self.host_url(ctx));
        store.queues.insert(queue_name, queue);

        Ok(sqs_response(
            ctx,
            &format!("    <QueueUrl>{url}</QueueUrl>"),
            serde_json::json!({"QueueUrl": url}),
        ))
    }

    fn delete_queue(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let store = self.get_store(&ctx.account_id, &ctx.region);
        store.queues.remove(&queue.name);
        store.deleted.insert(queue.name.clone(), now());
        Ok(empty_sqs_response(ctx))
    }

    fn get_queue_url(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_name = param_str(params, "QueueName").ok_or_else(|| {
            ServiceException::new("MissingParameter", "QueueName is required.", 400)
        })?;
        let queue = self.resolve_queue(ctx, None, Some(&queue_name))?;
        let url = queue.url(&self.host_url(ctx));
        Ok(sqs_response(
            ctx,
            &format!("    <QueueUrl>{url}</QueueUrl>"),
            serde_json::json!({"QueueUrl": url}),
        ))
    }

    fn get_queue_attributes(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let attr_names = extract_attribute_names(params);
        let attrs = queue.get_attributes(&attr_names);

        let mut xml = String::new();
        for (k, v) in &attrs {
            xml.push_str(&format!(
                "    <Attribute><Name>{k}</Name><Value>{v}</Value></Attribute>\n"
            ));
        }
        Ok(sqs_response(ctx, &xml, serde_json::json!({"Attributes": attrs})))
    }

    fn set_queue_attributes(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let _queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        Ok(empty_sqs_response(ctx))
    }

    fn list_queues(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let prefix = param_str(params, "QueueNamePrefix");
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let host = self.host_url(ctx);

        let mut xml = String::new();
        let mut urls = Vec::new();
        for entry in store.queues.iter() {
            let queue: &Arc<SqsQueue> = entry.value();
            if let Some(ref p) = prefix {
                if !queue.name.starts_with(p.as_str()) {
                    continue;
                }
            }
            let url = queue.url(&host);
            xml.push_str(&format!("    <QueueUrl>{url}</QueueUrl>\n"));
            urls.push(url);
        }
        Ok(sqs_response(ctx, &xml, serde_json::json!({"QueueUrls": urls})))
    }

    fn send_message(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let body = param_str(params, "MessageBody").ok_or_else(|| {
            ServiceException::new("MissingParameter", "MessageBody is required.", 400)
        })?;
        let delay = param_u64(params, "DelaySeconds");
        let dedup_id = param_str(params, "MessageDeduplicationId");
        let group_id = param_str(params, "MessageGroupId");
        let msg_attrs = extract_message_attributes(params);

        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let mut msg = SqsMessage::new(body, msg_attrs);
        msg.message_deduplication_id = dedup_id;
        msg.message_group_id = group_id;

        let result = queue.put_message(msg, delay);

        let mut xml = format!(
            "    <MessageId>{}</MessageId>\n    <MD5OfMessageBody>{}</MD5OfMessageBody>",
            result.message_id, result.md5_of_body
        );
        if let Some(ref md5) = result.md5_of_message_attributes {
            xml.push_str(&format!(
                "\n    <MD5OfMessageAttributes>{md5}</MD5OfMessageAttributes>"
            ));
        }
        if let Some(ref seq) = result.sequence_number {
            xml.push_str(&format!(
                "\n    <SequenceNumber>{seq}</SequenceNumber>"
            ));
        }
        let mut json_map = serde_json::json!({
            "MessageId": result.message_id,
            "MD5OfMessageBody": result.md5_of_body,
        });
        if let Some(ref md5) = result.md5_of_message_attributes {
            json_map["MD5OfMessageAttributes"] = serde_json::Value::String(md5.clone());
        }
        if let Some(ref seq) = result.sequence_number {
            json_map["SequenceNumber"] = serde_json::Value::String(seq.clone());
        }
        Ok(sqs_response(ctx, &xml, json_map))
    }

    fn send_message_batch(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;

        let entries = extract_batch_entries(params);
        if entries.is_empty() {
            return Err(ServiceException::new(
                "EmptyBatchRequest",
                "There should be at least one SendMessageBatchRequestEntry in the request.",
                400,
            ));
        }

        let mut success_xml = String::new();
        let error_xml = String::new();

        for entry in &entries {
            let id = entry
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let body = entry
                .get("MessageBody")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let delay = entry
                .get("DelaySeconds")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok());
            let dedup_id = entry
                .get("MessageDeduplicationId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let group_id = entry
                .get("MessageGroupId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let mut msg = SqsMessage::new(body.to_string(), HashMap::new());
            msg.message_deduplication_id = dedup_id;
            msg.message_group_id = group_id;

            let result = queue.put_message(msg, delay);
            success_xml.push_str(&format!(
                "    <SendMessageBatchResultEntry>\
                 <Id>{id}</Id>\
                 <MessageId>{}</MessageId>\
                 <MD5OfMessageBody>{}</MD5OfMessageBody>\
                 </SendMessageBatchResultEntry>\n",
                result.message_id, result.md5_of_body
            ));
        }

        let xml = format!("{success_xml}{error_xml}");
        Ok(sqs_response(
            ctx,
            &xml,
            serde_json::json!({}),
        ))
    }

    fn prepare_long_poll(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<LongPollRequest, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let max_messages = param_u64(params, "MaxNumberOfMessages").unwrap_or(1) as usize;
        let visibility_timeout = param_u64(params, "VisibilityTimeout");
        let request_wait = param_u64(params, "WaitTimeSeconds");

        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let queue_wait = queue.wait_time_seconds();
        let wait_time = request_wait.unwrap_or(queue_wait).min(MAX_LONG_POLL_SECONDS);

        Ok(LongPollRequest {
            queue,
            max_messages,
            visibility_timeout,
            wait_time,
        })
    }

    fn receive_message(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let max_messages = param_u64(params, "MaxNumberOfMessages").unwrap_or(1) as usize;
        let visibility_timeout = param_u64(params, "VisibilityTimeout");

        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let messages = queue.receive_messages(max_messages, visibility_timeout);
        Ok(build_receive_response(ctx, &messages))
    }

    fn delete_message(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let receipt = param_str(params, "ReceiptHandle").ok_or_else(|| {
            ServiceException::new("MissingParameter", "ReceiptHandle is required.", 400)
        })?;
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        queue.delete_message(&receipt);
        Ok(empty_sqs_response(ctx))
    }

    fn delete_message_batch(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let entries = extract_batch_entries(params);

        let mut xml = String::new();
        for entry in &entries {
            let id = entry
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let receipt = entry
                .get("ReceiptHandle")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            queue.delete_message(receipt);
            xml.push_str(&format!(
                "    <DeleteMessageBatchResultEntry><Id>{id}</Id></DeleteMessageBatchResultEntry>\n"
            ));
        }
        Ok(sqs_response(ctx, &xml, serde_json::json!({})))
    }

    fn purge_queue(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        queue.purge();
        Ok(empty_sqs_response(ctx))
    }

    fn change_message_visibility(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let receipt = param_str(params, "ReceiptHandle").unwrap_or_default();
        let vt = param_u64(params, "VisibilityTimeout").unwrap_or(0);
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        queue
            .change_visibility(&receipt, vt)
            .map_err(|e| ServiceException::new("ReceiptHandleIsInvalid", e, 400))?;
        Ok(empty_sqs_response(ctx))
    }

    fn change_message_visibility_batch(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let entries = extract_batch_entries(params);

        let mut xml = String::new();
        for entry in &entries {
            let id = entry
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let receipt = entry
                .get("ReceiptHandle")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let vt = entry
                .get("VisibilityTimeout")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let _ = queue.change_visibility(receipt, vt);
            xml.push_str(&format!(
                "    <ChangeMessageVisibilityBatchResultEntry>\
                 <Id>{id}</Id>\
                 </ChangeMessageVisibilityBatchResultEntry>\n"
            ));
        }
        Ok(sqs_response(ctx, &xml, serde_json::json!({})))
    }

    fn tag_queue(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let _queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        Ok(empty_sqs_response(ctx))
    }

    fn untag_queue(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let _queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        Ok(empty_sqs_response(ctx))
    }

    fn list_queue_tags(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let mut xml = String::new();
        let mut json_tags = serde_json::Map::new();
        for (k, v) in &queue.tags {
            xml.push_str(&format!(
                "    <Tag><Key>{k}</Key><Value>{v}</Value></Tag>\n"
            ));
            json_tags.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
        Ok(sqs_response(ctx, &xml, serde_json::json!({"Tags": json_tags})))
    }

    fn add_permission(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let _queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        Ok(empty_sqs_response(ctx))
    }

    fn remove_permission(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let _queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        Ok(empty_sqs_response(ctx))
    }

    fn list_dead_letter_source_queues(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let queue_url = param_str(params, "QueueUrl").unwrap_or_default();
        let target_queue = self.resolve_queue(ctx, Some(&queue_url), None)?;
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let host = self.host_url(ctx);

        let mut xml = String::new();
        let mut urls = Vec::new();
        for entry in store.queues.iter() {
            let q: &Arc<SqsQueue> = entry.value();
            if let Some(arn) = q.dead_letter_target_arn() {
                if arn == target_queue.arn {
                    let url = q.url(&host);
                    xml.push_str(&format!("    <QueueUrl>{url}</QueueUrl>\n"));
                    urls.push(url);
                }
            }
        }
        Ok(sqs_response(ctx, &xml, serde_json::json!({"QueueUrls": urls})))
    }

    fn start_message_move_task(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let task_handle = uuid::Uuid::new_v4().to_string();
        let xml = format!("    <TaskHandle>{task_handle}</TaskHandle>");
        Ok(sqs_response(ctx, &xml, serde_json::json!({"TaskHandle": task_handle})))
    }

    fn cancel_message_move_task(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let xml = "    <ApproximateNumberOfMessagesMoved>0</ApproximateNumberOfMessagesMoved>";
        Ok(sqs_response(ctx, &xml, serde_json::json!({"ApproximateNumberOfMessagesMoved": 0})))
    }

    fn list_message_move_tasks(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(empty_sqs_response(ctx))
    }
}

struct LongPollRequest {
    queue: Arc<SqsQueue>,
    max_messages: usize,
    visibility_timeout: Option<u64>,
    wait_time: u64,
}

impl LongPollRequest {
    async fn execute(self, ctx: RequestContext) -> Result<ServiceResponse, ServiceException> {
        let messages = self.queue.receive_messages(self.max_messages, self.visibility_timeout);
        if !messages.is_empty() || self.wait_time == 0 {
            return Ok(build_receive_response(&ctx, &messages));
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(self.wait_time);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, self.queue.notify.notified()).await {
                Ok(()) => {
                    let messages = self.queue.receive_messages(self.max_messages, self.visibility_timeout);
                    if !messages.is_empty() {
                        return Ok(build_receive_response(&ctx, &messages));
                    }
                }
                Err(_) => break,
            }
        }

        Ok(build_receive_response(&ctx, &[]))
    }
}

fn build_receive_response(
    ctx: &RequestContext,
    messages: &[(SqsMessage, String)],
) -> ServiceResponse {
    let mut xml = String::new();
    let mut json_messages = Vec::new();

    for (msg, receipt) in messages {
        xml.push_str("    <Message>\n");
        xml.push_str(&format!("      <MessageId>{}</MessageId>\n", msg.message_id));
        xml.push_str(&format!("      <ReceiptHandle>{receipt}</ReceiptHandle>\n"));
        xml.push_str(&format!("      <MD5OfBody>{}</MD5OfBody>\n", msg.md5_of_body));
        xml.push_str(&format!("      <Body>{}</Body>\n", xml_escape(&msg.body)));
        for (k, v) in &msg.attributes {
            xml.push_str(&format!("      <Attribute><Name>{k}</Name><Value>{v}</Value></Attribute>\n"));
        }
        if let Some(ref md5) = msg.md5_of_message_attributes {
            xml.push_str(&format!("      <MD5OfMessageAttributes>{md5}</MD5OfMessageAttributes>\n"));
        }
        for (k, attr) in &msg.message_attributes {
            xml.push_str("      <MessageAttribute>\n");
            xml.push_str(&format!("        <Name>{k}</Name>\n"));
            xml.push_str(&format!("        <Value><DataType>{}</DataType>", attr.data_type));
            if let Some(ref sv) = attr.string_value {
                xml.push_str(&format!("<StringValue>{}</StringValue>", xml_escape(sv)));
            }
            xml.push_str("</Value>\n      </MessageAttribute>\n");
        }
        xml.push_str("    </Message>\n");

        let mut jmsg = serde_json::json!({
            "MessageId": msg.message_id,
            "ReceiptHandle": receipt,
            "MD5OfBody": msg.md5_of_body,
            "Body": msg.body,
        });
        if !msg.attributes.is_empty() {
            jmsg["Attributes"] = serde_json::json!(msg.attributes);
        }
        if let Some(ref md5) = msg.md5_of_message_attributes {
            jmsg["MD5OfMessageAttributes"] = serde_json::Value::String(md5.clone());
        }
        if !msg.message_attributes.is_empty() {
            let mut jattrs = serde_json::Map::new();
            for (k, attr) in &msg.message_attributes {
                let mut a = serde_json::json!({"DataType": attr.data_type});
                if let Some(ref sv) = attr.string_value {
                    a["StringValue"] = serde_json::Value::String(sv.clone());
                }
                jattrs.insert(k.clone(), a);
            }
            jmsg["MessageAttributes"] = serde_json::Value::Object(jattrs);
        }
        json_messages.push(jmsg);
    }

    sqs_response(ctx, &xml, serde_json::json!({"Messages": json_messages}))
}

fn sqs_response(
    ctx: &RequestContext,
    inner_xml: &str,
    json_body: serde_json::Value,
) -> ServiceResponse {
    if ctx.is_json_protocol() {
        ServiceResponse::json(200, json_body.to_string())
            .with_header("x-amzn-requestid", ctx.request_id.clone())
    } else {
        let body = serializer::wrap_query_response(inner_xml, &ctx.operation, &ctx.request_id);
        ServiceResponse::xml(200, body)
    }
}

fn empty_sqs_response(ctx: &RequestContext) -> ServiceResponse {
    if ctx.is_json_protocol() {
        ServiceResponse::json(200, "{}".to_string())
            .with_header("x-amzn-requestid", ctx.request_id.clone())
    } else {
        let body = serializer::wrap_query_response("", &ctx.operation, &ctx.request_id);
        ServiceResponse::xml(200, body)
    }
}

fn param_str(params: &serde_json::Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn param_u64(params: &serde_json::Value, key: &str) -> Option<u64> {
    params.get(key).and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

fn extract_attributes(params: &serde_json::Value) -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    if let Some(obj) = params.get("Attributes").or(params.get("Attribute")).and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                attrs.insert(k.clone(), s.to_string());
            }
        }
    }
    for i in 1..=30 {
        let name_key = format!("Attribute.{i}.Name");
        let val_key = format!("Attribute.{i}.Value");
        if let (Some(name), Some(val)) = (param_str(params, &name_key), param_str(params, &val_key))
        {
            attrs.insert(name, val);
        } else {
            break;
        }
    }
    attrs
}

fn extract_tags(params: &serde_json::Value) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    if let Some(obj) = params.get("Tags").or(params.get("Tag")).and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                tags.insert(k.clone(), s.to_string());
            }
        }
    }
    for i in 1..=50 {
        let key = format!("Tag.{i}.Key");
        let val = format!("Tag.{i}.Value");
        if let (Some(k), Some(v)) = (param_str(params, &key), param_str(params, &val)) {
            tags.insert(k, v);
        } else {
            break;
        }
    }
    tags
}

fn extract_attribute_names(params: &serde_json::Value) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(arr) = params.get("AttributeNames").and_then(|v| v.as_array()) {
        for v in arr {
            if let Some(s) = v.as_str() {
                names.push(s.to_string());
            }
        }
    }
    for i in 1..=30 {
        let key = format!("AttributeName.{i}");
        if let Some(name) = param_str(params, &key) {
            names.push(name);
        } else {
            break;
        }
    }
    if names.is_empty() {
        names.push("All".to_string());
    }
    names
}

fn extract_message_attributes(
    params: &serde_json::Value,
) -> HashMap<String, MessageAttribute> {
    let mut attrs = HashMap::new();
    if let Some(obj) = params.get("MessageAttributes").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(dt) = v.get("DataType").and_then(|d| d.as_str()) {
                attrs.insert(
                    k.clone(),
                    MessageAttribute {
                        data_type: dt.to_string(),
                        string_value: v.get("StringValue").and_then(|s| s.as_str()).map(|s| s.to_string()),
                        binary_value: None,
                    },
                );
            }
        }
    }
    for i in 1..=30 {
        let name_key = format!("MessageAttribute.{i}.Name");
        let type_key = format!("MessageAttribute.{i}.Value.DataType");
        let str_key = format!("MessageAttribute.{i}.Value.StringValue");
        if let (Some(name), Some(dt)) = (param_str(params, &name_key), param_str(params, &type_key))
        {
            attrs.insert(
                name,
                MessageAttribute {
                    data_type: dt,
                    string_value: param_str(params, &str_key),
                    binary_value: None,
                },
            );
        } else {
            break;
        }
    }
    attrs
}

fn extract_batch_entries(params: &serde_json::Value) -> Vec<serde_json::Value> {
    if let Some(arr) = params.get("Entries").and_then(|v| v.as_array()) {
        return arr.clone();
    }

    let mut entries: HashMap<usize, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    if let Some(obj) = params.as_object() {
        for (k, v) in obj {
            if let Some(rest) = k.strip_prefix("SendMessageBatchRequestEntry.")
                .or_else(|| k.strip_prefix("DeleteMessageBatchRequestEntry."))
                .or_else(|| k.strip_prefix("ChangeMessageVisibilityBatchRequestEntry."))
            {
                if let Some((idx_str, field)) = rest.split_once('.') {
                    if let Ok(idx) = idx_str.parse::<usize>() {
                        let entry = entries.entry(idx).or_default();
                        entry.insert(field.to_string(), v.clone());
                    }
                }
            }
        }
    }

    let mut sorted: Vec<(usize, serde_json::Map<String, serde_json::Value>)> =
        entries.into_iter().collect();
    sorted.sort_by_key(|(k, _)| *k);
    sorted
        .into_iter()
        .map(|(_, v)| serde_json::Value::Object(v))
        .collect()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
