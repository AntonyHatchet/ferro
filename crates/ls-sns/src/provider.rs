use crate::models::*;
use ls_asf::context::RequestContext;
use ls_asf::error::ServiceException;
use ls_asf::serializer;
use ls_asf::service::{HandlerFuture, ServiceHandler, ServiceResponse};
use ls_store::AccountRegionBundle;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SnsService {
    stores: AccountRegionBundle<SnsStore>,
}

impl SnsService {
    pub fn new() -> Self {
        Self {
            stores: AccountRegionBundle::new(),
        }
    }

    fn get_store(&self, account_id: &str, region: &str) -> Arc<SnsStore> {
        self.stores.get(account_id, region)
    }
}

impl ServiceHandler for SnsService {
    fn service_name(&self) -> &str {
        "sns"
    }

    fn handle(&self, ctx: RequestContext, params: serde_json::Value) -> HandlerFuture {
        let result = self.dispatch(ctx, params);
        Box::pin(async move { result })
    }
}

impl SnsService {
    fn dispatch(
        &self,
        ctx: RequestContext,
        params: serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        match ctx.operation.as_str() {
            "CreateTopic" => self.create_topic(&ctx, &params),
            "DeleteTopic" => self.delete_topic(&ctx, &params),
            "ListTopics" => self.list_topics(&ctx, &params),
            "GetTopicAttributes" => self.get_topic_attributes(&ctx, &params),
            "SetTopicAttributes" => self.set_topic_attributes(&ctx, &params),
            "Subscribe" => self.subscribe(&ctx, &params),
            "Unsubscribe" => self.unsubscribe(&ctx, &params),
            "ConfirmSubscription" => self.confirm_subscription(&ctx, &params),
            "ListSubscriptions" => self.list_subscriptions(&ctx, &params),
            "ListSubscriptionsByTopic" => self.list_subscriptions_by_topic(&ctx, &params),
            "GetSubscriptionAttributes" => self.get_subscription_attributes(&ctx, &params),
            "SetSubscriptionAttributes" => self.set_subscription_attributes(&ctx, &params),
            "Publish" => self.publish(&ctx, &params),
            "PublishBatch" => self.publish_batch(&ctx, &params),
            "TagResource" => self.tag_resource(&ctx, &params),
            "UntagResource" => self.untag_resource(&ctx, &params),
            "ListTagsForResource" => self.list_tags_for_resource(&ctx, &params),
            "AddPermission" => self.add_permission(&ctx, &params),
            "RemovePermission" => self.remove_permission(&ctx, &params),
            "GetSMSAttributes" => self.get_sms_attributes(&ctx, &params),
            "SetSMSAttributes" => self.set_sms_attributes(&ctx, &params),
            "CheckIfPhoneNumberIsOptedOut" => self.check_phone_opted_out(&ctx, &params),
            "ListPhoneNumbersOptedOut" => self.list_phone_numbers_opted_out(&ctx, &params),
            "OptInPhoneNumber" => self.opt_in_phone_number(&ctx, &params),
            "CreatePlatformApplication" => self.create_platform_application(&ctx, &params),
            "DeletePlatformApplication" => self.delete_platform_application(&ctx, &params),
            "ListPlatformApplications" => self.list_platform_applications(&ctx, &params),
            "GetPlatformApplicationAttributes" => {
                self.get_platform_application_attributes(&ctx, &params)
            }
            "SetPlatformApplicationAttributes" => {
                self.set_platform_application_attributes(&ctx, &params)
            }
            "CreatePlatformEndpoint" => self.create_platform_endpoint(&ctx, &params),
            "DeleteEndpoint" => self.delete_endpoint(&ctx, &params),
            "ListEndpointsByPlatformApplication" => {
                self.list_endpoints_by_platform_application(&ctx, &params)
            }
            "GetEndpointAttributes" => self.get_endpoint_attributes(&ctx, &params),
            "SetEndpointAttributes" => self.set_endpoint_attributes(&ctx, &params),
            "GetDataProtectionPolicy" => self.get_data_protection_policy(&ctx, &params),
            "PutDataProtectionPolicy" => self.put_data_protection_policy(&ctx, &params),
            op => Err(ServiceException::new(
                "InvalidAction",
                format!("The action {op} is not valid for this endpoint."),
                400,
            )),
        }
    }

    fn create_topic(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let name = param_str(params, "Name").ok_or_else(|| {
            ServiceException::new("InvalidParameter", "Topic Name is required.", 400)
        })?;
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let arn = topic_arn(&name, &ctx.region, &ctx.account_id);

        if store.topics.contains_key(&arn) {
            return Ok(sns_response(
                &format!("    <TopicArn>{arn}</TopicArn>"),
                "CreateTopic",
                &ctx.request_id,
            ));
        }

        let attributes = extract_attributes(params);
        let topic = Topic {
            arn: arn.clone(),
            name,
            attributes,
            subscriptions: Vec::new(),
            data_protection_policy: param_str(params, "DataProtectionPolicy"),
        };
        store.topics.insert(arn.clone(), topic);

        Ok(sns_response(
            &format!("    <TopicArn>{arn}</TopicArn>"),
            "CreateTopic",
            &ctx.request_id,
        ))
    }

    fn delete_topic(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let arn = param_str(params, "TopicArn").ok_or_else(|| {
            ServiceException::new("InvalidParameter", "TopicArn is required.", 400)
        })?;
        let store = self.get_store(&ctx.account_id, &ctx.region);
        store.topics.remove(&arn);
        let subs_to_remove: Vec<String> = store
            .subscriptions
            .iter()
            .filter(|e| {
                let sub: &SnsSubscription = e.value();
                sub.topic_arn == arn
            })
            .map(|e| e.key().clone())
            .collect();
        for sub_arn in subs_to_remove {
            store.subscriptions.remove(&sub_arn);
        }
        Ok(sns_response("", "DeleteTopic", &ctx.request_id))
    }

    fn list_topics(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let mut xml = String::new();
        for entry in store.topics.iter() {
            let topic: &Topic = entry.value();
            xml.push_str(&format!(
                "      <member><TopicArn>{}</TopicArn></member>\n",
                topic.arn
            ));
        }
        let body = format!("    <Topics>\n{xml}    </Topics>");
        Ok(sns_response(&body, "ListTopics", &ctx.request_id))
    }

    fn get_topic_attributes(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let arn = param_str(params, "TopicArn").ok_or_else(|| {
            ServiceException::new("NotFound", "Topic does not exist.", 404)
        })?;
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let topic = store
            .topics
            .get(&arn)
            .ok_or_else(|| ServiceException::new("NotFound", "Topic does not exist.", 404))?;
        let mut xml = String::from("    <Attributes>\n");
        xml.push_str(&format!(
            "      <entry><key>TopicArn</key><value>{}</value></entry>\n",
            topic.arn
        ));
        xml.push_str(&format!(
            "      <entry><key>DisplayName</key><value>{}</value></entry>\n",
            topic.attributes.get("DisplayName").unwrap_or(&String::new())
        ));
        let sub_count = topic.subscriptions.len();
        xml.push_str(&format!(
            "      <entry><key>SubscriptionsConfirmed</key><value>{sub_count}</value></entry>\n"
        ));
        for (k, v) in &topic.attributes {
            xml.push_str(&format!(
                "      <entry><key>{k}</key><value>{v}</value></entry>\n"
            ));
        }
        xml.push_str("    </Attributes>");
        Ok(sns_response(&xml, "GetTopicAttributes", &ctx.request_id))
    }

    fn set_topic_attributes(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let arn = param_str(params, "TopicArn").unwrap_or_default();
        let attr_name = param_str(params, "AttributeName").unwrap_or_default();
        let attr_value = param_str(params, "AttributeValue").unwrap_or_default();
        let store = self.get_store(&ctx.account_id, &ctx.region);
        if let Some(mut topic) = store.topics.get_mut(&arn) {
            topic.attributes.insert(attr_name, attr_value);
        }
        Ok(sns_response("", "SetTopicAttributes", &ctx.request_id))
    }

    fn subscribe(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let topic_arn_val = param_str(params, "TopicArn").ok_or_else(|| {
            ServiceException::new("InvalidParameter", "TopicArn is required.", 400)
        })?;
        let protocol = param_str(params, "Protocol").ok_or_else(|| {
            ServiceException::new("InvalidParameter", "Protocol is required.", 400)
        })?;
        let endpoint = param_str(params, "Endpoint").unwrap_or_default();
        let store = self.get_store(&ctx.account_id, &ctx.region);

        if !store.topics.contains_key(&topic_arn_val) {
            return Err(ServiceException::new(
                "NotFound",
                "Topic does not exist.",
                404,
            ));
        }

        let sub_arn = subscription_arn(&topic_arn_val, &protocol);
        let sub = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn: topic_arn_val.clone(),
            protocol,
            endpoint,
            owner: ctx.account_id.clone(),
            pending_confirmation: false,
            raw_message_delivery: false,
            filter_policy: None,
            filter_policy_scope: "MessageAttributes".to_string(),
            attributes: HashMap::new(),
        };
        store.subscriptions.insert(sub_arn.clone(), sub);
        if let Some(mut topic) = store.topics.get_mut(&topic_arn_val) {
            topic.subscriptions.push(sub_arn.clone());
        }

        Ok(sns_response(
            &format!("    <SubscriptionArn>{sub_arn}</SubscriptionArn>"),
            "Subscribe",
            &ctx.request_id,
        ))
    }

    fn unsubscribe(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let sub_arn = param_str(params, "SubscriptionArn").unwrap_or_default();
        let store = self.get_store(&ctx.account_id, &ctx.region);
        if let Some((_, sub)) = store.subscriptions.remove(&sub_arn) {
            if let Some(mut topic) = store.topics.get_mut(&sub.topic_arn) {
                topic.subscriptions.retain(|s| s != &sub_arn);
            }
        }
        Ok(sns_response("", "Unsubscribe", &ctx.request_id))
    }

    fn confirm_subscription(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let _topic_arn = param_str(params, "TopicArn");
        let _token = param_str(params, "Token");
        let sub_arn = "arn:aws:sns:confirmed:000000000000:confirmed";
        Ok(sns_response(
            &format!("    <SubscriptionArn>{sub_arn}</SubscriptionArn>"),
            "ConfirmSubscription",
            &ctx.request_id,
        ))
    }

    fn list_subscriptions(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let mut xml = String::from("    <Subscriptions>\n");
        for entry in store.subscriptions.iter() {
            let sub: &SnsSubscription = entry.value();
            xml.push_str("      <member>\n");
            xml.push_str(&format!(
                "        <SubscriptionArn>{}</SubscriptionArn>\n",
                sub.subscription_arn
            ));
            xml.push_str(&format!("        <TopicArn>{}</TopicArn>\n", sub.topic_arn));
            xml.push_str(&format!(
                "        <Protocol>{}</Protocol>\n",
                sub.protocol
            ));
            xml.push_str(&format!(
                "        <Endpoint>{}</Endpoint>\n",
                sub.endpoint
            ));
            xml.push_str(&format!("        <Owner>{}</Owner>\n", sub.owner));
            xml.push_str("      </member>\n");
        }
        xml.push_str("    </Subscriptions>");
        Ok(sns_response(&xml, "ListSubscriptions", &ctx.request_id))
    }

    fn list_subscriptions_by_topic(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let topic_arn_val = param_str(params, "TopicArn").unwrap_or_default();
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let mut xml = String::from("    <Subscriptions>\n");
        for entry in store.subscriptions.iter() {
            let sub: &SnsSubscription = entry.value();
            if sub.topic_arn != topic_arn_val {
                continue;
            }
            xml.push_str("      <member>\n");
            xml.push_str(&format!(
                "        <SubscriptionArn>{}</SubscriptionArn>\n",
                sub.subscription_arn
            ));
            xml.push_str(&format!("        <TopicArn>{}</TopicArn>\n", sub.topic_arn));
            xml.push_str(&format!(
                "        <Protocol>{}</Protocol>\n",
                sub.protocol
            ));
            xml.push_str(&format!(
                "        <Endpoint>{}</Endpoint>\n",
                sub.endpoint
            ));
            xml.push_str(&format!("        <Owner>{}</Owner>\n", sub.owner));
            xml.push_str("      </member>\n");
        }
        xml.push_str("    </Subscriptions>");
        Ok(sns_response(
            &xml,
            "ListSubscriptionsByTopic",
            &ctx.request_id,
        ))
    }

    fn get_subscription_attributes(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let sub_arn = param_str(params, "SubscriptionArn").ok_or_else(|| {
            ServiceException::new("NotFound", "Subscription not found.", 404)
        })?;
        let store = self.get_store(&ctx.account_id, &ctx.region);
        let sub = store.subscriptions.get(&sub_arn).ok_or_else(|| {
            ServiceException::new("NotFound", "Subscription not found.", 404)
        })?;
        let mut xml = String::from("    <Attributes>\n");
        xml.push_str(&format!(
            "      <entry><key>SubscriptionArn</key><value>{}</value></entry>\n",
            sub.subscription_arn
        ));
        xml.push_str(&format!(
            "      <entry><key>TopicArn</key><value>{}</value></entry>\n",
            sub.topic_arn
        ));
        xml.push_str(&format!(
            "      <entry><key>Protocol</key><value>{}</value></entry>\n",
            sub.protocol
        ));
        xml.push_str(&format!(
            "      <entry><key>Endpoint</key><value>{}</value></entry>\n",
            sub.endpoint
        ));
        xml.push_str(&format!(
            "      <entry><key>RawMessageDelivery</key><value>{}</value></entry>\n",
            sub.raw_message_delivery
        ));
        xml.push_str(&format!(
            "      <entry><key>PendingConfirmation</key><value>{}</value></entry>\n",
            sub.pending_confirmation
        ));
        if let Some(ref fp) = sub.filter_policy {
            xml.push_str(&format!(
                "      <entry><key>FilterPolicy</key><value>{}</value></entry>\n",
                xml_escape(fp)
            ));
        }
        xml.push_str(&format!(
            "      <entry><key>FilterPolicyScope</key><value>{}</value></entry>\n",
            sub.filter_policy_scope
        ));
        for (k, v) in &sub.attributes {
            xml.push_str(&format!(
                "      <entry><key>{k}</key><value>{v}</value></entry>\n"
            ));
        }
        xml.push_str("    </Attributes>");
        Ok(sns_response(
            &xml,
            "GetSubscriptionAttributes",
            &ctx.request_id,
        ))
    }

    fn set_subscription_attributes(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let sub_arn = param_str(params, "SubscriptionArn").unwrap_or_default();
        let attr_name = param_str(params, "AttributeName").unwrap_or_default();
        let attr_value = param_str(params, "AttributeValue").unwrap_or_default();
        let store = self.get_store(&ctx.account_id, &ctx.region);
        if let Some(mut sub) = store.subscriptions.get_mut(&sub_arn) {
            match attr_name.as_str() {
                "RawMessageDelivery" => {
                    sub.raw_message_delivery = attr_value.to_lowercase() == "true"
                }
                "FilterPolicy" => sub.filter_policy = Some(attr_value),
                "FilterPolicyScope" => sub.filter_policy_scope = attr_value,
                _ => {
                    sub.attributes.insert(attr_name, attr_value);
                }
            }
        }
        Ok(sns_response(
            "",
            "SetSubscriptionAttributes",
            &ctx.request_id,
        ))
    }

    fn publish(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let _topic_arn_val = param_str(params, "TopicArn")
            .or_else(|| param_str(params, "TargetArn"))
            .ok_or_else(|| {
                ServiceException::new("InvalidParameter", "TopicArn or TargetArn is required.", 400)
            })?;
        let _message = param_str(params, "Message").ok_or_else(|| {
            ServiceException::new("InvalidParameter", "Message is required.", 400)
        })?;
        let _subject = param_str(params, "Subject");
        let message_id = uuid::Uuid::new_v4().to_string();

        let xml = format!("    <MessageId>{message_id}</MessageId>");
        Ok(sns_response(&xml, "Publish", &ctx.request_id))
    }

    fn publish_batch(
        &self,
        ctx: &RequestContext,
        params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let mut xml = String::from("    <Successful>\n");
        if let Some(entries) = params.get("PublishBatchRequestEntries").and_then(|v| v.as_array()) {
            for entry in entries {
                let id = entry.get("Id").and_then(|v| v.as_str()).unwrap_or("");
                let message_id = uuid::Uuid::new_v4().to_string();
                xml.push_str(&format!(
                    "      <member><Id>{id}</Id><MessageId>{message_id}</MessageId></member>\n"
                ));
            }
        }
        xml.push_str("    </Successful>");
        Ok(sns_response(&xml, "PublishBatch", &ctx.request_id))
    }

    fn tag_resource(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "TagResource", &ctx.request_id))
    }

    fn untag_resource(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "UntagResource", &ctx.request_id))
    }

    fn list_tags_for_resource(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <Tags/>",
            "ListTagsForResource",
            &ctx.request_id,
        ))
    }

    fn add_permission(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "AddPermission", &ctx.request_id))
    }

    fn remove_permission(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "RemovePermission", &ctx.request_id))
    }

    fn get_sms_attributes(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <attributes/>",
            "GetSMSAttributes",
            &ctx.request_id,
        ))
    }

    fn set_sms_attributes(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "SetSMSAttributes", &ctx.request_id))
    }

    fn check_phone_opted_out(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <isOptedOut>false</isOptedOut>",
            "CheckIfPhoneNumberIsOptedOut",
            &ctx.request_id,
        ))
    }

    fn list_phone_numbers_opted_out(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <phoneNumbers/>",
            "ListPhoneNumbersOptedOut",
            &ctx.request_id,
        ))
    }

    fn opt_in_phone_number(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "OptInPhoneNumber", &ctx.request_id))
    }

    fn create_platform_application(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let arn = format!(
            "arn:aws:sns:{}:{}:app/GCM/stub",
            ctx.region, ctx.account_id
        );
        Ok(sns_response(
            &format!("    <PlatformApplicationArn>{arn}</PlatformApplicationArn>"),
            "CreatePlatformApplication",
            &ctx.request_id,
        ))
    }

    fn delete_platform_application(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "",
            "DeletePlatformApplication",
            &ctx.request_id,
        ))
    }

    fn list_platform_applications(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <PlatformApplications/>",
            "ListPlatformApplications",
            &ctx.request_id,
        ))
    }

    fn get_platform_application_attributes(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <Attributes/>",
            "GetPlatformApplicationAttributes",
            &ctx.request_id,
        ))
    }

    fn set_platform_application_attributes(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "",
            "SetPlatformApplicationAttributes",
            &ctx.request_id,
        ))
    }

    fn create_platform_endpoint(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        let arn = format!(
            "arn:aws:sns:{}:{}:endpoint/GCM/stub/{}",
            ctx.region,
            ctx.account_id,
            uuid::Uuid::new_v4()
        );
        Ok(sns_response(
            &format!("    <EndpointArn>{arn}</EndpointArn>"),
            "CreatePlatformEndpoint",
            &ctx.request_id,
        ))
    }

    fn delete_endpoint(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "DeleteEndpoint", &ctx.request_id))
    }

    fn list_endpoints_by_platform_application(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <Endpoints/>",
            "ListEndpointsByPlatformApplication",
            &ctx.request_id,
        ))
    }

    fn get_endpoint_attributes(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "    <Attributes/>",
            "GetEndpointAttributes",
            &ctx.request_id,
        ))
    }

    fn set_endpoint_attributes(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response("", "SetEndpointAttributes", &ctx.request_id))
    }

    fn get_data_protection_policy(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "",
            "GetDataProtectionPolicy",
            &ctx.request_id,
        ))
    }

    fn put_data_protection_policy(
        &self,
        ctx: &RequestContext,
        _params: &serde_json::Value,
    ) -> Result<ServiceResponse, ServiceException> {
        Ok(sns_response(
            "",
            "PutDataProtectionPolicy",
            &ctx.request_id,
        ))
    }
}

fn sns_response(inner_xml: &str, action: &str, request_id: &str) -> ServiceResponse {
    let body = serializer::wrap_sns_query_response(inner_xml, action, request_id);
    ServiceResponse::xml(200, body)
}

fn param_str(params: &serde_json::Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn extract_attributes(params: &serde_json::Value) -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    if let Some(obj) = params.get("Attributes").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                attrs.insert(k.clone(), s.to_string());
            }
        }
    }
    for i in 1..=30 {
        let name_key = format!("Attributes.entry.{i}.key");
        let val_key = format!("Attributes.entry.{i}.value");
        if let (Some(name), Some(val)) = (param_str(params, &name_key), param_str(params, &val_key))
        {
            attrs.insert(name, val);
        } else {
            break;
        }
    }
    attrs
}

#[cfg(test)]
mod tests {
    use super::*;
    use ls_asf::context::RequestContext;
    use ls_asf::service::ServiceHandler;

    fn ctx(operation: &str) -> RequestContext {
        let mut c = RequestContext::new();
        c.service_name = "sns".to_string();
        c.operation = operation.to_string();
        c.region = "us-east-1".to_string();
        c.account_id = "000000000000".to_string();
        c
    }

    async fn create_topic(svc: &SnsService, name: &str) -> String {
        let params = serde_json::json!({"Name": name});
        svc.handle(ctx("CreateTopic"), params).await.unwrap();
        format!("arn:aws:sns:us-east-1:000000000000:{name}")
    }

    async fn subscribe_sqs(svc: &SnsService, topic_arn: &str, queue_arn: &str) -> String {
        let params = serde_json::json!({
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": queue_arn,
        });
        let resp = svc.handle(ctx("Subscribe"), params).await.unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let start = body.find("<SubscriptionArn>").unwrap() + "<SubscriptionArn>".len();
        let end = body[start..].find("</SubscriptionArn>").unwrap();
        body[start..start + end].to_string()
    }

    async fn set_sub_attr(svc: &SnsService, sub_arn: &str, name: &str, value: &str) {
        let params = serde_json::json!({
            "SubscriptionArn": sub_arn,
            "AttributeName": name,
            "AttributeValue": value,
        });
        svc.handle(ctx("SetSubscriptionAttributes"), params).await.unwrap();
    }

    async fn get_sub_attrs(svc: &SnsService, sub_arn: &str) -> String {
        let params = serde_json::json!({"SubscriptionArn": sub_arn});
        let resp = svc.handle(ctx("GetSubscriptionAttributes"), params).await.unwrap();
        String::from_utf8_lossy(&resp.body).to_string()
    }

    #[tokio::test]
    async fn set_and_get_filter_policy() {
        let svc = SnsService::new();
        let topic_arn = create_topic(&svc, "events").await;
        let sub_arn = subscribe_sqs(&svc, &topic_arn, "arn:aws:sqs:us-east-1:000:queue").await;

        let policy = r#"{"body":{"resourceType":["chat"]}}"#;
        set_sub_attr(&svc, &sub_arn, "FilterPolicy", policy).await;

        let attrs = get_sub_attrs(&svc, &sub_arn).await;
        assert!(attrs.contains("<key>FilterPolicy</key>"));
        assert!(attrs.contains("resourceType"));
    }

    #[tokio::test]
    async fn set_and_get_filter_policy_scope() {
        let svc = SnsService::new();
        let topic_arn = create_topic(&svc, "scoped").await;
        let sub_arn = subscribe_sqs(&svc, &topic_arn, "arn:aws:sqs:us-east-1:000:queue2").await;

        set_sub_attr(&svc, &sub_arn, "FilterPolicyScope", "MessageBody").await;

        let attrs = get_sub_attrs(&svc, &sub_arn).await;
        assert!(attrs.contains("<key>FilterPolicyScope</key>"));
        assert!(attrs.contains("<value>MessageBody</value>"));
    }

    #[tokio::test]
    async fn default_filter_policy_scope_is_message_attributes() {
        let svc = SnsService::new();
        let topic_arn = create_topic(&svc, "defaults").await;
        let sub_arn = subscribe_sqs(&svc, &topic_arn, "arn:aws:sqs:us-east-1:000:queue3").await;

        let attrs = get_sub_attrs(&svc, &sub_arn).await;
        assert!(attrs.contains("<key>FilterPolicyScope</key>"));
        assert!(attrs.contains("<value>MessageAttributes</value>"));
        assert!(!attrs.contains("<key>FilterPolicy</key>"));
    }

    #[tokio::test]
    async fn filter_policy_with_special_chars_is_escaped() {
        let svc = SnsService::new();
        let topic_arn = create_topic(&svc, "escape-test").await;
        let sub_arn = subscribe_sqs(&svc, &topic_arn, "arn:aws:sqs:us-east-1:000:queue4").await;

        let policy = r#"{"key":["a","b"]}"#;
        set_sub_attr(&svc, &sub_arn, "FilterPolicy", policy).await;

        let attrs = get_sub_attrs(&svc, &sub_arn).await;
        assert!(attrs.contains("<key>FilterPolicy</key>"));
        assert!(attrs.contains("&quot;key&quot;"));
    }

    #[tokio::test]
    async fn subscribe_and_set_raw_delivery() {
        let svc = SnsService::new();
        let topic_arn = create_topic(&svc, "raw-test").await;
        let sub_arn = subscribe_sqs(&svc, &topic_arn, "arn:aws:sqs:us-east-1:000:queue5").await;

        set_sub_attr(&svc, &sub_arn, "RawMessageDelivery", "true").await;

        let attrs = get_sub_attrs(&svc, &sub_arn).await;
        assert!(attrs.contains("<key>RawMessageDelivery</key><value>true</value>"));
    }

    #[tokio::test]
    async fn create_topic_and_subscribe() {
        let svc = SnsService::new();
        let topic_arn = create_topic(&svc, "my-topic").await;

        let params = serde_json::json!({
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": "arn:aws:sqs:us-east-1:000:my-queue",
        });
        let resp = svc.handle(ctx("Subscribe"), params).await.unwrap();
        assert_eq!(resp.status, 200);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<SubscriptionArn>"));
    }

    #[tokio::test]
    async fn subscribe_to_nonexistent_topic_fails() {
        let svc = SnsService::new();
        let params = serde_json::json!({
            "TopicArn": "arn:aws:sns:us-east-1:000:ghost",
            "Protocol": "sqs",
            "Endpoint": "arn:aws:sqs:us-east-1:000:queue",
        });
        let err = svc.handle(ctx("Subscribe"), params).await.unwrap_err();
        assert_eq!(err.status_code, 404);
    }
}
