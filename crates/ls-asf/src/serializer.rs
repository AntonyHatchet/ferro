use crate::error::ServiceException;

pub fn serialize_query_error(error: &ServiceException, request_id: &str) -> String {
    let error_type = if error.sender_fault {
        "Sender"
    } else {
        "Receiver"
    };

    format!(
        r#"<?xml version="1.0"?>
<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <Error>
    <Type>{error_type}</Type>
    <Code>{code}</Code>
    <Message>{message}</Message>
  </Error>
  <RequestId>{request_id}</RequestId>
</ErrorResponse>"#,
        code = xml_escape(&error.code),
        message = xml_escape(&error.message),
    )
}

pub fn serialize_json_error(error: &ServiceException, _request_id: &str) -> String {
    serde_json::json!({
        "__type": &error.code,
        "message": &error.message,
    })
    .to_string()
}

pub fn serialize_rest_xml_error(error: &ServiceException, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
  <RequestId>{request_id}</RequestId>
</Error>"#,
        code = xml_escape(&error.code),
        message = xml_escape(&error.message),
    )
}

pub fn wrap_query_response(inner_xml: &str, action: &str, request_id: &str) -> String {
    format!(
        r#"<{action}Response xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <{action}Result>
{inner_xml}
  </{action}Result>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
    )
}

pub fn wrap_sns_query_response(inner_xml: &str, action: &str, request_id: &str) -> String {
    format!(
        r#"<{action}Response xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <{action}Result>
{inner_xml}
  </{action}Result>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ServiceException;

    #[test]
    fn query_error_sender_fault() {
        let err = ServiceException::new("InvalidParameterValue", "Bad value", 400).with_sender_fault();
        let xml = serialize_query_error(&err, "req-123");
        assert!(xml.contains("<Type>Sender</Type>"));
        assert!(xml.contains("<Code>InvalidParameterValue</Code>"));
        assert!(xml.contains("<Message>Bad value</Message>"));
        assert!(xml.contains("<RequestId>req-123</RequestId>"));
    }

    #[test]
    fn query_error_receiver_fault() {
        let err = ServiceException::new("InternalError", "Something broke", 500);
        let xml = serialize_query_error(&err, "req-456");
        assert!(xml.contains("<Type>Receiver</Type>"));
    }

    #[test]
    fn query_error_escapes_xml() {
        let err = ServiceException::new("Code", "contains <html> & \"quotes\"", 400);
        let xml = serialize_query_error(&err, "req-789");
        assert!(xml.contains("&lt;html&gt;"));
        assert!(xml.contains("&amp;"));
        assert!(xml.contains("&quot;quotes&quot;"));
    }

    #[test]
    fn json_error_format() {
        let err = ServiceException::new("QueueDoesNotExist", "No such queue", 400);
        let json = serialize_json_error(&err, "req-111");
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["__type"], "QueueDoesNotExist");
        assert_eq!(parsed["message"], "No such queue");
    }

    #[test]
    fn rest_xml_error_format() {
        let err = ServiceException::new("NoSuchBucket", "Bucket not found", 404);
        let xml = serialize_rest_xml_error(&err, "req-222");
        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<Message>Bucket not found</Message>"));
        assert!(xml.contains("<RequestId>req-222</RequestId>"));
        assert!(xml.starts_with("<?xml"));
    }

    #[test]
    fn wrap_query_response_structure() {
        let inner = "    <QueueUrl>http://localhost/q</QueueUrl>";
        let xml = wrap_query_response(inner, "CreateQueue", "req-333");
        assert!(xml.contains("<CreateQueueResponse"));
        assert!(xml.contains("<CreateQueueResult>"));
        assert!(xml.contains("</CreateQueueResult>"));
        assert!(xml.contains("<RequestId>req-333</RequestId>"));
        assert!(xml.contains("http://localhost/q"));
    }

    #[test]
    fn wrap_sns_query_response_structure() {
        let inner = "    <TopicArn>arn:aws:sns:us-east-1:000:topic</TopicArn>";
        let xml = wrap_sns_query_response(inner, "CreateTopic", "req-444");
        assert!(xml.contains("xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\""));
        assert!(xml.contains("<CreateTopicResult>"));
        assert!(xml.contains("arn:aws:sns:us-east-1:000:topic"));
    }
}
