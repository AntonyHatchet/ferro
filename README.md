# Ferro — SQS, SNS, S3

Lightweight AWS service emulator for local development and testing, written in Rust. Supports SQS, SNS, and S3 with a 17 MB Docker image and instant startup.

## Quick Start

```bash
cargo run --bin ferro
# Listens on port 4566 by default

# Override port:
GATEWAY_LISTEN=:4577 cargo run --bin ferro
```

## Supported Services

| Service | Operations | Protocol |
|---------|-----------|----------|
| SQS | 23 (CreateQueue, SendMessage, ReceiveMessage, DeleteMessage, etc.) | query + json |
| SNS | 42 (CreateTopic, Subscribe, Publish, etc.) | query |
| S3 | ~80 (CreateBucket, PutObject, GetObject, multipart, versioning, etc.) | rest-xml |

## Initialization

Resources can be pre-created at startup using two mechanisms:

### 1. Declarative Config (`init.json`)

Place an `init.json` file in the init directory (default: `./init/` or `/etc/ferro/init/`):

```json
{
  "sqs": [
    {
      "name": "orders-queue",
      "attributes": {
        "VisibilityTimeout": "60",
        "MessageRetentionPeriod": "86400"
      },
      "tags": {
        "env": "local"
      }
    },
    {
      "name": "orders-dlq"
    },
    {
      "name": "events.fifo",
      "attributes": {
        "FifoQueue": "true",
        "ContentBasedDeduplication": "true"
      }
    }
  ],
  "sns": [
    {
      "name": "order-events",
      "subscriptions": [
        {
          "protocol": "sqs",
          "endpoint": "arn:aws:sqs:us-east-1:000000000000:orders-queue",
          "raw_message_delivery": true
        }
      ]
    },
    {
      "name": "audit_log_created",
      "subscriptions": [
        {
          "protocol": "sqs",
          "endpoint": "arn:aws:sqs:us-east-1:000000000000:audit-log-chat-queue",
          "filter_policy": "{\"body\":{\"resourceType\":[\"chat\"]}}",
          "filter_policy_scope": "MessageBody"
        }
      ]
    },
    {
      "name": "alerts"
    }
  ],
  "s3": [
    {
      "name": "app-assets",
      "seed_dir": "./seed-data",
      "cors": true
    },
    {
      "name": "uploads",
      "versioning": true,
      "cors": true
    },
    {
      "name": "logs"
    }
  ]
}
```

SNS subscriptions support the following optional fields:
- `raw_message_delivery` (default: `false`) — when `true`, SNS delivers the raw message to SQS without wrapping it in the SNS envelope JSON. Set this for any SQS consumer that expects the original message body.
- `filter_policy` — a JSON string defining an SNS filter policy. Only messages matching the policy are delivered to this subscription (e.g. `"{\"body\":{\"resourceType\":[\"chat\"]}}"`).
- `filter_policy_scope` — either `"MessageAttributes"` (default) or `"MessageBody"`. Controls whether the filter policy is evaluated against message attributes or the message body.

S3 buckets support `seed_dir` to upload an entire directory tree at boot.

S3 buckets support `cors` for CORS configuration:
- `"cors": true` — permissive CORS (all origins, common methods, all headers) for local dev
- `"cors": [{ "allowed_origins": ["https://example.com"], "allowed_methods": ["GET", "PUT"], "allowed_headers": ["*"] }]` — explicit rules with optional `expose_headers` and `max_age_seconds`

### 2. Shell Scripts (`ready.d/`)

Place `.sh` or `.py` scripts in the `ready.d/` directory. They run after the server is ready:

```
init/
  init.json            # declarative config (runs before server starts)
  ready.d/
    01-setup.sh        # runs after server is listening
    02-migrate.py
```

Scripts receive these environment 
- `AWS_ENDPOINT_URL` — the local server URL (e.g. `http://localhost:4566`)
- `AWS_DEFAULT_REGION` — defaults to `us-east-1`
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — set to `test`

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `GATEWAY_LISTEN` | `:4566` | Server listen address |
| `FERRO_LOG` | `info` | Log level / filter (see [Logging](#logging) below) |
| `FERRO_INIT_DIR` | `./init` or `/etc/ferro/init` | Init scripts and config root |
| `FERRO_INIT_CONFIG` | (auto-discovered) | Explicit path to `init.json` |
| `FERRO_DATA_DIR` | `/var/lib/ferro` | Data directory |
| `FERRO_MAX_BODY_SIZE` | (unlimited) | Max request body size (e.g. `256mb`, `1gb`) |
| `AWS_DEFAULT_REGION` | `us-east-1` | Default region for init resources |
| `AWS_ACCOUNT_ID` | `000000000000` | Default account ID |

### Logging

Control log verbosity with `FERRO_LOG` (or `RUST_LOG`). Request logs use per-service targets:

| Target | Logs |
|--------|------|
| `ferro::s3` | S3 requests (PutObject, GetObject, etc.) |
| `ferro::sqs` | SQS requests (SendMessage, ReceiveMessage, etc.) |
| `ferro::sns` | SNS requests (Publish, Subscribe, etc.) |
| `ferro::http` | CORS preflights and unmatched requests |
| `ferro::init` | Startup and init.json resource creation |

Examples:

```yaml
environment:
  # Default — all requests at info level
  - FERRO_LOG=info
  # Quiet SQS polling, normal S3/SNS
  - FERRO_LOG=info,ferro::sqs=warn
  # Debug S3 only
  - FERRO_LOG=warn,ferro::s3=debug
  # Everything including CORS preflights
  - FERRO_LOG=debug
  # Silent except errors
  - FERRO_LOG=warn
```

### Init Directory Structure

```
init/
  init.json              # declarative resource config (optional)
  boot.d/                # reserved for future use
  start.d/               # reserved for future use
  ready.d/               # shell scripts run after server is ready
    01-create-tables.sh
    02-seed-data.sh
  shutdown.d/            # reserved for future use
```

## Examples

### SQS — Message Queue

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Create a queue
aws sqs create-queue --queue-name orders

# Create a FIFO queue
aws sqs create-queue --queue-name events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true

# Create a queue with a dead-letter queue
aws sqs create-queue --queue-name orders-dlq
aws sqs create-queue --queue-name orders \
  --attributes '{
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:orders-dlq\",\"maxReceiveCount\":\"3\"}"
  }'

# Send a message
aws sqs send-message \
  --queue-url http://localhost:4566/000000000000/orders \
  --message-body '{"orderId": "12345", "amount": 99.99}'

# Send with message attributes
aws sqs send-message \
  --queue-url http://localhost:4566/000000000000/orders \
  --message-body '{"orderId": "67890"}' \
  --message-attributes '{"EventType":{"DataType":"String","StringValue":"OrderCreated"}}'

# Send a batch
aws sqs send-message-batch \
  --queue-url http://localhost:4566/000000000000/orders \
  --entries '[
    {"Id":"1","MessageBody":"{\"orderId\":\"aaa\"}"},
    {"Id":"2","MessageBody":"{\"orderId\":\"bbb\"}"}
  ]'

# Receive messages (long-poll for up to 5 seconds)
aws sqs receive-message \
  --queue-url http://localhost:4566/000000000000/orders \
  --max-number-of-messages 10 \
  --wait-time-seconds 5

# Delete a message (use the ReceiptHandle from receive-message)
aws sqs delete-message \
  --queue-url http://localhost:4566/000000000000/orders \
  --receipt-handle "RECEIPT_HANDLE_HERE"

# Purge all messages
aws sqs purge-queue \
  --queue-url http://localhost:4566/000000000000/orders

# Get queue attributes
aws sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/orders \
  --attribute-names All

# List all queues
aws sqs list-queues

# Delete a queue
aws sqs delete-queue \
  --queue-url http://localhost:4566/000000000000/orders
```

### SNS — Pub/Sub Topics

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Create a topic
aws sns create-topic --name order-events

# Create a FIFO topic
aws sns create-topic --name order-events.fifo \
  --attributes FifoTopic=true,ContentBasedDeduplication=true

# Subscribe an SQS queue to the topic
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:order-events \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:orders

# Subscribe an HTTP endpoint
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:order-events \
  --protocol https \
  --notification-endpoint https://example.com/webhook

# Publish a message
aws sns publish \
  --topic-arn arn:aws:sns:us-east-1:000000000000:order-events \
  --message '{"orderId": "12345", "status": "shipped"}' \
  --subject "Order Update"

# Publish with message attributes
aws sns publish \
  --topic-arn arn:aws:sns:us-east-1:000000000000:order-events \
  --message "Order shipped" \
  --message-attributes '{"EventType":{"DataType":"String","StringValue":"OrderShipped"}}'

# List all topics
aws sns list-topics

# List subscriptions for a topic
aws sns list-subscriptions-by-topic \
  --topic-arn arn:aws:sns:us-east-1:000000000000:order-events

# Set subscription filter policy
aws sns set-subscription-attributes \
  --subscription-arn "SUBSCRIPTION_ARN_HERE" \
  --attribute-name FilterPolicy \
  --attribute-value '{"EventType":["OrderCreated","OrderShipped"]}'

# Unsubscribe
aws sns unsubscribe --subscription-arn "SUBSCRIPTION_ARN_HERE"

# Tag a topic
aws sns tag-resource \
  --resource-arn arn:aws:sns:us-east-1:000000000000:order-events \
  --tags Key=env,Value=local Key=team,Value=backend

# Delete a topic
aws sns delete-topic \
  --topic-arn arn:aws:sns:us-east-1:000000000000:order-events
```

### S3 — Object Storage

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Create a bucket
aws s3 mb s3://my-app

# Upload a file
aws s3 cp README.md s3://my-app/docs/readme.md

# Upload a directory tree
aws s3 sync ./public s3://my-app/static/

# Download a file
aws s3 cp s3://my-app/docs/readme.md ./downloaded.md

# List objects
aws s3 ls s3://my-app/
aws s3 ls s3://my-app/docs/ --recursive

# Copy between buckets
aws s3 cp s3://my-app/docs/readme.md s3://backup/docs/readme.md

# Delete a file
aws s3 rm s3://my-app/docs/readme.md

# Delete a bucket (must be empty)
aws s3 rb s3://my-app

# Force delete a bucket and all contents
aws s3 rb s3://my-app --force
```

**S3 with the API directly (for versioning, multipart, etc.):**

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket my-app \
  --versioning-configuration Status=Enabled

# Check versioning status
aws s3api get-bucket-versioning --bucket my-app

# Upload with metadata
aws s3api put-object \
  --bucket my-app \
  --key data/report.csv \
  --body report.csv \
  --content-type text/csv \
  --metadata '{"generated-by":"pipeline","version":"2"}'

# Get object with headers
aws s3api head-object --bucket my-app --key data/report.csv

# List object versions (when versioning is enabled)
aws s3api list-object-versions --bucket my-app

# Multipart upload (for large files)
UPLOAD_ID=$(aws s3api create-multipart-upload \
  --bucket my-app --key large-file.bin \
  --query UploadId --output text)

aws s3api upload-part \
  --bucket my-app --key large-file.bin \
  --upload-id "$UPLOAD_ID" \
  --part-number 1 --body part1.bin

aws s3api upload-part \
  --bucket my-app --key large-file.bin \
  --upload-id "$UPLOAD_ID" \
  --part-number 2 --body part2.bin

aws s3api complete-multipart-upload \
  --bucket my-app --key large-file.bin \
  --upload-id "$UPLOAD_ID" \
  --multipart-upload '{"Parts":[
    {"PartNumber":1,"ETag":"ETAG_FROM_PART1"},
    {"PartNumber":2,"ETag":"ETAG_FROM_PART2"}
  ]}'

# Set bucket policy
aws s3api put-bucket-policy --bucket my-app \
  --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::my-app/*"}]}'

# Configure bucket tagging
aws s3api put-bucket-tagging --bucket my-app \
  --tagging '{"TagSet":[{"Key":"env","Value":"local"},{"Key":"project","Value":"demo"}]}'

# Set CORS
aws s3api put-bucket-cors --bucket my-app \
  --cors-configuration '{"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET","PUT"],"AllowedHeaders":["*"]}]}'

# Configure notifications (e.g. to SQS)
aws s3api put-bucket-notification-configuration --bucket my-app \
  --notification-configuration '{"QueueConfigurations":[{"QueueArn":"arn:aws:sqs:us-east-1:000000000000:uploads","Events":["s3:ObjectCreated:*"]}]}'
```

### End-to-End: SNS -> SQS Fan-Out

A common pattern where an SNS topic fans out messages to multiple SQS queues:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# 1. Create the queues
aws sqs create-queue --queue-name email-notifications
aws sqs create-queue --queue-name push-notifications
aws sqs create-queue --queue-name analytics

# 2. Create the topic
aws sns create-topic --name user-events

# 3. Subscribe all queues to the topic
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:user-events \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:email-notifications

aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:user-events \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:push-notifications

aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:user-events \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:analytics

# 4. Publish a message — it arrives in all three queues
aws sns publish \
  --topic-arn arn:aws:sns:us-east-1:000000000000:user-events \
  --message '{"userId":"u-123","event":"signup"}'

# 5. Each consumer reads from its own queue
aws sqs receive-message \
  --queue-url http://localhost:4566/000000000000/email-notifications

aws sqs receive-message \
  --queue-url http://localhost:4566/000000000000/analytics
```

### Using with Python (boto3)

```python
import boto3

session = boto3.Session(region_name="us-east-1")
endpoint = "http://localhost:4566"

sqs = session.client("sqs", endpoint_url=endpoint)
sns = session.client("sns", endpoint_url=endpoint)
s3  = session.client("s3",  endpoint_url=endpoint)

# SQS
sqs.create_queue(QueueName="tasks")
sqs.send_message(
    QueueUrl="http://localhost:4566/000000000000/tasks",
    MessageBody='{"task": "process_image", "id": 42}',
)
response = sqs.receive_message(
    QueueUrl="http://localhost:4566/000000000000/tasks",
    MaxNumberOfMessages=1,
)
for msg in response.get("Messages", []):
    print(msg["Body"])
    sqs.delete_message(
        QueueUrl="http://localhost:4566/000000000000/tasks",
        ReceiptHandle=msg["ReceiptHandle"],
    )

# SNS
topic = sns.create_topic(Name="alerts")
sns.publish(TopicArn=topic["TopicArn"], Message="Server restarted")

# S3
s3.create_bucket(Bucket="data")
s3.put_object(Bucket="data", Key="hello.txt", Body=b"Hello from Python!")
obj = s3.get_object(Bucket="data", Key="hello.txt")
print(obj["Body"].read().decode())
```

### Using with JavaScript (AWS SDK v3)

```javascript
import { SQSClient, CreateQueueCommand, SendMessageCommand, ReceiveMessageCommand } from "@aws-sdk/client-sqs";
import { S3Client, CreateBucketCommand, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";

const config = { region: "us-east-1", endpoint: "http://localhost:4566" };

const sqs = new SQSClient(config);
const s3  = new S3Client({ ...config, forcePathStyle: true });

// SQS
await sqs.send(new CreateQueueCommand({ QueueName: "jobs" }));
await sqs.send(new SendMessageCommand({
  QueueUrl: "http://localhost:4566/000000000000/jobs",
  MessageBody: JSON.stringify({ job: "resize", imageId: 7 }),
}));
const { Messages } = await sqs.send(new ReceiveMessageCommand({
  QueueUrl: "http://localhost:4566/000000000000/jobs",
  MaxNumberOfMessages: 1,
}));
console.log(Messages?.[0]?.Body);

// S3
await s3.send(new CreateBucketCommand({ Bucket: "uploads" }));
await s3.send(new PutObjectCommand({
  Bucket: "uploads", Key: "photo.txt", Body: "image data here",
}));
const obj = await s3.send(new GetObjectCommand({ Bucket: "uploads", Key: "photo.txt" }));
console.log(await obj.Body.transformToString());
```

### Using with docker-compose

```yaml
version: "3.8"

services:
  ferro:
    image: ferro
    ports:
      - "4566:4566"
    volumes:
      - ./init:/etc/ferro/init
      - ferro-data:/var/lib/ferro
    environment:
      - AWS_DEFAULT_REGION=us-east-1
      - FERRO_MAX_BODY_SIZE=256mb

  app:
    build: .
    depends_on:
      - ferro
    environment:
      - AWS_ENDPOINT_URL=http://ferro:4566
      - AWS_REGION=us-east-1
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test

volumes:
  ferro-data:
```

With an `init/init.json`:

```json
{
  "sqs": [{ "name": "tasks" }, { "name": "results" }],
  "s3":  [{ "name": "uploads" }, { "name": "processed" }]
}
```

## Docker

```bash
# Build
docker build -t ferro .

# Run with init volume
docker run -p 4566:4566 -v ./init:/etc/ferro/init ferro

# Run with persistent data
docker run -p 4566:4566 \
  -v ./init:/etc/ferro/init \
  -v ferro-data:/var/lib/ferro \
  ferro
```

## Project Structure

```
crates/
  ls-gateway/    # HTTP server, routing, init system
  ls-asf/        # AWS Service Framework: protocol parser/serializer, service registry
  ls-store/      # Account/Region state store (dashmap-backed)
  ls-sqs/        # SQS service implementation
  ls-sns/        # SNS service implementation
  ls-s3/         # S3 service implementation
```
