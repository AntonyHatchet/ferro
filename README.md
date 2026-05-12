Ferro — SQS, SNS, S3

Lightweight AWS service emulator for local development and testing, written in Rust.

<p align="center">
  <img src="https://github.com/user-attachments/assets/37bbc9ad-5fab-490a-8a21-fb2d95e54169" alt="Ferro banner" width="100%" />
</p>
<p align="center">
  <a href="https://github.com/AntonyHatchet/ferro/actions">
    <img src="https://img.shields.io/github/actions/workflow/status/AntonyHatchet/ferro/ci.yml?style=flat-square" />
  </a>
  <img src="https://img.shields.io/docker/image-size/ghcr.io/antonyhatchet/ferro/latest?style=flat-square" />
  <img src="https://img.shields.io/github/license/AntonyHatchet/ferro?style=flat-square" />
  <img src="https://img.shields.io/github/stars/AntonyHatchet/ferro?style=flat-square" />
</p>
<p align="center">
  <strong>⚡ Instant startup</strong>
  ·
  <strong>📦 17 MB Docker image</strong>
  ·
  <strong>🦀 Written in Rust</strong>
  ·
  <strong>🔌 AWS SDK compatible</strong>
</p>

⸻

Why Ferro?

Ferro is designed for developers who want:

* ⚡ Instant startup
* 📦 Tiny Docker images
* 🧪 Fast local integration testing
* 🔌 AWS SDK compatibility
* 🐳 Docker-first workflows
* 💨 Minimal resource usage

Unlike heavyweight cloud emulators, Ferro focuses on:

* speed
* simplicity
* developer experience

⸻

Features

Feature	Description
⚡ Instant startup	Starts in milliseconds
📦 Tiny image	~17 MB Docker image
🦀 Rust-powered	Fast and memory efficient
🔌 AWS compatible	Works with AWS SDKs & CLI
🧪 CI-friendly	Perfect for integration testing
🐳 Docker-first	Easy local environments

⸻

Supported Services

Service	Operations	Protocol
SQS	23+ operations	query + json
SNS	42+ operations	query
S3	~80 operations	rest-xml

⸻

Quick Start

Docker (Recommended)

docker run --rm \
  -p 4566:4566 \
  ghcr.io/antonyhatchet/ferro:latest

Ferro listens on:

http://localhost:4566

⸻

Cargo

cargo run --bin ferro

Override the port:

GATEWAY_LISTEN=:4577 cargo run --bin ferro

⸻

Golden Path Example

The fastest way to get started:

docker run -p 4566:4566 ghcr.io/antonyhatchet/ferro:latest
export AWS_ENDPOINT_URL=http://localhost:4566
aws sqs create-queue --queue-name jobs
aws sqs send-message \
  --queue-url http://localhost:4566/000000000000/jobs \
  --message-body 'hello world'
aws sqs receive-message \
  --queue-url http://localhost:4566/000000000000/jobs

⸻

Architecture

 AWS SDK / CLI
        │
        ▼
    Ferro Gateway
   ┌────┼────┐
   ▼    ▼    ▼
  SQS  SNS   S3

⸻

Designed For

* Local integration testing
* Docker Compose stacks
* Event-driven applications
* Queue-based architectures
* Offline development
* CI pipelines
* AWS SDK development

⸻

Example Workflows

<details>
<summary>SQS Examples</summary>
export AWS_ENDPOINT_URL=http://localhost:4566
# Create queue
aws sqs create-queue --queue-name orders
# Send message
aws sqs send-message \
  --queue-url http://localhost:4566/000000000000/orders \
  --message-body '{"orderId":"123"}'
# Receive messages
aws sqs receive-message \
  --queue-url http://localhost:4566/000000000000/orders
</details>
<details>
<summary>SNS → SQS Fan-Out</summary>
# Create queue
aws sqs create-queue --queue-name email-notifications
# Create topic
aws sns create-topic --name user-events
# Subscribe queue
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:user-events \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:email-notifications
</details>
<details>
<summary>S3 Examples</summary>
export AWS_ENDPOINT_URL=http://localhost:4566
# Create bucket
aws s3 mb s3://my-app
# Upload file
aws s3 cp README.md s3://my-app/docs/readme.md
# List objects
aws s3 ls s3://my-app/
</details>

⸻

Initialization

Resources can be pre-created during startup using:

* init.json
* ready.d/ scripts

Example:

init/
├── init.json
└── ready.d/
    ├── 01-setup.sh
    └── 02-seed.py

⸻

Example init.json

{
  "sqs": [
    { "name": "orders" },
    { "name": "events.fifo" }
  ],
  "sns": [
    { "name": "order-events" }
  ],
  "s3": [
    { "name": "uploads", "versioning": true }
  ]
}

⸻

Docker Compose

version: "3.8"
services:
  ferro:
    image: ghcr.io/antonyhatchet/ferro:latest
    ports:
      - "4566:4566"
    volumes:
      - ./init:/etc/ferro/init
      - ferro-data:/var/lib/ferro
volumes:
  ferro-data:

⸻

Environment Variables

Variable	Default	Purpose
GATEWAY_LISTEN	:4566	Server listen address
FERRO_LOG	info	Log level
FERRO_INIT_DIR	./init	Init config directory
FERRO_DATA_DIR	/var/lib/ferro	Persistent data
AWS_DEFAULT_REGION	us-east-1	AWS region

⸻

Logging

# Default
FERRO_LOG=info
# Debug S3 only
FERRO_LOG=warn,ferro::s3=debug
# Silence SQS polling
FERRO_LOG=info,ferro::sqs=warn

⸻

Comparison

	Ferro	LocalStack
Startup speed	⚡ Instant	Slower
Docker size	~17 MB	Large
Focus	SQS/SNS/S3	Full AWS
Memory usage	Low	Higher
Language	Rust	Python

⸻

Project Structure

crates/
├── ls-gateway/   # HTTP server + routing
├── ls-asf/       # AWS service framework
├── ls-store/     # Shared state store
├── ls-sqs/       # SQS implementation
├── ls-sns/       # SNS implementation
└── ls-s3/        # S3 implementation

⸻

Current Status

Ferro is under active development.

Implemented:

* ✅ SQS
* ✅ SNS
* ✅ S3

Planned:

* ⏳ Lambda
* ⏳ DynamoDB
* ⏳ Web UI
* ⏳ Metrics endpoint

⸻

Contributing

PRs and issues are welcome.

If you find compatibility issues with AWS SDKs or APIs, open an issue with:

* SDK version
* Request example
* Expected behavior
* Actual behavior

⸻

License

MIT
