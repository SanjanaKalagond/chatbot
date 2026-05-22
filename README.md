# Salesforce Chatbot - Technical Document

**Project:** Salesforce Chatbot  
**Environment:** Production (ap-south-1)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Architecture](#system-architecture)
3. [AWS Infrastructure](#aws-infrastructure)
4. [Quick Operations Guide](#quick-operations-guide)
5. [Deployment Guide](#deployment-guide)
6. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
7. [Code Repository & Structure](#code-repository--structure)
8. [Configuration Management](#configuration-management)
9. [Appendix: All Commands](#appendix-all-commands)

---

## Executive Summary

The Salesforce Chatbot is an AI-powered conversational interface that allows users to query Salesforce CRM data, B2B accounts, customer transcripts, and uploaded documents using natural language. The system uses Google Gemini AI for language processing and FAISS for vector-based document retrieval.

### Key URLs

| Service | URL |
|---------|-----|
| **Frontend (UI)** | https://chatbot.tonal-ops.com |
| **Backend API Docs** | https://chatbot.tonal-ops.com/docs |
| **Cognito Login** | https://auth.tonal-ops.com/login?client_id=ajbu174or2k1jce76u2umssvv&response_type=code&scope=openid+email+profile&redirect_uri=https%3A%2F%2Fchatbot.tonal-ops.com%2F |
| **AWS Console** | https://ap-south-1.console.aws.amazon.com/ecs/v2/clusters/company-chatbot-prod-cluster |

### Current Status

- **Production Environment:** Stable and running
- **Services:** 2/3 active (API + UI running, Sync service disabled)
- **Authentication:** Cognito configured
---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        End Users                                │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  Route53 DNS Records  │
         │  chatbot.tonal-ops.com│
         │  auth.tonal-ops.com   │
         └───────────┬───────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  Application Load     │
         │  Balancer (ALB)       │
         │  + HTTPS (ACM Cert)   │
         └───────────┬───────────┘
                     │
         ┌───────────┴────────────┐
         │                        │
         ▼                        ▼
┌─────────────────┐      ┌─────────────────┐
│  ECS Service    │      │  ECS Service    │
│  UI (Streamlit) │      │  API (FastAPI)  │
│  Port: 8501     │      │  Port: 8000     │
└────────┬────────┘      └────────┬────────┘
         │                        │
         │                        ▼
         │               ┌─────────────────┐
         │               │  External APIs  │
         │               │  - Salesforce   │
         │               │  - Gemini AI    │
         │               │  - AWS S3       │
         │               │  - RDS Postgres │
         │               └─────────────────┘
         │
         └────────────────┐
                          ▼
                 ┌─────────────────┐
                 │  AWS Cognito    │
                 │  User Pool      │
                 └─────────────────┘
```

### Technology Stack

| Layer | Technology |
|-------|------------|
| **Frontend** | Streamlit (Python web framework) |
| **Backend API** | FastAPI (Python async web framework) |
| **AI/LLM** | Google Gemini AI |
| **Vector DB** | FAISS (Facebook AI Similarity Search) |
| **Database** | AWS RDS PostgreSQL |
| **Storage** | AWS S3 (documents + FAISS indexes) |
| **Container Registry** | AWS ECR |
| **Orchestration** | AWS ECS Fargate |
| **Load Balancer** | AWS Application Load Balancer |
| **DNS** | AWS Route53 |
| **Authentication** | AWS Cognito |
| **IaC** | Terraform |
| **CI/CD** | Manual (GitHub Codespaces) |

### Service Components

1. **API Service** (`company-chatbot-prod-api-svc`)
   - FastAPI application
   - Handles chat queries, document uploads, RAG retrieval
   - Connects to Salesforce, Gemini AI, RDS, S3
   - CPU: 1024, Memory: 2048 MB
   - Desired Count: 1 task

2. **UI Service** (`company-chatbot-prod-ui-svc`)
   - Streamlit web interface
   - User-facing chat interface
   - CPU: 512, Memory: 1024 MB
   - Desired Count: 1 task

3. **Sync Service** (`company-chatbot-prod-sync-svc`)
   - Incremental Salesforce data sync
   - Currently DISABLED (desired count = 0)
   - CPU: 1024, Memory: 2048 MB

---

### AWS Infrastructure

<p align="center">
  <img src="images/architecture.png" width="900"/>
</p>

## Operations Guide
### Emergency Stop

**When:** System issues, security incident, cost control, maintenance

```bash
# Stop API Service
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-api-svc \
  --desired-count 0 \
  --region ap-south-1

# Stop UI Service
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-ui-svc \
  --desired-count 0 \
  --region ap-south-1

# Verify services are stopped
aws ecs describe-services \
  --cluster company-chatbot-prod-cluster \
  --services company-chatbot-prod-api-svc company-chatbot-prod-ui-svc \
  --region ap-south-1 \
  --query 'services[*].{Service:serviceName,Desired:desiredCount,Running:runningCount}' \
  --output table
```

**Expected Output:**
```
---------------------------------------------------------------------
|                        DescribeServices                          |
+---------------------------------+-----------+---------------------+
|            Service              | Desired   |      Running        |
+---------------------------------+-----------+---------------------+
|  company-chatbot-prod-api-svc  |  0        |  0                  |
|  company-chatbot-prod-ui-svc   |  0        |  0                  |
+---------------------------------+-----------+---------------------+
```

### Restart Services

**When:** After maintenance, after code deployment, daily operations

```bash
# Start API Service
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-api-svc \
  --desired-count 1 \
  --region ap-south-1

# Start UI Service
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-ui-svc \
  --desired-count 1 \
  --region ap-south-1

# Wait 30-60 seconds for tasks to start, then verify
aws ecs describe-services \
  --cluster company-chatbot-prod-cluster \
  --services company-chatbot-prod-api-svc company-chatbot-prod-ui-svc \
  --region ap-south-1 \
  --query 'services[*].{Service:serviceName,Desired:desiredCount,Running:runningCount,Status:status}' \
  --output table
```
### Check Service Status

```bash
# Quick status check
aws ecs describe-services \
  --cluster company-chatbot-prod-cluster \
  --services company-chatbot-prod-api-svc company-chatbot-prod-ui-svc \
  --region ap-south-1 \
  --query 'services[*].{Service:serviceName,Status:status,Desired:desiredCount,Running:runningCount,Pending:pendingCount}' \
  --output table

# Detailed task information
aws ecs list-tasks \
  --cluster company-chatbot-prod-cluster \
  --service-name company-chatbot-prod-api-svc \
  --region ap-south-1

# Get task details (replace TASK_ID with actual task ID from above)
aws ecs describe-tasks \
  --cluster company-chatbot-prod-cluster \
  --tasks TASK_ID \
  --region ap-south-1
```

### View Logs

```bash
# View API logs (last 50 entries)
aws logs tail /ecs/company-chatbot-prod/api \
  --follow \
  --region ap-south-1

# View UI logs
aws logs tail /ecs/company-chatbot-prod/ui \
  --follow \
  --region ap-south-1

# View logs from specific time range
aws logs filter-log-events \
  --log-group-name /ecs/company-chatbot-prod/api \
  --start-time $(date -u -d '1 hour ago' +%s)000 \
  --region ap-south-1

# Search for errors
aws logs filter-log-events \
  --log-group-name /ecs/company-chatbot-prod/api \
  --filter-pattern "ERROR" \
  --region ap-south-1
```

### Enable/Disable Sync Service

```bash
# Enable sync service (incremental Salesforce data sync)
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-sync-svc \
  --desired-count 1 \
  --region ap-south-1

# Disable sync service
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-sync-svc \
  --desired-count 0 \
  --region ap-south-1

# Check sync service status
aws ecs describe-services \
  --cluster company-chatbot-prod-cluster \
  --services company-chatbot-prod-sync-svc \
  --region ap-south-1 \
  --query 'services[0].{Name:serviceName,Status:status,Desired:desiredCount,Running:runningCount}'
```

### Health Checks

```bash
# Check API health endpoint
curl https://chatbot.tonal-ops.com/api/health

# Expected response:
# {"status":"healthy"}

# Check if services are responding
curl -I https://chatbot.tonal-ops.com

# Check ALB target health
aws elbv2 describe-target-health \
  --target-group-arn $(aws elbv2 describe-target-groups \
    --region ap-south-1 \
    --query "TargetGroups[?contains(TargetGroupName, 'company-chatbot-prod')].TargetGroupArn" \
    --output text | head -1) \
  --region ap-south-1
```

---

## Deployment Guide
### Code Location

**Primary Repository:** GitHub Codespaces  
**Account:** sanjanakalagond-tonal  
**Workspace:** salesforce-chatbot

### Build and Push Docker Images

```bash
# Navigate to project directory
cd /path/to/salesforce-chatbot

# Get ECR login token
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com

# Build API image
docker build -t company-chatbot-prod-api -f dockerfile .

# Tag API image
docker tag company-chatbot-prod-api:latest \
  <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-api:latest

# Push API image
docker push <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-api:latest

# Build UI image
docker build -t company-chatbot-prod-ui -f dockerfile.streamlit .

# Tag UI image
docker tag company-chatbot-prod-ui:latest \
  <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-ui:latest

# Push UI image
docker push <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-ui:latest

# Build Sync image (same as API image with different entrypoint)
docker tag company-chatbot-prod-api:latest \
  <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-sync:latest

docker push <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-sync:latest
```

### Deploy with Terraform

```bash
# Navigate to terraform directory
cd terraform/

# Initialize Terraform (first time only)
terraform init

# Create/update terraform.tfvars with your configuration
# (See Configuration Management section for required variables)

# Preview changes
terraform plan

# Apply changes
terraform apply

# To see what resources exist
terraform state list

# To see details of a specific resource
terraform state show aws_ecs_service.api
```

### Manual ECS Service Update (After Image Push)

```bash
# Update API service to use new image
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-api-svc \
  --force-new-deployment \
  --region ap-south-1

# Update UI service to use new image
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-ui-svc \
  --force-new-deployment \
  --region ap-south-1

# Monitor deployment
aws ecs describe-services \
  --cluster company-chatbot-prod-cluster \
  --services company-chatbot-prod-api-svc company-chatbot-prod-ui-svc \
  --region ap-south-1 \
  --query 'services[*].{Service:serviceName,Status:status,Deployments:deployments[*].{Status:status,Desired:desiredCount,Running:runningCount,Pending:pendingCount}}'
```

### Rollback Procedure

```bash
# List task definition revisions
aws ecs list-task-definitions \
  --family-prefix company-chatbot-prod-api \
  --region ap-south-1

# Update service to use previous task definition revision
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-api-svc \
  --task-definition company-chatbot-prod-api:PREVIOUS_REVISION_NUMBER \
  --region ap-south-1
```
### CloudWatch Metrics to Monitor

**ECS Metrics:**
- `CPUUtilization` - Should stay below 80%
- `MemoryUtilization` - Should stay below 80%
- `DesiredTaskCount` vs `RunningTaskCount` - Should match
- `TargetResponseTime` - Should be < 1000ms

**ALB Metrics:**
- `TargetResponseTime` - API response latency
- `HTTPCode_Target_4XX_Count` - Client errors
- `HTTPCode_Target_5XX_Count` - Server errors
- `HealthyHostCount` - Should match desired task count
- `UnHealthyHostCount` - Should be 0

**RDS Metrics:**
- `DatabaseConnections` - Current DB connections
- `CPUUtilization` - Database CPU usage
- `FreeableMemory` - Available memory
- `DiskQueueDepth` - I/O operations queue

## Code Repository & Structure
### Directory Structure

```
salesforce-chatbot/
├── app/                          # Main application code
│   ├── config.py                # Configuration management
│   ├── main.py                  # FastAPI application entry point
│   ├── json_sanitize.py         # JSON sanitization utilities
│   ├── database/                # Database layer
│   │   ├── postgres.py          # PostgreSQL connection
│   │   ├── schema.py            # Database schema definitions
│   │   └── sync_metadata.py     # Sync tracking
│   ├── ingestion/               # Data ingestion pipelines
│   │   ├── salesforce_to_postgres.py    # Salesforce → PostgreSQL
│   │   ├── b2b_accounts_pipeline.py     # B2B data pipeline
│   │   ├── transcript_pipeline.py       # Transcript processing
│   │   ├── document_pipeline.py         # Document ingestion
│   │   ├── document_to_s3.py           # S3 document upload
│   │   ├── build_faiss_index.py        # FAISS index builder
│   │   └── incremental_sync.py         # Incremental sync logic
│   ├── llm/                     # Language model layer
│   │   ├── gemini_client.py     # Google Gemini API client
│   │   ├── orchestrator.py      # Query orchestration
│   │   ├── sql_generator.py     # SQL generation for CRM
│   │   ├── sql_generator_b2b.py # SQL generation for B2B
│   │   └── b2b_query_catalog.py # B2B query templates
│   ├── rag/                     # Retrieval-Augmented Generation
│   │   ├── blob_parser.py       # Document parsing (PDF, DOCX, etc.)
│   │   ├── chunking.py          # Text chunking
│   │   ├── embeddings.py        # Text embeddings generation
│   │   ├── retrieval.py         # Document retrieval
│   │   └── vector_store.py      # FAISS vector store management
│   ├── salesforce/              # Salesforce integration
│   │   ├── auth.py              # Salesforce authentication
│   │   ├── bulk_client.py       # Bulk API client
│   │   ├── extractor.py         # Data extraction
│   │   ├── live_fetcher.py      # Real-time data fetcher
│   │   └── objects.py           # Salesforce object definitions
│   └── sentiment/               # Sentiment analysis
│       └── sentiment_model.py   # Sentiment classification
├── services/                    # Business logic layer
│   ├── chat_service.py          # Chat handling
│   ├── customer_service.py      # Customer data service
│   ├── ingestion_service.py     # Ingestion orchestration
│   ├── rag_service.py           # RAG service
│   ├── sql_service.py           # SQL execution service
│   └── transcript_service.py    # Transcript management
├── streamlit/                   # Frontend UI
│   └── app.py                   # Streamlit web app
├── scripts/                     # Utility scripts
│   ├── create_tables.py         # Database table creation
│   ├── run_full_ingestion.py    # Full data ingestion
│   └── run_incremental_sync.py  # Incremental sync runner
├── terraform/                   # Infrastructure as Code
│   ├── main.tf                  # Main Terraform config
│   ├── variables.tf             # Variable definitions
│   ├── output.tf                # Output definitions
│   ├── providers.tf             # Provider configuration
│   ├── versions.tf              # Version constraints
│   ├── terraform.tfvars.example # Example variables file
│   └── README.md                # Terraform documentation
├── dockerfile                   # API/Sync service Dockerfile
├── dockerfile.streamlit         # UI service Dockerfile
├── docker-compose.yml           # Local development setup
├── requirements.txt             # Python 
```

### Key Files Explained

**app/main.py**
- FastAPI application
- Endpoints: `/chat`, `/upload`, `/save_interaction`, `/clear_session_docs`, `/health`
- CORS middleware, rate limiting, security headers

**streamlit/app.py**
- User interface built with Streamlit
- Chat interface with history
- Document upload functionality
- Query examples and help sidebar

**app/llm/orchestrator.py**
- Main query processing logic
- Routes queries to appropriate handlers (SQL, RAG, B2B, Hybrid, General)
- Integrates with Gemini AI

**app/ingestion/incremental_sync.py**
- Runs periodic sync from Salesforce to PostgreSQL
- Updates FAISS indexes
- Executed by sync service every 20 minutes (when enabled)

**terraform/main.tf**
- Defines all AWS infrastructure
- VPC, subnets, security groups
- ECS cluster, services, task definitions
- ALB, target groups, listeners
- Route53 DNS records
- Cognito domain configuration

### Environment Variables

The application uses the following environment variables:

```env
# Salesforce
SALESFORCE_CLIENT_ID=
SALESFORCE_CLIENT_SECRET=
SALESFORCE_USERNAME=
SALESFORCE_PASSWORD=
SALESFORCE_ACCESS_TOKEN=

# AWS
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=ap-south-1

# Storage
S3_BUCKET_NAME=salesforce-chatbot-data
FAISS_BUCKET_NAME=salesforce-chatbot-faiss-index

# AI/ML
GEMINI_API_KEY=

# Database
DATABASE_URL=postgresql://postgres:SFchatbot!@company-sf-chatbot-db.ciotza7hmbyq.ap-south-1.rds.amazonaws.com:5432/sf_chatbot
DB_PORT=5432
DB_NAME=sf_chatbot
DB_USER=postgres
DB_PASSWORD=SFchatbot!

# Application
PYTHONPATH=/workspaces/chatbot
```

## Configuration Management

### Terraform Variables

Create `terraform/terraform.tfvars` file with the following:

```hcl
# Project Configuration
project_name = "company-chatbot"
environment  = "prod"
aws_region   = "ap-south-1"

# Network Configuration
vpc_cidr            = "10.0.0.0/16"
public_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]

# ECS Configuration
api_desired_count  = 1
ui_desired_count   = 1
sync_enabled       = false  # Set to true to enable incremental sync
sync_desired_count = 0      # Keep at 0 to prevent cost issues

# Container Images (replace with your ECR URLs)
api_image  = "<AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-api:latest"
ui_image   = "<AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-ui:latest"
sync_image = "<AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-sync:latest"

# ALB Configuration
enable_alb            = true
alb_resource_suffix   = "v2"
ecs_assign_public_ip  = true
redirect_http_to_https = true

# Domain Configuration
domain_name          = "chatbot.tonal-ops.com"
hosted_zone_id       = "<YOUR_ROUTE53_HOSTED_ZONE_ID>"
alb_certificate_arn  = "<YOUR_ACM_CERTIFICATE_ARN>"

# Cognito Configuration
cognito_user_pool_id           = "<YOUR_COGNITO_USER_POOL_ID>"
cognito_custom_domain          = "auth.tonal-ops.com"
cognito_certificate_arn        = "<YOUR_COGNITO_ACM_CERTIFICATE_ARN>"
cognito_custom_domain_zone_id  = "<YOUR_ROUTE53_HOSTED_ZONE_ID>"

# Application Secrets (sensitive - use terraform.tfvars, not version control)
aws_access_key_id       = "<YOUR_AWS_ACCESS_KEY>"
aws_secret_access_key   = "<YOUR_AWS_SECRET_KEY>"
database_url            = "postgresql://user:password@host:5432/dbname"
s3_bucket_name          = "<YOUR_S3_BUCKET>"
faiss_bucket_name       = "<YOUR_FAISS_BUCKET>"
gemini_api_key          = "<YOUR_GEMINI_API_KEY>"

# Salesforce Configuration
salesforce_client_id     = "<YOUR_SALESFORCE_CLIENT_ID>"
salesforce_client_secret = "<YOUR_SALESFORCE_CLIENT_SECRET>"
salesforce_username      = "<YOUR_SALESFORCE_USERNAME>"
salesforce_password      = "<YOUR_SALESFORCE_PASSWORD>"
salesforce_access_token  = "<YOUR_SALESFORCE_ACCESS_TOKEN>"

# Resource Sizing
api_cpu    = 1024
api_memory = 2048
ui_cpu     = 512
ui_memory  = 1024
sync_cpu   = 1024
sync_memory = 2048

# Sync Configuration
sync_interval_seconds = 1200  # 20 minutes
```

### Updating Configuration

```bash
# Edit terraform.tfvars
nano terraform/terraform.tfvars

# Apply changes
cd terraform/
terraform plan
terraform apply

# The apply will automatically update task definitions with new env vars
# Force a new deployment to pick up the changes
aws ecs update-service \
  --cluster company-chatbot-prod-cluster \
  --service company-chatbot-prod-api-svc \
  --force-new-deployment \
  --region ap-south-1
```

---
### Enabling Cognito Authentication
**To Enable Authentication:**

1. Update ALB listener rules to require authentication:

```bash
# Get listener ARN
aws elbv2 describe-listeners \
  --load-balancer-arn $(aws elbv2 describe-load-balancers --names company-chatbot-prod-v2-alb --region ap-south-1 --query 'LoadBalancers[0].LoadBalancerArn' --output text) \
  --region ap-south-1
```

2. Modify Terraform configuration:

```hcl
# In terraform/main.tf, uncomment or add authentication action to ALB listener
# This requires updating the listener rules to include:

resource "aws_lb_listener_rule" "cognito_auth" {
  listener_arn = aws_lb_listener.https[0].arn
  priority     = 1

  action {
    type = "authenticate-cognito"

    authenticate_cognito {
      user_pool_arn       = aws_cognito_user_pool.main.arn
      user_pool_client_id = aws_cognito_user_pool_client.main.id
      user_pool_domain    = var.cognito_custom_domain

      on_unauthenticated_request = "authenticate"
    }
  }

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ui[0].arn
  }

  condition {
    path_pattern {
      values = ["/*"]
    }
  }
}
```

3. Apply Terraform changes:

```bash
cd terraform/
terraform plan
terraform apply
```

**Important:** After enabling Cognito authentication:
- All users must log in via https://auth.tonal-ops.com
- username: demo@tonal.com
- Password: TonalChatbot10

### User Management

```bash
# Create a new user
aws cognito-idp admin-create-user \
  --user-pool-id <COGNITO_USER_POOL_ID> \
  --username user@example.com \
  --user-attributes Name=email,Value=user@example.com Name=email_verified,Value=true \
  --region ap-south-1

# Set user password
aws cognito-idp admin-set-user-password \
  --user-pool-id <COGNITO_USER_POOL_ID> \
  --username user@example.com \
  --password "TempPassword123!" \
  --permanent \
  --region ap-south-1

# List users
aws cognito-idp list-users \
  --user-pool-id <COGNITO_USER_POOL_ID> \
  --region ap-south-1

# Delete user
aws cognito-idp admin-delete-user \
  --user-pool-id <COGNITO_USER_POOL_ID> \
  --username user@example.com \
  --region ap-south-1
```

### IAM Roles and Permissions

**ECS Execution Role** (`company-chatbot-prod-ecs-exec-role`):
- Pulls images from ECR
- Writes logs to CloudWatch
- Access to Secrets Manager (if configured)

**ECS Task Role** (`company-chatbot-prod-ecs-task-role`):
- Access to S3 buckets (documents, FAISS)
- Access to Salesforce APIs
- Access to Gemini APIs
- Access to RDS (via security groups)

## Appendix: All Commands

### ECS Service Management

```bash
# Stop all services
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-api-svc --desired-count 0 --region ap-south-1
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-ui-svc --desired-count 0 --region ap-south-1
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-sync-svc --desired-count 0 --region ap-south-1

# Start all services
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-api-svc --desired-count 1 --region ap-south-1
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-ui-svc --desired-count 1 --region ap-south-1

# Verify service status
aws ecs describe-services --cluster company-chatbot-prod-cluster --services company-chatbot-prod-api-svc company-chatbot-prod-ui-svc company-chatbot-prod-sync-svc --region ap-south-1 --query 'services[*].{Service:serviceName,Desired:desiredCount,Running:runningCount,Status:status}' --output table

# Force new deployment
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-api-svc --force-new-deployment --region ap-south-1
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-ui-svc --force-new-deployment --region ap-south-1

# List running tasks
aws ecs list-tasks --cluster company-chatbot-prod-cluster --region ap-south-1

# Describe specific task
aws ecs describe-tasks --cluster company-chatbot-prod-cluster --tasks TASK_ARN --region ap-south-1

# Stop specific task
aws ecs stop-task --cluster company-chatbot-prod-cluster --task TASK_ARN --region ap-south-1

# Update service with new task definition
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-api-svc --task-definition company-chatbot-prod-api:REVISION --region ap-south-1

# Scale service
aws ecs update-service --cluster company-chatbot-prod-cluster --service company-chatbot-prod-api-svc --desired-count 2 --region ap-south-1
```
### ECR Management

```bash
# Login to ECR
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com

# List repositories
aws ecr describe-repositories --region ap-south-1

# List images in repository
aws ecr list-images --repository-name company-chatbot-prod-api --region ap-south-1

# Describe image
aws ecr describe-images --repository-name company-chatbot-prod-api --image-ids imageTag=latest --region ap-south-1

# Delete image
aws ecr batch-delete-image --repository-name company-chatbot-prod-api --image-ids imageTag=OLD_TAG --region ap-south-1

# Lifecycle policy (delete old images)
aws ecr put-lifecycle-policy --repository-name company-chatbot-prod-api --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"Keep last 10 images","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10},"action":{"type":"expire"}}]}' --region ap-south-1
```

### ALB Management

```bash
# Describe load balancer
aws elbv2 describe-load-balancers --names company-chatbot-prod-v2-alb --region ap-south-1

# List target groups
aws elbv2 describe-target-groups --load-balancer-arn ALB_ARN --region ap-south-1

# Check target health
aws elbv2 describe-target-health --target-group-arn TARGET_GROUP_ARN --region ap-south-1

# Describe listeners
aws elbv2 describe-listeners --load-balancer-arn ALB_ARN --region ap-south-1

# Modify listener
aws elbv2 modify-listener --listener-arn LISTENER_ARN --default-actions Type=forward,TargetGroupArn=TARGET_GROUP_ARN --region ap-south-1
```

### RDS Management

```bash
# Describe RDS instances
aws rds describe-db-instances --region ap-south-1

# Create snapshot
aws rds create-db-snapshot --db-instance-identifier DB_INSTANCE_ID --db-snapshot-identifier snapshot-$(date +%Y%m%d-%H%M%S) --region ap-south-1

# List snapshots
aws rds describe-db-snapshots --db-instance-identifier DB_INSTANCE_ID --region ap-south-1

# Restore from snapshot
aws rds restore-db-instance-from-db-snapshot --db-instance-identifier NEW_INSTANCE_ID --db-snapshot-identifier SNAPSHOT_ID --region ap-south-1

# Modify instance (resize, etc.)
aws rds modify-db-instance --db-instance-identifier DB_INSTANCE_ID --db-instance-class db.t3.medium --apply-immediately --region ap-south-1
```

### S3 Management

```bash
# List buckets
aws s3 ls

# List objects in bucket
aws s3 ls s3://BUCKET_NAME/ --recursive

# Sync local directory to S3
aws s3 sync /local/path s3://BUCKET_NAME/path/

# Copy file to S3
aws s3 cp file.txt s3://BUCKET_NAME/path/

# Download from S3
aws s3 cp s3://BUCKET_NAME/path/file.txt ./

# Delete object
aws s3 rm s3://BUCKET_NAME/path/file.txt

# Set lifecycle policy (auto-delete old files)
aws s3api put-bucket-lifecycle-configuration --bucket BUCKET_NAME --lifecycle-configuration file://lifecycle.json
```

### Cognito Management

```bash
# List user pools
aws cognito-idp list-user-pools --max-results 10 --region ap-south-1

# Describe user pool
aws cognito-idp describe-user-pool --user-pool-id POOL_ID --region ap-south-1

# Create user
aws cognito-idp admin-create-user --user-pool-id POOL_ID --username user@example.com --user-attributes Name=email,Value=user@example.com --region ap-south-1

# Set user password
aws cognito-idp admin-set-user-password --user-pool-id POOL_ID --username user@example.com --password "Password123!" --permanent --region ap-south-1

# List users
aws cognito-idp list-users --user-pool-id POOL_ID --region ap-south-1

# Delete user
aws cognito-idp admin-delete-user --user-pool-id POOL_ID --username user@example.com --region ap-south-1

# Disable user
aws cognito-idp admin-disable-user --user-pool-id POOL_ID --username user@example.com --region ap-south-1

# Enable user
aws cognito-idp admin-enable-user --user-pool-id POOL_ID --username user@example.com --region ap-south-1
```

### Docker Commands

```bash
# Build API image
docker build -t company-chatbot-prod-api -f dockerfile .

# Build UI image
docker build -t company-chatbot-prod-ui -f dockerfile.streamlit .

# Run locally
docker-compose up

# Tag image for ECR
docker tag company-chatbot-prod-api:latest <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-api:latest

# Push to ECR
docker push <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-api:latest

# Pull from ECR
docker pull <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-api:latest

# Inspect image
docker inspect <AWS_ACCOUNT_ID>.dkr.ecr.ap-south-1.amazonaws.com/company-chatbot-prod-api:latest

# Remove old images
docker image prune -a
```

### Terraform Commands

```bash
# Navigate to terraform directory
cd terraform/

# Initialize
terraform init

# Validate configuration
terraform validate

# Format code
terraform fmt

# Preview changes
terraform plan

# Apply changes
terraform apply

# Apply with auto-approve
terraform apply -auto-approve

# Destroy specific resource
terraform destroy -target=aws_ecs_service.sync

# Destroy all
terraform destroy

# Show state
terraform show

# List resources
terraform state list

# Show specific resource
terraform state show aws_ecs_service.api

# Import existing resource
terraform import aws_ecs_service.api arn:aws:ecs:ap-south-1:ACCOUNT:service/CLUSTER/SERVICE

# Refresh state
terraform refresh

# Output values
terraform output
```

### Health Checks

```bash
# Check API health
curl https://chatbot.tonal-ops.com/api/health

# Check frontend
curl -I https://chatbot.tonal-ops.com

# Test API endpoint
curl -X POST https://chatbot.tonal-ops.com/api/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "test", "history": []}'

# Check DNS resolution
nslookup chatbot.tonal-ops.com
dig chatbot.tonal-ops.com

# Check SSL certificate
echo | openssl s_client -connect chatbot.tonal-ops.com:443 -servername chatbot.tonal-ops.com 2>/dev/null | openssl x509 -noout -dates

# Test ALB endpoint
curl -I http://ALB_DNS_NAME
```
