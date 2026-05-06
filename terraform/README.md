# Terraform Foundation for Chatbot

This Terraform stack creates a practical foundation to run your chatbot on AWS:

- VPC + public subnets + internet gateway
- ECS Fargate cluster
- ALB with path-based routing (`/api*`, `/docs*` to API; default to UI)
- ECR repositories for `api`, `ui`, and `sync`
- Scheduled sync task via EventBridge
- Optional Route53 alias record

## Prerequisites

- Terraform `>= 1.5`
- AWS CLI configured for the target account
- Permission to create VPC, ECS, ECR, ALB, IAM, Route53, CloudWatch resources

## 1) Initialize variables

From `terraform/`:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set:

- `api_image`, `ui_image`, `sync_image` (image URIs)
- optional `domain_name`, `hosted_zone_id` for Route53

## 2) Deploy base infrastructure

```bash
terraform init
terraform plan
terraform apply
```

If images are not available yet in ECR, temporarily use public placeholder images, deploy infra, then update image URIs and re-apply.

## 3) Build and push images

After first apply, get ECR repo URLs:

```bash
terraform output api_ecr_repository_url
terraform output ui_ecr_repository_url
terraform output sync_ecr_repository_url
```

Then build and push your three images (from repository root):

```bash
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin <account>.dkr.ecr.ap-south-1.amazonaws.com
docker build -t <api_repo_url>:latest -f dockerfile .
docker push <api_repo_url>:latest

docker build -t <ui_repo_url>:latest -f dockerfile.streamlit .
docker push <ui_repo_url>:latest

docker build -t <sync_repo_url>:latest -f dockerfile .
docker push <sync_repo_url>:latest
```

Update `terraform.tfvars` image values if needed, then:

```bash
terraform apply
```

## 4) Access

- UI and API are available on ALB DNS from:

```bash
terraform output alb_dns_name
```

- API routes use ALB path forwarding:
  - `http://<alb_dns>/api...`
  - `http://<alb_dns>/docs`

## Notes

- This is intentionally a strong starter foundation for migration speed.
- For production hardening, add:
  - HTTPS (`aws_acm_certificate` + `443` listener)
  - private subnets + NAT
  - autoscaling policies
  - Secrets Manager integration for env vars
