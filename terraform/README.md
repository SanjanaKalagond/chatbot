# Terraform Deployment

Deploy the Salesforce chatbot infrastructure on AWS.

## Prerequisites

- AWS CLI configured with company credentials
- Docker images built and pushed to ECR
- `.env` file configured with company resources

## Deploy

```bash
cd terraform

terraform init
terraform validate
terraform plan
terraform apply
```

## Outputs

```bash
terraform output
```

## Access

**Frontend**: `http://<alb_dns_name>/`  
**Backend**: `http://<alb_dns_name>/docs`

Example:
- Frontend: http://tonal-chatbot-dev-alb-1490346913.ap-south-1.elb.amazonaws.com/
- Backend: http://tonal-chatbot-dev-alb-1490346913.ap-south-1.elb.amazonaws.com/docs

## Update

After code changes, rebuild images, push to ECR, then:

```bash
terraform apply
```