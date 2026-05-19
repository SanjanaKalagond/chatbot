variable "project_name" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
}

variable "public_subnet_cidrs" {
  description = "Public subnet CIDRs"
  type        = list(string)
}

variable "api_image" {
  description = "API container image"
  type        = string
}

variable "ui_image" {
  description = "UI container image"
  type        = string
}

variable "ui_api_url" {
  description = "Base URL that the UI service uses to call the API"
  type        = string
  default     = "http://localhost:8000"
}

variable "enable_alb" {
  description = "Enable ALB, listeners, target groups, and ECS service LB attachment"
  type        = bool
  default     = false
}

variable "alb_resource_suffix" {
  description = "Suffix used for ALB resource names to avoid clashes with old ALB stack"
  type        = string
  default     = "v2"
}

variable "ecs_assign_public_ip" {
  description = "Assign public IP to ECS service tasks (set false only with private subnets + NAT)"
  type        = bool
  default     = true
}

variable "sync_image" {
  description = "Sync container image"
  type        = string
}

variable "api_desired_count" {
  description = "API ECS desired task count"
  type        = number
  default     = 1
}

variable "ui_desired_count" {
  description = "UI ECS desired task count"
  type        = number
  default     = 1
}

variable "sync_enabled" {
  description = "Enable scheduled sync task"
  type        = bool
  default     = true
}

variable "sync_schedule_expression" {
  description = "EventBridge schedule for sync"
  type        = string
}

variable "domain_name" {
  description = "Optional custom domain"
  type        = string
  default     = ""
}

variable "hosted_zone_id" {
  description = "Optional Route53 hosted zone ID"
  type        = string
  default     = ""
}

variable "alb_certificate_arn" {
  description = "ACM certificate ARN in the ALB region for HTTPS listener"
  type        = string
  default     = ""
}

variable "redirect_http_to_https" {
  description = "Redirect HTTP traffic on ALB to HTTPS when certificate is configured"
  type        = bool
  default     = true
}

variable "common_tags" {
  description = "Common tags applied to resources"
  type        = map(string)
  default     = {}
}

variable "aws_access_key_id" {
  description = "AWS Access Key ID for application"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS Secret Access Key for application"
  type        = string
  sensitive   = true
}

variable "database_url" {
  description = "PostgreSQL database connection URL"
  type        = string
  sensitive   = true
}

variable "s3_bucket_name" {
  description = "S3 bucket name for documents"
  type        = string
}

variable "faiss_bucket_name" {
  description = "S3 bucket name for FAISS index"
  type        = string
}

variable "gemini_api_key" {
  description = "Google Gemini API key"
  type        = string
  sensitive   = true
}

variable "salesforce_client_id" {
  description = "Salesforce OAuth client ID"
  type        = string
  sensitive   = true
}

variable "salesforce_client_secret" {
  description = "Salesforce OAuth client secret"
  type        = string
  sensitive   = true
}

variable "salesforce_username" {
  description = "Salesforce username"
  type        = string
  sensitive   = true
}

variable "salesforce_password" {
  description = "Salesforce password"
  type        = string
  sensitive   = true
}

variable "salesforce_access_token" {
  description = "Salesforce access token"
  type        = string
  sensitive   = true
}

variable "cognito_user_pool_id" {
  description = "Cognito User Pool ID used for managed login domains"
  type        = string
  default     = ""
}

variable "cognito_prefix_domain" {
  description = "Cognito service-owned domain prefix (for example: my-prefix)"
  type        = string
  default     = ""
}

variable "cognito_custom_domain" {
  description = "Cognito custom domain (for example: auth.example.com)"
  type        = string
  default     = ""
}

variable "cognito_certificate_arn" {
  description = "ACM certificate ARN in us-east-1 for Cognito custom domain"
  type        = string
  default     = ""
}

variable "cognito_custom_domain_zone_id" {
  description = "Route53 hosted zone ID for the Cognito custom domain"
  type        = string
  default     = ""
}
variable "api_container_port" {
  description = "API container port"
  type        = number
  default     = 8000
}

variable "ui_container_port" {
  description = "UI container port"
  type        = number
  default     = 8501
}

variable "health_check_path" {
  description = "Health check endpoint path"
  type        = string
  default     = "/health"
}

variable "api_cpu" {
  description = "API task CPU"
  type        = number
  default     = 1024
}

variable "api_memory" {
  description = "API task memory"
  type        = number
  default     = 2048
}

variable "ui_cpu" {
  description = "UI task CPU"
  type        = number
  default     = 512
}

variable "ui_memory" {
  description = "UI task memory"
  type        = number
  default     = 1024
}

variable "sync_cpu" {
  description = "Sync task CPU"
  type        = number
  default     = 1024
}

variable "sync_memory" {
  description = "Sync task memory"
  type        = number
  default     = 2048
}