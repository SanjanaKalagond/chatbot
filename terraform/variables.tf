variable "project_name" {
  description = "Project prefix used for naming resources."
  type        = string
  default     = "tonal-chatbot"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for deployment."
  type        = string
  default     = "ap-south-1"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC."
  type        = string
  default     = "10.30.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (at least 2)."
  type        = list(string)
  default     = ["10.30.1.0/24", "10.30.2.0/24"]
}

variable "api_container_port" {
  description = "Port used by FastAPI container."
  type        = number
  default     = 8000
}

variable "ui_container_port" {
  description = "Port used by Streamlit container."
  type        = number
  default     = 8501
}

variable "api_cpu" {
  description = "Fargate CPU units for API task."
  type        = number
  default     = 512
}

variable "api_memory" {
  description = "Fargate memory (MB) for API task."
  type        = number
  default     = 1024
}

variable "ui_cpu" {
  description = "Fargate CPU units for UI task."
  type        = number
  default     = 512
}

variable "ui_memory" {
  description = "Fargate memory (MB) for UI task."
  type        = number
  default     = 1024
}

variable "sync_cpu" {
  description = "Fargate CPU units for sync task."
  type        = number
  default     = 512
}

variable "sync_memory" {
  description = "Fargate memory (MB) for sync task."
  type        = number
  default     = 1024
}

variable "api_desired_count" {
  description = "Desired number of API tasks."
  type        = number
  default     = 1
}

variable "ui_desired_count" {
  description = "Desired number of UI tasks."
  type        = number
  default     = 1
}

variable "api_image" {
  description = "Container image URI for API service."
  type        = string
}

variable "ui_image" {
  description = "Container image URI for UI service."
  type        = string
}

variable "sync_image" {
  description = "Container image URI for sync task."
  type        = string
}

variable "sync_schedule_expression" {
  description = "EventBridge schedule for sync task."
  type        = string
  default     = "rate(30 minutes)"
}

variable "sync_enabled" {
  description = "Enable scheduled sync task."
  type        = bool
  default     = true
}

variable "health_check_path" {
  description = "Health check path for API target group."
  type        = string
  default     = "/docs"
}

variable "domain_name" {
  description = "Root DNS name (example: chatbot.tonal.com). Leave empty to skip Route53 records."
  type        = string
  default     = ""
}

variable "hosted_zone_id" {
  description = "Route53 hosted zone ID where domain_name record should be created. Leave empty to skip."
  type        = string
  default     = ""
}

variable "common_tags" {
  description = "Additional tags for all resources."
  type        = map(string)
  default     = {}
}
