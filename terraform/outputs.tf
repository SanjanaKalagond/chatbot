output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.main.id
}

output "ecs_cluster_name" {
  description = "The name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "ecs_cluster_arn" {
  description = "The ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

output "alb_dns_name" {
  description = "ALB DNS name for UI/API access."
  value       = try(aws_lb.main[0].dns_name, null)
}

output "app_base_url" {
  description = "Primary app base URL (custom domain if set, otherwise ALB DNS)."
  value = var.enable_alb ? (
    var.domain_name != "" ? (
      var.alb_certificate_arn != "" ? "https://${var.domain_name}" : "http://${var.domain_name}"
    ) : (
      var.alb_certificate_arn != "" ? "https://${aws_lb.main[0].dns_name}" : "http://${aws_lb.main[0].dns_name}"
    )
  ) : null
}

output "api_service_name" {
  description = "The name of the API ECS service"
  value       = aws_ecs_service.api.name
}

output "ui_service_name" {
  description = "The name of the UI ECS service"
  value       = aws_ecs_service.ui.name
}

output "api_ecr_repository_url" {
  description = "ECR repository URL for API image."
  value       = aws_ecr_repository.api.repository_url
}

output "ui_ecr_repository_url" {
  description = "ECR repository URL for UI image."
  value       = aws_ecr_repository.ui.repository_url
}

output "sync_ecr_repository_url" {
  description = "ECR repository URL for sync image."
  value       = aws_ecr_repository.sync.repository_url
}

output "route53_record_fqdn" {
  description = "Created Route53 record FQDN (if enabled)."
  value       = try(aws_route53_record.app[0].fqdn, null)
}

output "cognito_prefix_domain_url" {
  description = "Service-owned Cognito managed login domain URL."
  value       = var.cognito_prefix_domain != "" ? "https://${var.cognito_prefix_domain}.auth.${var.aws_region}.amazoncognito.com" : null
}

output "cognito_custom_domain_url" {
  description = "Custom Cognito managed login domain URL."
  value       = var.cognito_custom_domain != "" ? "https://${var.cognito_custom_domain}" : null
}

output "cognito_effective_domain_url" {
  description = "Preferred Cognito domain URL (custom domain first, then prefix domain)."
  value       = var.cognito_custom_domain != "" ? "https://${var.cognito_custom_domain}" : (var.cognito_prefix_domain != "" ? "https://${var.cognito_prefix_domain}.auth.${var.aws_region}.amazoncognito.com" : null)
}

output "cognito_custom_domain_target_alias" {
  description = "CloudFront alias target used by Cognito custom domain."
  value       = try(aws_cognito_user_pool_domain.custom[0].cloudfront_distribution, null)
}

output "cloudwatch_log_group_api" {
  description = "CloudWatch log group for API"
  value       = aws_cloudwatch_log_group.api.name
}

output "cloudwatch_log_group_ui" {
  description = "CloudWatch log group for UI"
  value       = aws_cloudwatch_log_group.ui.name
}

output "cloudwatch_log_group_sync" {
  description = "CloudWatch log group for Sync"
  value       = aws_cloudwatch_log_group.sync.name
}

output "sync_schedule_expression" {
  description = "Schedule expression for sync task"
  value       = var.sync_enabled ? var.sync_schedule_expression : "Sync disabled"
}

output "deployment_notes" {
  description = "Deployment notes"

  value = <<-EOT

  ============================================================================
  ECS DEPLOYMENT ACTIVE
  ============================================================================

  ALB enabled: ${var.enable_alb}

  To inspect ECS services:

    aws ecs list-services \
      --cluster ${aws_ecs_cluster.main.name} \
      --region ${var.aws_region}

  To inspect ECS tasks:

    aws ecs list-tasks \
      --cluster ${aws_ecs_cluster.main.name} \
      --region ${var.aws_region}

  To inspect logs:

    aws logs tail ${aws_cloudwatch_log_group.api.name} --follow --region ${var.aws_region}

    aws logs tail ${aws_cloudwatch_log_group.ui.name} --follow --region ${var.aws_region}

    aws logs tail ${aws_cloudwatch_log_group.sync.name} --follow --region ${var.aws_region}

  ALB DNS (if enabled):

    ${try(aws_lb.main[0].dns_name, "ALB disabled")}

  ============================================================================
  EOT
}