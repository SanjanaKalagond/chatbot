output "alb_dns_name" {
  description = "ALB DNS name for UI/API access."
  value       = aws_lb.main.dns_name
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

output "ecs_cluster_name" {
  description = "ECS cluster name."
  value       = aws_ecs_cluster.main.name
}

output "route53_record_fqdn" {
  description = "Created Route53 record FQDN (if enabled)."
  value       = try(aws_route53_record.app[0].fqdn, null)
}
