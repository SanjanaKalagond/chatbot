locals {
  name_prefix = "${var.project_name}-${var.environment}"
  alb_prefix  = "${local.name_prefix}-${var.alb_resource_suffix}"
  azs         = slice(data.aws_availability_zones.available.names, 0, length(var.public_subnet_cidrs))
  alb_https_enabled = var.enable_alb && var.alb_certificate_arn != ""
  ui_api_url_effective = var.enable_alb ? "http://${aws_lb.main[0].dns_name}" : var.ui_api_url
  cognito_domain_base = var.cognito_custom_domain != "" ? "https://${var.cognito_custom_domain}" : (
    var.cognito_prefix_domain != "" ? "https://${var.cognito_prefix_domain}.auth.${var.aws_region}.amazoncognito.com" : ""
  )
  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.common_tags
  )
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = merge(local.tags, { Name = "${local.name_prefix}-vpc" })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.tags, { Name = "${local.name_prefix}-igw" })
}

resource "aws_subnet" "public" {
  count                   = length(var.public_subnet_cidrs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true
  tags = merge(local.tags, {
    Name = "${local.name_prefix}-public-${count.index + 1}"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.tags, { Name = "${local.name_prefix}-public-rt" })
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route_table_association" "public" {
  for_each = {
    for idx, cidr in var.public_subnet_cidrs :
    idx => cidr
  }

  subnet_id      = aws_subnet.public[tonumber(each.key)].id
  route_table_id = aws_route_table.public.id
}

# ALB Security Group - COMMENTED OUT (no ALB in this deployment)
# resource "aws_security_group" "alb" {
#   name        = "${local.name_prefix}-alb-sg"
#   description = "Allow inbound web traffic to ALB"
#   vpc_id      = aws_vpc.main.id
#
#   ingress {
#     description = "HTTP from internet"
#     from_port   = 80
#     to_port     = 80
#     protocol    = "tcp"
#     cidr_blocks = ["0.0.0.0/0"]
#   }
#
#   egress {
#     from_port   = 0
#     to_port     = 0
#     protocol    = "-1"
#     cidr_blocks = ["0.0.0.0/0"]
#   }
#
#   tags = merge(local.tags, {
#     Name = "${local.name_prefix}-alb-sg"
#   })
# }

resource "aws_security_group" "ecs_service" {
  name        = "${local.name_prefix}-ecs-sg"
  description = "Allow traffic from ALB to ECS services"
  vpc_id      = aws_vpc.main.id

  dynamic "ingress" {
    for_each = var.enable_alb ? [] : [var.api_container_port, var.ui_container_port]
    content {
      description = "Public ingress while ALB is disabled"
      from_port   = ingress.value
      to_port     = ingress.value
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

resource "aws_security_group" "alb" {
  count       = var.enable_alb ? 1 : 0
  name        = substr(replace("${local.alb_prefix}-alb-sg", "/[^a-zA-Z0-9-]/", ""), 0, 255)
  description = "Allow inbound web traffic to ALB"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTP from internet"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  dynamic "ingress" {
    for_each = local.alb_https_enabled ? [1] : []
    content {
      description = "HTTPS from internet"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, {
    Name = "${local.alb_prefix}-alb-sg"
  })
}

resource "aws_vpc_security_group_ingress_rule" "ecs_api_from_alb" {
  count                        = var.enable_alb ? 1 : 0
  security_group_id            = aws_security_group.ecs_service.id
  referenced_security_group_id = aws_security_group.alb[0].id
  from_port                    = var.api_container_port
  ip_protocol                  = "tcp"
  to_port                      = var.api_container_port
}

resource "aws_vpc_security_group_ingress_rule" "ecs_ui_from_alb" {
  count                        = var.enable_alb ? 1 : 0
  security_group_id            = aws_security_group.ecs_service.id
  referenced_security_group_id = aws_security_group.alb[0].id
  from_port                    = var.ui_container_port
  ip_protocol                  = "tcp"
  to_port                      = var.ui_container_port
}

resource "aws_cloudwatch_log_group" "api" {
  name = "/ecs/company-chatbot-prod/api"
  retention_in_days = 30
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "ui" {
  name = "/ecs/company-chatbot-prod/ui"
  retention_in_days = 30
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "sync" {
  name = "/ecs/company-chatbot-prod/sync"
  retention_in_days = 30
  tags              = local.tags
}

resource "aws_ecr_repository" "api" {
  name                 = "${local.name_prefix}-api"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  tags                 = local.tags
}

resource "aws_ecr_repository" "ui" {
  name                 = "${local.name_prefix}-ui"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  tags                 = local.tags
}

resource "aws_ecr_repository" "sync" {
  name                 = "${local.name_prefix}-sync"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  tags                 = local.tags
}

data "aws_iam_policy_document" "ecs_task_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution_role" {
  name               = "${local.name_prefix}-ecs-exec-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "ecs_execution_role_policy" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task_role" {
  name               = "${local.name_prefix}-ecs-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "ecs_task_s3_access" {
  name = "${local.name_prefix}-ecs-task-s3-policy"
  role = aws_iam_role.ecs_task_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*",
          "arn:aws:s3:::${var.faiss_bucket_name}",
          "arn:aws:s3:::${var.faiss_bucket_name}/*"
        ]
      }
    ]
  })
}

resource "aws_ecs_cluster" "main" {
  name = "${local.name_prefix}-cluster"
  tags = local.tags
}

resource "aws_lb" "main" {
  count              = var.enable_alb ? 1 : 0
  name               = substr(replace("${local.alb_prefix}-alb", "/[^a-zA-Z0-9-]/", ""), 0, 32)
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb[0].id]
  subnets            = aws_subnet.public[*].id
  tags               = local.tags
}

resource "aws_lb_target_group" "api" {
  count       = var.enable_alb ? 1 : 0
  name        = substr(replace("${local.alb_prefix}-api-tg", "/[^a-zA-Z0-9-]/", ""), 0, 32)
  port        = var.api_container_port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  health_check {
    path                = var.health_check_path
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = local.tags
}

resource "aws_lb_target_group" "ui" {
  count       = var.enable_alb ? 1 : 0
  name        = substr(replace("${local.alb_prefix}-ui-tg", "/[^a-zA-Z0-9-]/", ""), 0, 32)
  port        = var.ui_container_port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  health_check {
    path                = "/"
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = local.tags
}

resource "aws_lb_listener" "http" {
  count             = var.enable_alb ? 1 : 0
  load_balancer_arn = aws_lb.main[0].arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = local.alb_https_enabled && var.redirect_http_to_https ? "redirect" : "forward"

    dynamic "redirect" {
      for_each = local.alb_https_enabled && var.redirect_http_to_https ? [1] : []
      content {
        port        = "443"
        protocol    = "HTTPS"
        status_code = "HTTP_301"
      }
    }

    target_group_arn = local.alb_https_enabled && var.redirect_http_to_https ? null : aws_lb_target_group.ui[0].arn
  }
}

resource "aws_lb_listener" "https" {
  count             = local.alb_https_enabled ? 1 : 0
  load_balancer_arn = aws_lb.main[0].arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = var.alb_certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ui[0].arn
  }
}

resource "aws_lb_listener_rule" "api_path" {
  count        = var.enable_alb ? 1 : 0
  listener_arn = local.alb_https_enabled ? aws_lb_listener.https[0].arn : aws_lb_listener.http[0].arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api[0].arn
  }

  condition {
    path_pattern {
      values = [
        "/api*",
        "/docs*",
        "/openapi.json*",
        "/chat*",
        "/upload*"
      ]
    }
  }
}

resource "aws_lb_listener_rule" "api_path_extra" {
  count        = var.enable_alb ? 1 : 0
  listener_arn = local.alb_https_enabled ? aws_lb_listener.https[0].arn : aws_lb_listener.http[0].arn
  priority     = 11

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api[0].arn
  }

  condition {
    path_pattern {
      values = [
        "/save_interaction*",
        "/clear_session_docs*",
        "/health*"
      ]
    }
  }
}

resource "aws_ecs_task_definition" "api" {
  family                   = "${local.name_prefix}-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.api_cpu
  memory                   = var.api_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "api"
      image     = var.api_image
      essential = true
      portMappings = [{
        containerPort = var.api_container_port
        hostPort      = var.api_container_port
        protocol      = "tcp"
      }]
      environment = [
        { name = "SALESFORCE_CLIENT_ID", value = var.salesforce_client_id },
        { name = "SALESFORCE_CLIENT_SECRET", value = var.salesforce_client_secret },
        { name = "SALESFORCE_USERNAME", value = var.salesforce_username },
        { name = "SALESFORCE_PASSWORD", value = var.salesforce_password },
        { name = "SALESFORCE_ACCESS_TOKEN", value = var.salesforce_access_token },
        { name = "AWS_ACCESS_KEY_ID", value = var.aws_access_key_id },
        { name = "AWS_SECRET_ACCESS_KEY", value = var.aws_secret_access_key },
        { name = "AWS_REGION", value = var.aws_region },
        { name = "AWS_DEFAULT_REGION", value = var.aws_region },
        { name = "S3_BUCKET_NAME", value = var.s3_bucket_name },
        { name = "FAISS_BUCKET_NAME", value = var.faiss_bucket_name },
        { name = "GEMINI_API_KEY", value = var.gemini_api_key },
        { name = "DATABASE_URL", value = var.database_url },
        { name = "COGNITO_USER_POOL_ID", value = var.cognito_user_pool_id },
        { name = "COGNITO_PREFIX_DOMAIN", value = var.cognito_prefix_domain },
        { name = "COGNITO_CUSTOM_DOMAIN", value = var.cognito_custom_domain },
        { name = "COGNITO_DOMAIN_BASE", value = local.cognito_domain_base },
        { name = "PYTHONPATH", value = "/app" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.api.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  tags = local.tags
}

resource "aws_ecs_task_definition" "ui" {
  family                   = "${local.name_prefix}-ui"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.ui_cpu
  memory                   = var.ui_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "ui"
      image     = var.ui_image
      essential = true
      portMappings = [{
        containerPort = var.ui_container_port
        hostPort      = var.ui_container_port
        protocol      = "tcp"
      }]
      environment = [
        { name = "API_URL", value = local.ui_api_url_effective }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ui.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  tags = local.tags
}

resource "aws_ecs_task_definition" "sync" {
  family                   = "${local.name_prefix}-sync"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.sync_cpu
  memory                   = var.sync_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "sync"
      image     = var.sync_image
      essential = true
      command   = ["python", "app/ingestion/incremental_sync.py"]
      environment = [
        { name = "SALESFORCE_CLIENT_ID", value = var.salesforce_client_id },
        { name = "SALESFORCE_CLIENT_SECRET", value = var.salesforce_client_secret },
        { name = "SALESFORCE_USERNAME", value = var.salesforce_username },
        { name = "SALESFORCE_PASSWORD", value = var.salesforce_password },
        { name = "SALESFORCE_ACCESS_TOKEN", value = var.salesforce_access_token },
        { name = "AWS_ACCESS_KEY_ID", value = var.aws_access_key_id },
        { name = "AWS_SECRET_ACCESS_KEY", value = var.aws_secret_access_key },
        { name = "AWS_REGION", value = var.aws_region },
        { name = "AWS_DEFAULT_REGION", value = var.aws_region },
        { name = "S3_BUCKET_NAME", value = var.s3_bucket_name },
        { name = "FAISS_BUCKET_NAME", value = var.faiss_bucket_name },
        { name = "GEMINI_API_KEY", value = var.gemini_api_key },
        { name = "DATABASE_URL", value = var.database_url },
        { name = "COGNITO_USER_POOL_ID", value = var.cognito_user_pool_id },
        { name = "COGNITO_PREFIX_DOMAIN", value = var.cognito_prefix_domain },
        { name = "COGNITO_CUSTOM_DOMAIN", value = var.cognito_custom_domain },
        { name = "COGNITO_DOMAIN_BASE", value = local.cognito_domain_base },
        { name = "PYTHONPATH", value = "/app" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.sync.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  tags = local.tags
}

resource "aws_ecs_service" "api" {
  name            = "${local.name_prefix}-api-svc"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_service.id]
    assign_public_ip = var.ecs_assign_public_ip
  }

  dynamic "load_balancer" {
    for_each = var.enable_alb ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.api[0].arn
      container_name   = "api"
      container_port   = var.api_container_port
    }
  }

  depends_on = [aws_lb_listener.http]
  tags = local.tags
}

resource "aws_ecs_service" "ui" {
  name            = "${local.name_prefix}-ui-svc"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.ui.arn
  desired_count   = var.ui_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_service.id]
    assign_public_ip = var.ecs_assign_public_ip
  }

  dynamic "load_balancer" {
    for_each = var.enable_alb ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.ui[0].arn
      container_name   = "ui"
      container_port   = var.ui_container_port
    }
  }

  depends_on = [aws_lb_listener.http]
  tags = local.tags
}

resource "aws_iam_role" "events_invoke_ecs" {
  count              = var.sync_enabled ? 1 : 0
  name               = "${local.name_prefix}-events-ecs-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "events_invoke_ecs" {
  count = var.sync_enabled ? 1 : 0
  name  = "${local.name_prefix}-events-ecs-policy"
  role  = aws_iam_role.events_invoke_ecs[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        Resource = [
          aws_ecs_task_definition.sync.arn
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.ecs_execution_role.arn, aws_iam_role.ecs_task_role.arn]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "sync_schedule" {
  count               = var.sync_enabled ? 1 : 0
  name                = "${local.name_prefix}-sync-schedule"
  schedule_expression = var.sync_schedule_expression
  tags                = local.tags
}

resource "aws_cloudwatch_event_target" "sync_schedule" {
  count     = var.sync_enabled ? 1 : 0
  rule      = aws_cloudwatch_event_rule.sync_schedule[0].name
  target_id = "ecs-sync-task"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.events_invoke_ecs[0].arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.sync.arn
    network_configuration {
      subnets          = aws_subnet.public[*].id
      security_groups  = [aws_security_group.ecs_service.id]
      assign_public_ip = true
    }
  }
}

resource "aws_route53_record" "app" {
  count   = var.enable_alb && var.domain_name != "" && var.hosted_zone_id != "" ? 1 : 0
  zone_id = var.hosted_zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_lb.main[0].dns_name
    zone_id                = aws_lb.main[0].zone_id
    evaluate_target_health = true
  }
}

resource "aws_cognito_user_pool_domain" "prefix" {
  count        = var.cognito_user_pool_id != "" && var.cognito_prefix_domain != "" ? 1 : 0
  domain       = var.cognito_prefix_domain
  user_pool_id = var.cognito_user_pool_id
}

resource "aws_cognito_user_pool_domain" "custom" {
  count           = var.cognito_user_pool_id != "" && var.cognito_custom_domain != "" && var.cognito_certificate_arn != "" ? 1 : 0
  domain          = var.cognito_custom_domain
  user_pool_id    = var.cognito_user_pool_id
  certificate_arn = var.cognito_certificate_arn
}

resource "aws_route53_record" "cognito_custom_domain_a" {
  count   = var.cognito_user_pool_id != "" && var.cognito_custom_domain != "" && var.cognito_certificate_arn != "" && var.cognito_custom_domain_zone_id != "" ? 1 : 0
  zone_id = var.cognito_custom_domain_zone_id
  name    = var.cognito_custom_domain
  type    = "A"
  allow_overwrite = true

  alias {
    name                   = aws_cognito_user_pool_domain.custom[0].cloudfront_distribution
    zone_id                = "Z2FDTNDATAQYW2"
    evaluate_target_health = false
  }
}

resource "aws_route53_record" "cognito_custom_domain_aaaa" {
  count   = var.cognito_user_pool_id != "" && var.cognito_custom_domain != "" && var.cognito_certificate_arn != "" && var.cognito_custom_domain_zone_id != "" ? 1 : 0
  zone_id = var.cognito_custom_domain_zone_id
  name    = var.cognito_custom_domain
  type    = "AAAA"
  allow_overwrite = true

  alias {
    name                   = aws_cognito_user_pool_domain.custom[0].cloudfront_distribution
    zone_id                = "Z2FDTNDATAQYW2"
    evaluate_target_health = false
  }
}