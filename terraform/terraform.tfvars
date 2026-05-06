project_name = "tonal-chatbot"
environment  = "dev"
aws_region   = "ap-south-1"

vpc_cidr            = "10.30.0.0/16"
public_subnet_cidrs = ["10.30.1.0/24", "10.30.2.0/24"]

api_image  = "544433947689.dkr.ecr.ap-south-1.amazonaws.com/tonal-chatbot-dev-api:latest"

ui_image   = "544433947689.dkr.ecr.ap-south-1.amazonaws.com/tonal-chatbot-dev-ui:latest"

sync_image = "544433947689.dkr.ecr.ap-south-1.amazonaws.com/tonal-chatbot-dev-sync:latest"

api_desired_count = 1
ui_desired_count  = 1

sync_enabled             = true
sync_schedule_expression = "rate(30 minutes)"

# Optional Route53 alias. Leave empty strings to skip DNS record creation.
domain_name    = ""
hosted_zone_id = ""

common_tags = {
  Owner = "tonal"
}
