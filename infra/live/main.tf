# S0: the root module wires the given modules (../modules) into one lab.
#
# Network: the account's default VPC. Learner Lab has no NAT gateway, so every host sits in a public
# subnet with a public IP (Elastic IP) for outbound traffic (GHCR, GitHub, SSM). Inbound is governed
# entirely by the security groups in security.tf.

data "aws_vpc" "default" {
  default = true
}

# All default subnets: RDS needs a subnet group spanning at least two AZs.
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# The hosts share one AZ (var.availability_zone): no cross-AZ traffic between them.
data "aws_subnet" "hosts" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = var.availability_zone
  default_for_az    = true
}

# --- Hosts (contracts/runtime.yaml: platform, app, llm) ---

module "platform" {
  source             = "../modules/host"
  role               = "platform"
  instance_type      = var.platform_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [aws_security_group.platform.id]
}

module "app" {
  source             = "../modules/host"
  role               = "app"
  instance_type      = var.app_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [aws_security_group.app.id]
}

module "llm" {
  source             = "../modules/host"
  role               = "llm"
  instance_type      = var.llm_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [aws_security_group.llm.id]
}

# --- Data stores ---

module "rds" {
  source             = "../modules/rds"
  instance_class     = var.rds_instance_class
  subnet_ids         = data.aws_subnets.default.ids
  security_group_ids = [aws_security_group.rds.id]
}

module "artifacts" {
  source = "../modules/bucket"
  name   = "lab-artifacts"
}

# Secret container only; its value is put out of band (contracts/runtime.yaml: secrets.wiki).
module "wiki_secret" {
  source      = "../modules/secret"
  name        = "lab/wiki"
  description = "Wiki url and token for the support assistant (S6)."
}
