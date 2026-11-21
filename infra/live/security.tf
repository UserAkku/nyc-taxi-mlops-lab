# S0: security groups and rules.
#
# Decision point: who may reach the app (ingress scope)? That's var.ingress_cidrs; the record is in
# decisions/. Everything else is host-to-host, by security group reference (never by IP), with the
# ports from contracts/runtime.yaml:
#   internet (ingress_cidrs) -> app       80    Traefik
#   app                      -> platform  5000  MLflow
#   app, platform            -> llm       8080  chat, 8081 embeddings
#   app, platform            -> rds       5432  Postgres
# No SSH and no port 22: SSM Session Manager for a shell, SSM port forwarding for UIs (Dagster 3000).

locals {
  # Hosts that are clients of llm and rds.
  clients = {
    app      = aws_security_group.app.id
    platform = aws_security_group.platform.id
  }
  llm_ports = { chat = 8080, embed = 8081 }
  llm_rules = {
    for pair in setproduct(keys(local.clients), keys(local.llm_ports)) :
    "${pair[0]}-${pair[1]}" => { source = local.clients[pair[0]], port = local.llm_ports[pair[1]] }
  }
}

resource "aws_security_group" "app" {
  name        = "lab-app"
  description = "App host: Traefik on 80 from ingress_cidrs"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-app" }
}

resource "aws_security_group" "platform" {
  name        = "lab-platform"
  description = "Platform host: MLflow from the app host"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-platform" }
}

resource "aws_security_group" "llm" {
  name        = "lab-llm"
  description = "LLM host: chat and embeddings from app and platform"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-llm" }
}

resource "aws_security_group" "rds" {
  name        = "lab-rds"
  description = "Postgres from app and platform"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-rds" }
}

# --- Ingress ---

resource "aws_vpc_security_group_ingress_rule" "app_http" {
  for_each          = toset(var.ingress_cidrs)
  security_group_id = aws_security_group.app.id
  description       = "Traefik (HTTP)"
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_app" {
  security_group_id            = aws_security_group.platform.id
  description                  = "MLflow from the app host"
  referenced_security_group_id = aws_security_group.app.id
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
}

resource "aws_vpc_security_group_ingress_rule" "llm" {
  for_each                     = local.llm_rules
  security_group_id            = aws_security_group.llm.id
  description                  = "LLM ${each.key}"
  referenced_security_group_id = each.value.source
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
}

resource "aws_vpc_security_group_ingress_rule" "rds" {
  for_each                     = local.clients
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from ${each.key}"
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

# --- Egress ---
# Hosts need outbound HTTPS for GHCR, GitHub (the runner long-polls it), SSM and AWS APIs, plus the
# host-to-host ports above. RDS gets no egress rule: it only answers connections.

resource "aws_vpc_security_group_egress_rule" "hosts_all" {
  for_each          = { app = aws_security_group.app.id, platform = aws_security_group.platform.id, llm = aws_security_group.llm.id }
  security_group_id = each.value
  description       = "All outbound"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
