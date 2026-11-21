# S2: the SageMaker notebook (infra/modules/notebook), in the hosts' subnet so it reaches MLflow on the
# platform host (port 5000, runtime.yaml). It clones your public repo; github_repo comes from
# terraform.tfvars. It costs about $0.05/hour while running and a session refresh does not restart it:
# stop it at night (aws sagemaker stop-notebook-instance --notebook-instance-name lab).

resource "terraform_data" "require_github_repo" {
  lifecycle {
    precondition {
      condition     = var.github_repo != null
      error_message = "Set github_repo = \"<owner>/<repo>\" in infra/live/terraform.tfvars: the notebook clones that public repo."
    }
  }
}

module "notebook" {
  source    = "../modules/notebook"
  subnet_id = data.aws_subnet.hosts.id
  repo_url  = "https://github.com/${coalesce(var.github_repo, "unset/unset")}"

  # Written to ~/SageMaker/.lab.env, which the notebook's first cell reads.
  env = {
    MLFLOW_TRACKING_URI = "http://${module.platform.private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.warehouse.cluster_id
    REDSHIFT_DATABASE   = module.warehouse.database
    REDSHIFT_DB_USER    = "labadmin"
    LAB_ARTIFACT_BUCKET = module.artifacts.name
  }

  depends_on = [terraform_data.require_github_repo]
}

# The notebook is a client of MLflow, like the app host's serving (S3): by security group, port 5000 only.
resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_notebook" {
  security_group_id            = aws_security_group.platform.id
  description                  = "MLflow from the notebook"
  referenced_security_group_id = module.notebook.security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
}

output "notebook_name" {
  value = module.notebook.name
}
