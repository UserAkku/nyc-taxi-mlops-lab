# S1: the warehouse. One ra3.large Redshift node (infra/modules/redshift), in the same AZ as the hosts.
#
# No ingress rule anywhere: Dagster talks to Redshift through the Data API (an AWS API call made with
# the platform host's instance profile), never to the cluster's port. LabRole is attached to the
# cluster, so COPY can say `iam_role default`.
#
# Outputs below are the names contracts/platform.yaml lists under terraform_outputs.

module "warehouse" {
  source            = "../modules/redshift"
  subnet_ids        = data.aws_subnets.default.ids
  availability_zone = var.availability_zone
}

output "redshift_cluster_id" {
  value = module.warehouse.cluster_id
}

output "redshift_database" {
  value = module.warehouse.database
}
