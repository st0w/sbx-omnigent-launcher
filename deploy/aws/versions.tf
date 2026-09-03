terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.50"
    }
  }

  # Local state holds the VPC layout and instance ids. Nothing secret is
  # written here BY THIS MODULE (no passwords, no tokens — the publish
  # token stays in Secrets Manager and is only referenced by ARN), but a
  # remote backend with encryption + locking is still the right home for
  # anything shared. Uncomment and fill in:
  #
  # backend "s3" {
  #   bucket       = "my-tfstate"
  #   key          = "omnigent/aws/terraform.tfstate"
  #   region       = "us-east-1"
  #   encrypt      = true
  #   use_lockfile = true
  # }
}

# Credentials come from the environment (SSO, instance profile, or a named
# profile) — never from this file. See the AWS provider docs for the
# resolution order.
provider "aws" {
  region = var.region

  default_tags {
    tags = merge(
      {
        Project   = var.name
        ManagedBy = "terraform"
        Module    = "sbx-omnigent-launcher/deploy/aws"
      },
      var.tags,
    )
  }
}
