terraform {
  required_version = ">= 1.5"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 6.0"
    }
  }

  # OCI native backend (Terraform >= 1.12). Configure via:
  #   terraform init -backend-config="bucket=..." -backend-config="namespace=..." -backend-config="region=..."
  backend "oci" {}
}

provider "oci" {
  tenancy_ocid = var.tenancy_ocid
  region       = var.region
}

locals {
  rss_a_name = "rss-a"
  rss_b_name = "rss-b"
  nss_a_name = "nss-a"
  nss_b_name = "nss-b"

  rss_ha_enabled = var.vpc_template == "standard" || var.root_server_ha

  # Common tags for all resources
  freeform_tags = {
    "project"    = "fractalbits"
    "cluster_id" = var.cluster_id
  }
}
