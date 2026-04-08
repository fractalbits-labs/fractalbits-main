terraform {
  required_version = ">= 1.5"

  required_providers {
    alicloud = {
      source  = "aliyun/alicloud"
      version = "~> 1.220"
    }
  }

  backend "oss" {
    # Configure via: terraform init -backend-config="bucket=<state-bucket>" -backend-config="prefix=vpc" -backend-config="region=<region>"
  }
}

provider "alicloud" {
  region = var.region
}

locals {
  rss_a_name = "rss-a"
  rss_b_name = "rss-b"
  nss_a_name = "nss-a"
  nss_b_name = "nss-b"

  rss_ha_enabled = var.root_server_ha
  deploy_bucket  = "fractalbits-deploy-${var.region}"
}
