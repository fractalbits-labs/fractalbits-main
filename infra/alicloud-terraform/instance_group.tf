# --- API Server Auto Scaling Group ---
#
# Uses ESS (Elastic Scaling Service) for API servers so they can be
# dynamically scaled and attached to the NLB server group.
# BSS instances are managed as plain alicloud_instance in compute.tf
# (simpler, like OCI's approach) since they need local NVMe and
# don't scale dynamically.

resource "alicloud_ess_scaling_group" "api_server" {
  scaling_group_name = "fractalbits-api-server-sg"
  vswitch_ids        = [alicloud_vswitch.fractalbits.id]
  min_size           = var.num_api_servers
  max_size           = var.num_api_servers * 3
  desired_capacity   = var.num_api_servers
  removal_policies   = ["OldestInstance", "Default"]

  # Attach to NLB server group for traffic distribution
  alb_server_group {
    alb_server_group_id = alicloud_nlb_server_group.api.id
    port                = 80
    weight              = 100
  }
}

resource "alicloud_ess_scaling_configuration" "api_server" {
  scaling_group_id           = alicloud_ess_scaling_group.api_server.id
  scaling_configuration_name = "fractalbits-api-server-config"
  image_id                   = var.os_image
  instance_type              = var.api_instance_type
  security_group_id          = alicloud_security_group.fractalbits.id
  system_disk_category       = "cloud_essd"
  system_disk_size           = var.system_disk_size
  internet_max_bandwidth_out = 0
  active                     = true
  force_delete               = true

  user_data = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
    ram_role_name        = alicloud_ram_role.fractalbits.name
    deploy_staging_bucket = local.deploy_bucket
    service_role         = "api_server"
    instance_role_args   = ""
    polardb_ddb_endpoint = var.rss_backend == "ddb" ? alicloud_drds_instance.polardb[0].connection_string : ""
  }))

  role_name = alicloud_ram_role.fractalbits.name
}
