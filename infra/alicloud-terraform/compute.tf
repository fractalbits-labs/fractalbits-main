# RSS instances
resource "alicloud_instance" "rss_a" {
  instance_name        = local.rss_a_name
  host_name            = local.rss_a_name
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.rss_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "root_server --leader"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}

resource "alicloud_instance" "rss_b" {
  count                = local.rss_ha_enabled ? 1 : 0
  instance_name        = local.rss_b_name
  host_name            = local.rss_b_name
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.rss_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "root_server"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}

# NSS instances
resource "alicloud_instance" "nss_a" {
  instance_name        = local.nss_a_name
  host_name            = local.nss_a_name
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.nss_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "nss_server"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}

resource "alicloud_disk" "nss_a_journal" {
  availability_zone = var.zone_id
  disk_name         = "nss-a-journal"
  category          = var.journal_disk_category
  size              = var.journal_disk_size
}

resource "alicloud_disk_attachment" "nss_a_journal" {
  disk_id     = alicloud_disk.nss_a_journal.id
  instance_id = alicloud_instance.nss_a.id
  device_name = "/dev/xvdb"
}

resource "alicloud_instance" "nss_b" {
  instance_name        = local.nss_b_name
  host_name            = local.nss_b_name
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.nss_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "nss_server --standby"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}

resource "alicloud_disk" "nss_b_journal" {
  availability_zone = var.zone_id
  disk_name         = "nss-b-journal"
  category          = var.journal_disk_category
  size              = var.journal_disk_size
}

resource "alicloud_disk_attachment" "nss_b_journal" {
  disk_id     = alicloud_disk.nss_b_journal.id
  instance_id = alicloud_instance.nss_b.id
  device_name = "/dev/xvdb"
}

# BSS instances
resource "alicloud_instance" "bss" {
  count                = var.num_bss_nodes
  instance_name        = "bss-${count.index + 1}"
  host_name            = "bss-${count.index + 1}"
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.bss_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "bss_server"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}

# API Server instances
resource "alicloud_instance" "api_server" {
  count                = var.num_api_servers
  instance_name        = "api-server-${count.index + 1}"
  host_name            = "api-server-${count.index + 1}"
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.api_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "api_server"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}

# Bench server instances (optional)
resource "alicloud_instance" "bench_server" {
  count                = var.with_bench ? 1 : 0
  instance_name        = "bench-server"
  host_name            = "bench-server"
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.bench_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "bench_server"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}

resource "alicloud_instance" "bench_client" {
  count                = var.with_bench ? var.num_bench_clients : 0
  instance_name        = "bench-client-${count.index + 1}"
  host_name            = "bench-client-${count.index + 1}"
  availability_zone    = var.zone_id
  security_groups      = [alicloud_security_group.fractalbits.id]
  instance_type        = var.bench_instance_type
  system_disk_category = "cloud_essd"
  system_disk_size     = var.system_disk_size
  image_id             = var.os_image
  vswitch_id           = alicloud_vswitch.fractalbits.id
  internet_max_bandwidth_out = 0

  user_data = base64encode(templatefile("${path.module}/templates/startup-script.sh.tpl", {
    role           = "bench_client"
    deploy_bucket  = local.deploy_bucket
    nss_a_name     = local.nss_a_name
    nss_b_name     = local.nss_b_name
  }))
}
