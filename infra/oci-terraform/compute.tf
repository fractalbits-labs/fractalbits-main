# --- OS Image Lookup ---

data "oci_core_images" "ubuntu" {
  count                    = var.os_image_id == "" ? 1 : 0
  compartment_id           = var.compartment_ocid
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "24.04"
  shape                    = var.rss_shape
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"

  filter {
    name   = "display_name"
    values = ["^Canonical-Ubuntu-24.04-aarch64-.*|^Canonical-Ubuntu-24.04-.*"]
    regex  = true
  }
}

locals {
  image_id = var.os_image_id != "" ? var.os_image_id : data.oci_core_images.ubuntu[0].images[0].id

  common_metadata = {
    "cluster_id" = var.cluster_id
    "rss_backend" = var.rss_backend
  }
}

# --- RSS-A (Root Storage Server - Leader) ---

resource "oci_core_instance" "rss_a" {
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain
  display_name        = "rss-a-${var.cluster_id}"

  shape = var.rss_shape
  shape_config {
    ocpus         = var.rss_ocpus
    memory_in_gbs = var.rss_memory_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.private_a.id
    assign_public_ip = false
    display_name     = "rss-a-vnic"
    nsg_ids          = []
  }

  source_details {
    source_type             = "image"
    source_id               = local.image_id
    boot_volume_size_in_gbs = var.boot_volume_size_gb
  }

  metadata = merge(local.common_metadata, {
    "service_role"  = "root_server"
    "instance_role" = "leader"
    "user_data" = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
      bucket_name   = var.deploy_staging_bucket
      deploy_target = "oci"
      service_role  = "root_server"
      instance_role = "leader"
      rss_a_ip      = "" # resolved post-boot via IMDS
      cluster_id    = var.cluster_id
    }))
  })

  freeform_tags = merge(local.freeform_tags, {
    "role" = "rss"
  })

  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}

# --- RSS-B (Root Storage Server - Follower, HA only) ---

resource "oci_core_instance" "rss_b" {
  count               = local.rss_ha_enabled ? 1 : 0
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain_b != "" ? var.availability_domain_b : var.availability_domain
  display_name        = "rss-b-${var.cluster_id}"

  shape = var.rss_shape
  shape_config {
    ocpus         = var.rss_ocpus
    memory_in_gbs = var.rss_memory_gb
  }

  create_vnic_details {
    subnet_id        = local.rss_ha_enabled && length(oci_core_subnet.private_b) > 0 ? oci_core_subnet.private_b[0].id : oci_core_subnet.private_a.id
    assign_public_ip = false
    display_name     = "rss-b-vnic"
  }

  source_details {
    source_type             = "image"
    source_id               = local.image_id
    boot_volume_size_in_gbs = var.boot_volume_size_gb
  }

  metadata = merge(local.common_metadata, {
    "service_role"  = "root_server"
    "instance_role" = "follower"
    "user_data" = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
      bucket_name   = var.deploy_staging_bucket
      deploy_target = "oci"
      service_role  = "root_server"
      instance_role = "follower"
      rss_a_ip      = ""
      cluster_id    = var.cluster_id
    }))
  })

  freeform_tags = merge(local.freeform_tags, {
    "role" = "rss"
  })

  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}

# --- NSS-A (Namespace Server - Primary) ---

resource "oci_core_instance" "nss_a" {
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain
  display_name        = "nss-a-${var.cluster_id}"

  shape = var.nss_shape
  shape_config {
    ocpus         = var.nss_ocpus
    memory_in_gbs = var.nss_memory_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.private_a.id
    assign_public_ip = false
    display_name     = "nss-a-vnic"
  }

  source_details {
    source_type             = "image"
    source_id               = local.image_id
    boot_volume_size_in_gbs = var.boot_volume_size_gb
  }

  metadata = merge(local.common_metadata, {
    "service_role"  = "nss_server"
    "instance_role" = "primary"
    "user_data" = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
      bucket_name   = var.deploy_staging_bucket
      deploy_target = "oci"
      service_role  = "nss_server"
      instance_role = "primary"
      rss_a_ip      = ""
      cluster_id    = var.cluster_id
    }))
  })

  freeform_tags = merge(local.freeform_tags, {
    "role" = "nss"
  })

  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}

# --- NSS-B (Namespace Server - Standby) ---

resource "oci_core_instance" "nss_b" {
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain
  display_name        = "nss-b-${var.cluster_id}"

  shape = var.nss_shape
  shape_config {
    ocpus         = var.nss_ocpus
    memory_in_gbs = var.nss_memory_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.private_a.id
    assign_public_ip = false
    display_name     = "nss-b-vnic"
  }

  source_details {
    source_type             = "image"
    source_id               = local.image_id
    boot_volume_size_in_gbs = var.boot_volume_size_gb
  }

  metadata = merge(local.common_metadata, {
    "service_role"  = "nss_server"
    "instance_role" = "standby"
    "user_data" = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
      bucket_name   = var.deploy_staging_bucket
      deploy_target = "oci"
      service_role  = "nss_server"
      instance_role = "standby"
      rss_a_ip      = ""
      cluster_id    = var.cluster_id
    }))
  })

  freeform_tags = merge(local.freeform_tags, {
    "role" = "nss"
  })

  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}

# --- Bench Server (optional) ---

resource "oci_core_instance" "bench" {
  count               = var.with_bench ? 1 : 0
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain
  display_name        = "bench-${var.cluster_id}"

  shape = var.api_shape
  shape_config {
    ocpus         = var.api_ocpus
    memory_in_gbs = var.api_memory_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.private_a.id
    assign_public_ip = false
    display_name     = "bench-vnic"
  }

  source_details {
    source_type             = "image"
    source_id               = local.image_id
    boot_volume_size_in_gbs = var.boot_volume_size_gb
  }

  metadata = merge(local.common_metadata, {
    "service_role"  = "bench_server"
    "instance_role" = "bench"
    "user_data" = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
      bucket_name   = var.deploy_staging_bucket
      deploy_target = "oci"
      service_role  = "bench_server"
      instance_role = "bench"
      rss_a_ip      = ""
      cluster_id    = var.cluster_id
    }))
  })

  freeform_tags = merge(local.freeform_tags, {
    "role" = "bench"
  })

  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}
