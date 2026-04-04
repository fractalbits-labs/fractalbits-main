# --- API Server Instance Configuration ---

resource "oci_core_instance_configuration" "api_server" {
  compartment_id = var.compartment_ocid
  display_name   = "api-server-config-${var.cluster_id}"

  instance_details {
    instance_type = "compute"

    launch_details {
      compartment_id = var.compartment_ocid
      display_name   = "api-${var.cluster_id}"

      shape = var.api_shape
      shape_config {
        ocpus         = var.api_ocpus
        memory_in_gbs = var.api_memory_gb
      }

      create_vnic_details {
        subnet_id        = oci_core_subnet.private_a.id
        assign_public_ip = false
      }

      source_details {
        source_type             = "image"
        image_id                = local.image_id
        boot_volume_size_in_gbs = var.boot_volume_size_gb
      }

      metadata = merge(local.common_metadata, {
        "service_role"  = "api_server"
        "instance_role" = "api"
        "user_data" = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
          bucket_name   = var.deploy_staging_bucket
          deploy_target = "oci"
          service_role  = "api_server"
          instance_role = "api"
          rss_a_ip      = ""
          cluster_id    = var.cluster_id
        }))
      })

      freeform_tags = merge(local.freeform_tags, {
        "role" = "api"
      })
    }
  }

  freeform_tags = local.freeform_tags

  lifecycle {
    create_before_destroy = true
  }
}

# --- API Server Instance Pool ---

resource "oci_core_instance_pool" "api_servers" {
  compartment_id            = var.compartment_ocid
  instance_configuration_id = oci_core_instance_configuration.api_server.id
  display_name              = "api-servers-${var.cluster_id}"
  size                      = var.num_api_servers

  placement_configurations {
    availability_domain = var.availability_domain
    primary_subnet_id   = oci_core_subnet.private_a.id
  }

  freeform_tags = local.freeform_tags
}

# --- BSS Server (DenseIO shape with local NVMe) ---

resource "oci_core_instance" "bss" {
  count               = var.num_bss_nodes
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain
  display_name        = "bss-${count.index}-${var.cluster_id}"

  shape = var.bss_shape
  shape_config {
    ocpus         = var.bss_ocpus
    memory_in_gbs = var.bss_memory_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.private_a.id
    assign_public_ip = false
    display_name     = "bss-${count.index}-vnic"
  }

  source_details {
    source_type             = "image"
    source_id               = local.image_id
    boot_volume_size_in_gbs = var.boot_volume_size_gb
  }

  metadata = merge(local.common_metadata, {
    "service_role"  = "bss_server"
    "instance_role" = "bss"
    "user_data" = base64encode(templatefile("${path.module}/templates/cloud-init.sh.tpl", {
      bucket_name   = var.deploy_staging_bucket
      deploy_target = "oci"
      service_role  = "bss_server"
      instance_role = "bss"
      rss_a_ip      = ""
      cluster_id    = var.cluster_id
    }))
  })

  freeform_tags = merge(local.freeform_tags, {
    "role" = "bss"
  })

  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}
