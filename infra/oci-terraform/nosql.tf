# OCI NoSQL tables for RSS backend.
#
# These tables are created via Terraform when rss_backend = "oci_nosql".
# Unlike Firestore (which is created manually out-of-band), OCI NoSQL tables
# can be safely managed by Terraform since they support idempotent creation.

resource "oci_nosql_table" "service_discovery" {
  count          = var.rss_backend == "oci_nosql" ? 1 : 0
  compartment_id = var.compartment_ocid
  name           = "fractalbits-service-discovery"

  ddl_statement = <<-EOT
    CREATE TABLE IF NOT EXISTS "fractalbits-service-discovery" (
      id STRING,
      value STRING,
      version INTEGER,
      PRIMARY KEY(id)
    )
  EOT

  table_limits {
    max_read_units     = 50
    max_write_units    = 50
    max_storage_in_gbs = 1
  }

  freeform_tags = local.freeform_tags
}

resource "oci_nosql_table" "leader_election" {
  count          = var.rss_backend == "oci_nosql" ? 1 : 0
  compartment_id = var.compartment_ocid
  name           = "fractalbits-leader-election"

  ddl_statement = <<-EOT
    CREATE TABLE IF NOT EXISTS "fractalbits-leader-election" (
      leader_key STRING,
      instance_id STRING,
      ip_address STRING,
      port INTEGER,
      lease_expiry LONG,
      fence_token LONG,
      renewal_count LONG,
      last_heartbeat LONG,
      version STRING,
      PRIMARY KEY(leader_key)
    )
  EOT

  table_limits {
    max_read_units     = 50
    max_write_units    = 50
    max_storage_in_gbs = 1
  }

  freeform_tags = local.freeform_tags
}

resource "oci_nosql_table" "api_keys_and_buckets" {
  count          = var.rss_backend == "oci_nosql" ? 1 : 0
  compartment_id = var.compartment_ocid
  name           = "fractalbits-api-keys-and-buckets"

  ddl_statement = <<-EOT
    CREATE TABLE IF NOT EXISTS "fractalbits-api-keys-and-buckets" (
      id STRING,
      value STRING,
      version INTEGER,
      PRIMARY KEY(id)
    )
  EOT

  table_limits {
    max_read_units     = 50
    max_write_units    = 50
    max_storage_in_gbs = 1
  }

  freeform_tags = local.freeform_tags
}
