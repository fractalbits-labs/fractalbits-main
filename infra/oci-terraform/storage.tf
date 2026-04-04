# --- NSS Journal Block Volume ---

resource "oci_core_volume" "nss_journal" {
  count               = var.journal_type == "block_volume" ? 1 : 0
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain
  display_name        = "nss-journal-${var.cluster_id}"
  size_in_gbs         = var.journal_volume_size_gb
  vpus_per_gb         = var.journal_volume_vpus_per_gb

  freeform_tags = merge(local.freeform_tags, {
    "role" = "nss-journal"
  })
}

# Attach journal volume to NSS-A
resource "oci_core_volume_attachment" "nss_a_journal" {
  count           = var.journal_type == "block_volume" ? 1 : 0
  instance_id     = oci_core_instance.nss_a.id
  volume_id       = oci_core_volume.nss_journal[0].id
  attachment_type = "paravirtualized"
  display_name    = "nss-a-journal-attachment"
  is_shareable    = true
}

# Attach same journal volume to NSS-B (shared multi-writer)
resource "oci_core_volume_attachment" "nss_b_journal" {
  count           = var.journal_type == "block_volume" ? 1 : 0
  instance_id     = oci_core_instance.nss_b.id
  volume_id       = oci_core_volume.nss_journal[0].id
  attachment_type = "paravirtualized"
  display_name    = "nss-b-journal-attachment"
  is_shareable    = true
}

# --- Deploy Staging Bucket ---
# Created out-of-band (by xtask upload) since it must exist before terraform apply.
# This data source just references it for outputs.

data "oci_objectstorage_namespace" "ns" {
  compartment_id = var.compartment_ocid
}
