# --- NSS Journal Shared Cloud Disk ---
#
# ESSD cloud disk for NSS journal with multi-attach support.
# Attached to both NSS-A and NSS-B for shared access during failover.
# Note: Multi-attach (shared) disks require ESSD category and
# both instances must be in the same availability zone.
#
# The journal disk resources in compute.tf are per-instance (separate disks).
# This shared disk provides an alternative for environments that need
# a single shared journal between NSS-A and NSS-B.

resource "alicloud_ecs_disk" "nss_journal_shared" {
  disk_name         = "nss-journal-shared"
  zone_id           = var.zone_id
  category          = var.journal_disk_category
  size              = var.journal_disk_size
  multi_attach      = "Enabled"
}

resource "alicloud_disk_attachment" "nss_a_journal_shared" {
  disk_id     = alicloud_ecs_disk.nss_journal_shared.id
  instance_id = alicloud_instance.nss_a.id
  device_name = "/dev/xvdc"
}

resource "alicloud_disk_attachment" "nss_b_journal_shared" {
  disk_id     = alicloud_ecs_disk.nss_journal_shared.id
  instance_id = alicloud_instance.nss_b.id
  device_name = "/dev/xvdc"

  depends_on = [alicloud_disk_attachment.nss_a_journal_shared]
}
