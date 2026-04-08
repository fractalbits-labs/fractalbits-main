# --- PolarDB-X (DRDS) Instance ---
#
# Only created when rss_backend = "ddb" (DynamoDB-compatible mode).
# PolarDB-X provides distributed SQL compatible with DynamoDB API via
# the DRDS (Distributed Relational Database Service) interface.
#
# Tables (service-discovery, leader-election, api-keys-and-buckets)
# are created by the fractalbits-bootstrap binary at runtime.

resource "alicloud_drds_instance" "polardb" {
  count                = var.rss_backend == "ddb" ? 1 : 0
  description          = "fractalbits-polardb-x"
  zone_id              = var.zone_id
  instance_series      = "drds.sn2.4c16g"
  instance_charge_type = "PostPaid"
  vswitch_id           = alicloud_vswitch.fractalbits.id
  specification        = "drds.sn2.4c16g.8C32G"
}
