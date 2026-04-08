output "vpc_id" {
  value = alicloud_vpc.fractalbits.id
}

output "vswitch_id" {
  value = alicloud_vswitch.fractalbits.id
}

output "security_group_id" {
  value = alicloud_security_group.fractalbits.id
}

output "rss_a_private_ip" {
  value = alicloud_instance.rss_a.private_ip
}

output "nss_a_private_ip" {
  value = alicloud_instance.nss_a.private_ip
}

output "nss_b_private_ip" {
  value = alicloud_instance.nss_b.private_ip
}

output "bss_private_ips" {
  value = alicloud_instance.bss[*].private_ip
}

output "api_server_private_ips" {
  value = alicloud_instance.api_server[*].private_ip
}

output "nlb_dns_name" {
  value = alicloud_nlb_load_balancer.api.dns_name
}

output "ram_role_name" {
  value = alicloud_ram_role.fractalbits.name
}

output "polardb_endpoint" {
  value = var.rss_backend == "ddb" ? alicloud_drds_instance.polardb[0].connection_string : ""
}

output "nss_journal_shared_disk_id" {
  value = alicloud_ecs_disk.nss_journal_shared.id
}
