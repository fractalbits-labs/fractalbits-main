output "cluster_id" {
  value = var.cluster_id
}

output "vcn_id" {
  value = oci_core_vcn.fractalbits.id
}

output "rss_a_name" {
  value = oci_core_instance.rss_a.display_name
}

output "rss_a_private_ip" {
  value = oci_core_instance.rss_a.private_ip
}

output "rss_b_name" {
  value = local.rss_ha_enabled ? oci_core_instance.rss_b[0].display_name : ""
}

output "nss_a_name" {
  value = oci_core_instance.nss_a.display_name
}

output "nss_a_private_ip" {
  value = oci_core_instance.nss_a.private_ip
}

output "nss_b_name" {
  value = oci_core_instance.nss_b.display_name
}

output "nss_b_private_ip" {
  value = oci_core_instance.nss_b.private_ip
}

output "api_lb_ip" {
  value = oci_network_load_balancer_network_load_balancer.api_lb.ip_addresses[0].ip_address
}

output "api_instance_pool_id" {
  value = oci_core_instance_pool.api_servers.id
}

output "bss_instance_names" {
  value = [for b in oci_core_instance.bss : b.display_name]
}

output "bench_server_name" {
  value = var.with_bench ? oci_core_instance.bench[0].display_name : ""
}

output "deploy_staging_bucket" {
  value = var.deploy_staging_bucket
}

output "object_storage_namespace" {
  value = data.oci_objectstorage_namespace.ns.namespace
}
