# --- VCN ---

resource "oci_core_vcn" "fractalbits" {
  compartment_id = var.compartment_ocid
  display_name   = "fractalbits-vcn-${var.cluster_id}"
  cidr_blocks    = ["10.0.0.0/16"]
  dns_label      = "fb${replace(substr(var.cluster_id, 0, min(11, length(var.cluster_id))), "-", "")}"
  freeform_tags  = local.freeform_tags
}

# --- Gateways ---

resource "oci_core_nat_gateway" "fractalbits" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.fractalbits.id
  display_name   = "fractalbits-nat-${var.cluster_id}"
  freeform_tags  = local.freeform_tags
}

data "oci_core_services" "all_services" {
  filter {
    name   = "name"
    values = ["All .* Services In Oracle Services Network"]
    regex  = true
  }
}

resource "oci_core_service_gateway" "fractalbits" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.fractalbits.id
  display_name   = "fractalbits-svc-gw-${var.cluster_id}"

  services {
    service_id = data.oci_core_services.all_services.services[0].id
  }

  freeform_tags = local.freeform_tags
}

# --- Route Tables ---

resource "oci_core_route_table" "private" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.fractalbits.id
  display_name   = "fractalbits-private-rt-${var.cluster_id}"

  route_rules {
    network_entity_id = oci_core_nat_gateway.fractalbits.id
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    description       = "NAT gateway for outbound internet"
  }

  route_rules {
    network_entity_id = oci_core_service_gateway.fractalbits.id
    destination       = data.oci_core_services.all_services.services[0].cidr_block
    destination_type  = "SERVICE_CIDR_BLOCK"
    description       = "Service gateway for OCI services"
  }

  freeform_tags = local.freeform_tags
}

# --- Subnets ---

resource "oci_core_subnet" "private_a" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.fractalbits.id
  display_name               = "fractalbits-private-a-${var.cluster_id}"
  cidr_block                 = "10.0.1.0/24"
  prohibit_public_ip_on_vnic = true
  route_table_id             = oci_core_route_table.private.id
  security_list_ids          = [oci_core_security_list.private.id]
  dns_label                  = "priva"
  freeform_tags              = local.freeform_tags
}

resource "oci_core_subnet" "private_b" {
  count                      = local.rss_ha_enabled ? 1 : 0
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.fractalbits.id
  display_name               = "fractalbits-private-b-${var.cluster_id}"
  cidr_block                 = "10.0.2.0/24"
  prohibit_public_ip_on_vnic = true
  route_table_id             = oci_core_route_table.private.id
  security_list_ids          = [oci_core_security_list.private.id]
  dns_label                  = "privb"
  freeform_tags              = local.freeform_tags
}

# --- Security Lists ---

resource "oci_core_security_list" "private" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.fractalbits.id
  display_name   = "fractalbits-private-sl-${var.cluster_id}"

  # Allow all egress
  egress_security_rules {
    protocol    = "all"
    destination = "0.0.0.0/0"
    description = "Allow all outbound"
  }

  # Allow all intra-VCN traffic
  ingress_security_rules {
    protocol    = "all"
    source      = "10.0.0.0/16"
    description = "Allow all intra-VCN traffic"
  }

  # SSH from OCI Bastion service range (or all for simplicity)
  ingress_security_rules {
    protocol = "6" # TCP
    source   = "0.0.0.0/0"

    tcp_options {
      min = 22
      max = 22
    }

    description = "SSH access"
  }

  # ICMP
  ingress_security_rules {
    protocol = "1" # ICMP
    source   = "10.0.0.0/16"

    icmp_options {
      type = 3
      code = 4
    }

    description = "ICMP path discovery"
  }

  freeform_tags = local.freeform_tags
}
