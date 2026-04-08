resource "alicloud_vpc" "fractalbits" {
  vpc_name   = "fractalbits-vpc"
  cidr_block = "10.0.0.0/16"
}

resource "alicloud_vswitch" "fractalbits" {
  vpc_id       = alicloud_vpc.fractalbits.id
  cidr_block   = "10.0.1.0/24"
  zone_id      = var.zone_id
  vswitch_name = "fractalbits-subnet"
}

resource "alicloud_security_group" "fractalbits" {
  name   = "fractalbits-sg"
  vpc_id = alicloud_vpc.fractalbits.id
}

# Allow all intra-VPC traffic
resource "alicloud_security_group_rule" "allow_internal" {
  type              = "ingress"
  ip_protocol       = "all"
  nic_type          = "intranet"
  policy            = "accept"
  port_range        = "-1/-1"
  priority          = 1
  security_group_id = alicloud_security_group.fractalbits.id
  cidr_ip           = "10.0.0.0/16"
}

# Allow SSH from anywhere (for management)
resource "alicloud_security_group_rule" "allow_ssh" {
  type              = "ingress"
  ip_protocol       = "tcp"
  nic_type          = "intranet"
  policy            = "accept"
  port_range        = "22/22"
  priority          = 1
  security_group_id = alicloud_security_group.fractalbits.id
  cidr_ip           = "0.0.0.0/0"
}

# Allow all outbound traffic
resource "alicloud_security_group_rule" "allow_egress" {
  type              = "egress"
  ip_protocol       = "all"
  nic_type          = "intranet"
  policy            = "accept"
  port_range        = "-1/-1"
  priority          = 1
  security_group_id = alicloud_security_group.fractalbits.id
  cidr_ip           = "0.0.0.0/0"
}

# NAT Gateway for internet access
resource "alicloud_nat_gateway" "fractalbits" {
  vpc_id           = alicloud_vpc.fractalbits.id
  nat_gateway_name = "fractalbits-nat"
  payment_type     = "PayAsYouGo"
  vswitch_id       = alicloud_vswitch.fractalbits.id
  nat_type         = "Enhanced"
}

resource "alicloud_eip_address" "nat" {
  address_name         = "fractalbits-nat-eip"
  bandwidth            = "100"
  internet_charge_type = "PayByTraffic"
}

resource "alicloud_eip_association" "nat" {
  allocation_id = alicloud_eip_address.nat.id
  instance_id   = alicloud_nat_gateway.fractalbits.id
  instance_type = "Nat"
}

resource "alicloud_snat_entry" "fractalbits" {
  snat_table_id     = alicloud_nat_gateway.fractalbits.snat_table_ids
  source_vswitch_id = alicloud_vswitch.fractalbits.id
  snat_ip           = alicloud_eip_address.nat.ip_address

  depends_on = [alicloud_eip_association.nat]
}
