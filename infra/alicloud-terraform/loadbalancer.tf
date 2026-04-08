# --- Network Load Balancer (Internal, Layer 4) ---

resource "alicloud_nlb_load_balancer" "api" {
  load_balancer_name = "fractalbits-api-nlb"
  load_balancer_type = "Network"
  address_type       = "Intranet"
  vpc_id             = alicloud_vpc.fractalbits.id

  zone_mappings {
    vswitch_id = alicloud_vswitch.fractalbits.id
    zone_id    = var.zone_id
  }
}

# --- Server Group ---

resource "alicloud_nlb_server_group" "api" {
  server_group_name = "fractalbits-api-sg"
  server_group_type = "Instance"
  vpc_id            = alicloud_vpc.fractalbits.id
  protocol          = "TCP"

  health_check {
    health_check_enabled         = true
    health_check_type            = "TCP"
    health_check_connect_port    = 80
    healthy_threshold            = 3
    unhealthy_threshold          = 3
    health_check_interval        = 10
    health_check_connect_timeout = 5
  }
}

# Register static API server instances as backends.
# ESS scaling group instances are registered automatically via
# alb_server_group in instance_group.tf.
resource "alicloud_nlb_server_group_server_attachment" "api_server" {
  count           = var.num_api_servers
  server_group_id = alicloud_nlb_server_group.api.id
  server_type     = "Ecs"
  server_id       = alicloud_instance.api_server[count.index].id
  server_ip       = alicloud_instance.api_server[count.index].private_ip
  port            = 80
  weight          = 100
}

# --- Listener ---

resource "alicloud_nlb_listener" "api" {
  listener_protocol  = "TCP"
  listener_port      = 80
  load_balancer_id   = alicloud_nlb_load_balancer.api.id
  server_group_id    = alicloud_nlb_server_group.api.id
  idle_timeout       = 900
}
