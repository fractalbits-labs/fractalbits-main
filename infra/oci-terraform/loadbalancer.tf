# --- Network Load Balancer (Internal, Layer 4) ---

resource "oci_network_load_balancer_network_load_balancer" "api_lb" {
  compartment_id = var.compartment_ocid
  display_name   = "api-lb-${var.cluster_id}"
  subnet_id      = oci_core_subnet.private_a.id
  is_private     = true

  freeform_tags = local.freeform_tags
}

# --- Backend Set ---

resource "oci_network_load_balancer_backend_set" "api_backend" {
  name                     = "api-backend-${var.cluster_id}"
  network_load_balancer_id = oci_network_load_balancer_network_load_balancer.api_lb.id
  policy                   = "FIVE_TUPLE"

  health_checker {
    protocol          = "TCP"
    port              = 80
    interval_in_millis = 10000
    timeout_in_millis  = 5000
    retries           = 3
  }
}

# --- Listener ---

resource "oci_network_load_balancer_listener" "api_listener" {
  name                     = "api-listener-${var.cluster_id}"
  network_load_balancer_id = oci_network_load_balancer_network_load_balancer.api_lb.id
  default_backend_set_name = oci_network_load_balancer_backend_set.api_backend.name
  protocol                 = "TCP"
  port                     = 80
}

# Note: Backends from the instance pool are registered dynamically.
# For static backends, use oci_network_load_balancer_backend resources.
