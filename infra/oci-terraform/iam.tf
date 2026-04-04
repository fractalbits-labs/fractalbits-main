# --- Dynamic Group (Instance Principal) ---
# All instances in this compartment can authenticate to OCI services

resource "oci_identity_dynamic_group" "fractalbits" {
  compartment_id = var.tenancy_ocid
  name           = "fractalbits-${substr(var.cluster_id, 0, min(20, length(var.cluster_id)))}"
  description    = "Fractalbits compute instances for cluster ${var.cluster_id}"
  matching_rule  = "Any {instance.compartment.id = '${var.compartment_ocid}'}"

  freeform_tags = local.freeform_tags
}

# --- IAM Policies ---

resource "oci_identity_policy" "fractalbits" {
  compartment_id = var.compartment_ocid
  name           = "fractalbits-policy-${substr(var.cluster_id, 0, min(20, length(var.cluster_id)))}"
  description    = "Allow Fractalbits instances to access OCI services"

  statements = [
    "Allow dynamic-group ${oci_identity_dynamic_group.fractalbits.name} to manage object-family in compartment id ${var.compartment_ocid}",
    "Allow dynamic-group ${oci_identity_dynamic_group.fractalbits.name} to manage nosql-family in compartment id ${var.compartment_ocid}",
    "Allow dynamic-group ${oci_identity_dynamic_group.fractalbits.name} to use instance-family in compartment id ${var.compartment_ocid}",
    "Allow dynamic-group ${oci_identity_dynamic_group.fractalbits.name} to read all-resources in compartment id ${var.compartment_ocid}",
  ]

  freeform_tags = local.freeform_tags
}
