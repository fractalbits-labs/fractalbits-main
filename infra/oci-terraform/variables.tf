variable "tenancy_ocid" {
  type        = string
  description = "OCI tenancy OCID"
}

variable "compartment_ocid" {
  type        = string
  description = "OCI compartment OCID for all resources"
}

variable "region" {
  type    = string
  default = "us-ashburn-1"
}

variable "availability_domain" {
  type        = string
  description = "Primary availability domain (e.g., Uocm:US-ASHBURN-AD-1)"
}

variable "availability_domain_b" {
  type        = string
  default     = ""
  description = "Secondary AD for HA (RSS-B). If empty, uses primary AD."
}

variable "cluster_id" {
  type        = string
  description = "Unique cluster identifier (e.g., timestamp-based)"
}

variable "vpc_template" {
  type    = string
  default = "mini"
  validation {
    condition     = contains(["mini", "standard"], var.vpc_template)
    error_message = "vpc_template must be 'mini' or 'standard'"
  }
}

variable "num_api_servers" {
  type    = number
  default = 1
}

variable "num_bss_nodes" {
  type    = number
  default = 1
}

# --- Instance shapes (Flex) ---

variable "rss_shape" {
  type    = string
  default = "VM.Standard.E5.Flex"
}

variable "rss_ocpus" {
  type    = number
  default = 2
}

variable "rss_memory_gb" {
  type    = number
  default = 16
}

variable "nss_shape" {
  type    = string
  default = "VM.Standard.E5.Flex"
}

variable "nss_ocpus" {
  type    = number
  default = 4
}

variable "nss_memory_gb" {
  type    = number
  default = 32
}

variable "bss_shape" {
  type    = string
  default = "VM.DenseIO.E5.Flex"
  description = "BSS shape - DenseIO for local NVMe, Standard with Block Volume fallback"
}

variable "bss_ocpus" {
  type    = number
  default = 8
  description = "VM.DenseIO.E5.Flex valid: 8/16/24/32/40/48"
}

variable "bss_memory_gb" {
  type    = number
  default = 96
  description = "VM.DenseIO.E5.Flex valid: 96/192/288/384/480/576"
}

variable "api_shape" {
  type    = string
  default = "VM.Standard.E5.Flex"
}

variable "api_ocpus" {
  type    = number
  default = 2
}

variable "api_memory_gb" {
  type    = number
  default = 16
}

# --- Backend & storage ---

variable "rss_backend" {
  type    = string
  default = "etcd"
  validation {
    condition     = contains(["etcd", "oci_nosql"], var.rss_backend)
    error_message = "rss_backend must be 'etcd' or 'oci_nosql'"
  }
}

variable "journal_type" {
  type    = string
  default = "block_volume"
  validation {
    condition     = contains(["local_nvme", "block_volume"], var.journal_type)
    error_message = "journal_type must be 'local_nvme' or 'block_volume'"
  }
}

variable "journal_volume_size_gb" {
  type    = number
  default = 50
  description = "OCI minimum Block Volume size is 50 GB"
}

variable "journal_volume_vpus_per_gb" {
  type        = number
  default     = 20
  description = "Block volume performance: 10=Balanced, 20=Higher, 30+=Ultra High"
}

variable "root_server_ha" {
  type    = bool
  default = false
}

variable "with_bench" {
  type    = bool
  default = false
}

variable "num_bench_clients" {
  type    = number
  default = 0
}

variable "use_generic_binaries" {
  type    = bool
  default = true
}

variable "os_image_id" {
  type        = string
  default     = ""
  description = "Custom image OCID. If empty, latest Ubuntu 24.04 is used."
}

variable "boot_volume_size_gb" {
  type    = number
  default = 50
}

variable "deploy_staging_bucket" {
  type        = string
  description = "OCI Object Storage bucket name for deploy staging"
}
