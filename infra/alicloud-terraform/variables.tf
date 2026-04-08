variable "region" {
  type    = string
  default = "cn-shanghai"
}

variable "zone_id" {
  type    = string
  default = "cn-shanghai-b"
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

variable "rss_instance_type" {
  type    = string
  default = "ecs.g7.xlarge"
}

variable "nss_instance_type" {
  type    = string
  default = "ecs.g7.2xlarge"
}

variable "bss_instance_type" {
  type    = string
  default = "ecs.i3.2xlarge"
}

variable "api_instance_type" {
  type    = string
  default = "ecs.g7.xlarge"
}

variable "bench_instance_type" {
  type    = string
  default = "ecs.g7.xlarge"
}

variable "rss_backend" {
  type    = string
  default = "ddb"
  validation {
    condition     = contains(["ddb", "etcd"], var.rss_backend)
    error_message = "rss_backend must be 'ddb' or 'etcd'"
  }
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

variable "system_disk_size" {
  type    = number
  default = 40
}

variable "journal_disk_size" {
  type    = number
  default = 20
}

variable "journal_disk_category" {
  type    = string
  default = "cloud_essd"
}

variable "os_image" {
  type    = string
  default = "ubuntu_22_04_x64_20G_alibase_20240101.vhd"
}
