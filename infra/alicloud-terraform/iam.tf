# --- RAM Role for ECS Instance Principal ---

resource "alicloud_ram_role" "fractalbits" {
  name        = "fractalbits-ecs-role"
  description = "RAM role for Fractalbits ECS instances"
  force       = true

  document = jsonencode({
    Version   = "1"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["ecs.aliyuncs.com"]
        }
      }
    ]
  })
}

# --- OSS Access Policy ---

resource "alicloud_ram_policy" "oss_access" {
  policy_name     = "fractalbits-oss-access"
  description     = "Allow full OSS access for deploy bucket"
  force           = true

  policy_document = jsonencode({
    Version   = "1"
    Statement = [
      {
        Action   = "oss:*"
        Effect   = "Allow"
        Resource = [
          "acs:oss:*:*:${local.deploy_bucket}",
          "acs:oss:*:*:${local.deploy_bucket}/*",
        ]
      }
    ]
  })
}

resource "alicloud_ram_role_policy_attachment" "oss_access" {
  role_name   = alicloud_ram_role.fractalbits.name
  policy_name = alicloud_ram_policy.oss_access.policy_name
  policy_type = "Custom"
}

# --- ECS Read Policy ---

resource "alicloud_ram_policy" "ecs_read" {
  policy_name     = "fractalbits-ecs-read"
  description     = "Allow ECS Describe* for service discovery"
  force           = true

  policy_document = jsonencode({
    Version   = "1"
    Statement = [
      {
        Action   = "ecs:Describe*"
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "alicloud_ram_role_policy_attachment" "ecs_read" {
  role_name   = alicloud_ram_role.fractalbits.name
  policy_name = alicloud_ram_policy.ecs_read.policy_name
  policy_type = "Custom"
}

# --- PolarDB Access Policy (conditional) ---

resource "alicloud_ram_policy" "polardb_access" {
  count           = var.rss_backend == "ddb" ? 1 : 0
  policy_name     = "fractalbits-polardb-access"
  description     = "Allow PolarDB-X (DRDS) access for RSS backend"
  force           = true

  policy_document = jsonencode({
    Version   = "1"
    Statement = [
      {
        Action = [
          "drds:*",
          "polardbx:*",
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "alicloud_ram_role_policy_attachment" "polardb_access" {
  count       = var.rss_backend == "ddb" ? 1 : 0
  role_name   = alicloud_ram_role.fractalbits.name
  policy_name = alicloud_ram_policy.polardb_access[0].policy_name
  policy_type = "Custom"
}

# --- Attach role to all instances ---

resource "alicloud_ram_role_attachment" "all_instances" {
  role_name = alicloud_ram_role.fractalbits.name

  instance_ids = concat(
    [alicloud_instance.rss_a.id],
    alicloud_instance.rss_b[*].id,
    [alicloud_instance.nss_a.id],
    [alicloud_instance.nss_b.id],
    alicloud_instance.bss[*].id,
    alicloud_instance.api_server[*].id,
    alicloud_instance.bench_server[*].id,
    alicloud_instance.bench_client[*].id,
  )
}
