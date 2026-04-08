#!/bin/bash
set -euo pipefail

LOG_FILE="/var/log/fractalbits-bootstrap.log"

mkdir -p /opt/fractalbits/bin
ARCH=$(arch)
# Retry download to handle eventual consistency of OSS
for attempt in 1 2 3 4 5; do
  aliyun oss cp "oss://${deploy_bucket}/$ARCH/fractalbits-bootstrap" /opt/fractalbits/bin/fractalbits-bootstrap && break
  echo "Download attempt $attempt failed, retrying in 30s..." | tee -a "$LOG_FILE"
  sleep 30
done
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap "oss://${deploy_bucket}" --role ${role} --nss-a-id ${nss_a_name} --nss-b-id ${nss_b_name} 2>&1 | tee -a "$LOG_FILE"
