#!/bin/bash
set -ex
exec > >(tee -a /var/log/fractalbits-bootstrap.log) 2>&1
echo "=== Bootstrap started at $(date) ==="

# Get instance metadata from Alicloud metadata service
INSTANCE_ID=$(curl -sf http://100.100.100.200/latest/meta-data/instance-id)
REGION=$(curl -sf http://100.100.100.200/latest/meta-data/region-id)

echo "Instance: $INSTANCE_ID Region: $REGION"

# Install aliyun CLI
if ! command -v aliyun &>/dev/null; then
  echo "Installing aliyun CLI..."
  curl -fsSL https://aliyuncli.alicdn.com/aliyun-cli-linux-latest-amd64.tgz | tar xz -C /usr/local/bin/
fi

# Configure CLI with instance RAM role (no credentials needed)
aliyun configure set \
  --profile default \
  --mode EcsRamRole \
  --ram-role-name ${ram_role_name} \
  --region $REGION

# Download bootstrap binary from OSS
ARCH=$(uname -m)
BUCKET=${deploy_staging_bucket}
mkdir -p /opt/fractalbits/bin

for i in $(seq 1 5); do
  aliyun oss cp oss://$BUCKET/$ARCH/fractalbits-bootstrap /opt/fractalbits/bin/fractalbits-bootstrap && break
  echo "Download attempt $i failed, retrying in 30s..."
  sleep 30
done
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap

# Set PolarDB-DDB endpoint if configured
%{ if polardb_ddb_endpoint != "" ~}
export AWS_ENDPOINT_URL_DYNAMODB=${polardb_ddb_endpoint}
export AWS_DEFAULT_REGION=$REGION
%{ endif ~}

# Run bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap oss://$BUCKET \
  --role ${service_role} \
  ${instance_role_args}

echo "=== Bootstrap completed at $(date) ==="
