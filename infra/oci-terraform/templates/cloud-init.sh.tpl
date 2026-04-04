#!/bin/bash
set -euo pipefail

LOG_FILE="/var/log/fractalbits-bootstrap.log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Bootstrap started at $(date) ==="
echo "Role: ${service_role} / ${instance_role}"

mkdir -p /opt/fractalbits/bin

# Detect architecture
ARCH=$(arch)
if [ "$ARCH" = "aarch64" ]; then
  ARCH="aarch64"
else
  ARCH="x86_64"
fi

# Install OCI CLI if not present (needed for Object Storage downloads)
if ! command -v oci &>/dev/null; then
  echo "Installing OCI CLI..."
  bash -c "$(curl -L https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh)" -- --accept-all-defaults
  export PATH="$HOME/bin:$PATH"
fi

# Download bootstrap binary from OCI Object Storage with retries
BUCKET="${bucket_name}"
OBJECT_NAME="$ARCH/fractalbits-bootstrap"
LOCAL_PATH="/opt/fractalbits/bin/fractalbits-bootstrap"

for attempt in 1 2 3 4 5; do
  echo "Download attempt $attempt: oci os object get --bucket-name $BUCKET --name $OBJECT_NAME"
  if oci os object get \
    --auth instance_principal \
    --bucket-name "$BUCKET" \
    --name "$OBJECT_NAME" \
    --file "$LOCAL_PATH" 2>&1; then
    echo "Download succeeded"
    break
  fi
  echo "Download attempt $attempt failed, retrying in 30s..."
  sleep 30
done

chmod +x "$LOCAL_PATH"

# Build role arguments
ROLE_ARGS="--role ${service_role}"
%{ if instance_role == "leader" ~}
ROLE_ARGS="$ROLE_ARGS --rss-role leader"
%{ endif ~}
%{ if instance_role == "follower" ~}
ROLE_ARGS="$ROLE_ARGS --rss-role follower"
%{ endif ~}
%{ if instance_role == "primary" ~}
ROLE_ARGS="$ROLE_ARGS --nss-role primary"
%{ endif ~}
%{ if instance_role == "standby" ~}
ROLE_ARGS="$ROLE_ARGS --nss-role standby"
%{ endif ~}

echo "Starting bootstrap: $LOCAL_PATH oci://$BUCKET $ROLE_ARGS"
$LOCAL_PATH "oci://$BUCKET" $ROLE_ARGS

echo "=== Bootstrap completed at $(date) ==="
