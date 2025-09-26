#!/bin/bash

set -e

# Get AWS account ID
ACCOUNT_ID=ACCOUNT_ID_OWNER
REGION="us-east-2"

# S3 Express bucket configuration
BUCKET_NAME="test-cross-az-${ACCOUNT_ID}--use2-az2--x-s3"
TEMP_DIR="/tmp/s3express-test-$$"

echo "Starting S3 Express cross-AZ test..."
echo "Bucket: $BUCKET_NAME"
echo "Temp directory: $TEMP_DIR"

# Create temporary directory for test files
mkdir -p "$TEMP_DIR"

# Create temporary JSON config file for S3 Express bucket
CONFIG_FILE="$TEMP_DIR/bucket-config.json"
cat > "$CONFIG_FILE" << 'EOF'
{
  "Location": {
    "Type": "AvailabilityZone",
    "Name": "use2-az2"
  },
  "Bucket": {
    "DataRedundancy": "SingleAvailabilityZone",
    "Type": "Directory"
  }
}
EOF

# Create S3 Express bucket in use2-az2
echo "Creating S3 Express bucket in use2-az2..."
if ! aws s3api create-bucket \
    --bucket "$BUCKET_NAME" \
    --region "$REGION" \
    --create-bucket-configuration "file://$CONFIG_FILE"; then
    echo "Failed to create S3 Express bucket"
    rm -rf "$TEMP_DIR"
    exit 1
fi

# Generate test files (small files < 512KB, total ~1GB)
echo "Generating test files..."
FILE_COUNT=2500  # 2500 files * 400KB average = ~1GB
FILE_SIZES=(100 200 300 400 500)  # KB sizes

for i in $(seq 1 $FILE_COUNT); do
    SIZE_INDEX=$((i % ${#FILE_SIZES[@]}))
    SIZE_KB=${FILE_SIZES[$SIZE_INDEX]}
    dd if=/dev/urandom of="$TEMP_DIR/file_${i}.dat" bs=1024 count=$SIZE_KB 2>/dev/null
    if [ $((i % 100)) -eq 0 ]; then
        echo "Generated $i/$FILE_COUNT files..."
    fi
done

echo "Generated $FILE_COUNT files"

# Upload files to S3 Express bucket
echo "Uploading files to S3 Express bucket..."
aws s3 cp "$TEMP_DIR/" "s3://$BUCKET_NAME/" --recursive --region "$REGION"

echo "Upload complete"

# Clean up - delete all objects and bucket
echo "Cleaning up..."
echo "Deleting objects from bucket..."
aws s3 rm "s3://$BUCKET_NAME" --recursive --region "$REGION"

echo "Deleting bucket..."
aws s3api delete-bucket --bucket "$BUCKET_NAME" --region "$REGION"

# Clean up local files
echo "Removing temporary files..."
rm -rf "$TEMP_DIR"

echo "S3 Express cross-AZ test completed successfully"
