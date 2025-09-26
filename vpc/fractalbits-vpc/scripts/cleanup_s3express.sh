#!/bin/bash

# Script to delete S3 Express directory bucket objects and buckets

set -e

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

BUCKETS=(
    "fractalbits-data-$ACCOUNT_ID--usw2-az3--x-s3"
    "fractalbits-data-$ACCOUNT_ID--usw2-az4--x-s3"
    "fractalbits-data-$ACCOUNT_ID--use1-az4--x-s3"
    "fractalbits-data-$ACCOUNT_ID--use1-az6--x-s3"
)

# Process each bucket in parallel
for BUCKET in "${BUCKETS[@]}"; do
    {
        # Delete all objects using aws s3 rm (faster than list + delete-objects)
        aws s3 rm "s3://$BUCKET" --recursive 2>/dev/null || true

        # Delete the bucket
        aws s3api delete-bucket --bucket "$BUCKET" 2>/dev/null || true

        echo "Deleted: $BUCKET"
    } &
done

# Wait for all background jobs to complete
wait

echo "Done"
