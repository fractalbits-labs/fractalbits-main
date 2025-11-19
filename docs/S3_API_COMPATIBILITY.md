# S3 API Compatibility

FractalBits supports the most commonly used S3 operations.

## Supported Operations

**Object Operations:**
- ✅ PutObject
- ✅ GetObject
- ✅ DeleteObject
- ✅ HeadObject
- ✅ CopyObject
- ✅ GetObjectAttributes

**Multipart Upload:**
- ✅ CreateMultipartUpload
- ✅ UploadPart
- ✅ CompleteMultipartUpload
- ✅ AbortMultipartUpload
- ✅ ListParts
- ✅ ListMultipartUploads

**Bucket Operations:**
- ✅ CreateBucket
- ✅ DeleteBucket
- ✅ HeadBucket
- ✅ ListBuckets

**List Operations:**
- ✅ ListObjects (v1)
- ✅ ListObjectsV2

(*Note: We return objects in lexicographical order, which is not in S3 Express One Zone*)

**Batch Operations:**
- ✅ DeleteObjects (batch delete)

## Extension APIs

FractalBits provides additional operations beyond standard S3:
- `RenameObject`: Atomic object rename within bucket
- `RenameFolder`: Atomic folder rename (prefix change)

## Authentication

Full support for **AWS Signature V4** authentication, including trailing headers (chunked uploads).

## Known Limitations

The following S3 features are not yet supported:
- ❌ Versioning
- ❌ Server-side encryption (SSE-S3, SSE-KMS)
- ❌ Object tagging
- ❌ Access Control Lists (ACLs)
- ❌ Bucket policies
- ❌ Lifecycle policies
- ❌ Cross-region replication

We're actively working on expanding S3 API coverage.
