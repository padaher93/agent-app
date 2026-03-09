# Storage Contract (V1)

Date: 2026-03-06  
Owner: Platform

## Purpose

Define how package attachments are stored and resolved across distributed services (gateway, internal API, extraction runtime, evidence viewer).

## Accepted `storage_uri` schemes

1. Local filesystem:
   - absolute path (`/var/data/file.pdf`)
   - `file:///var/data/file.pdf`
2. HTTP/HTTPS object URLs:
   - `https://...` (including signed URLs)
3. S3 object URIs:
   - `s3://bucket/key`

Unknown schemes are rejected in strict runtime profiles.

## Resolution behavior

1. Local paths: resolved directly.
2. HTTP/HTTPS: downloaded to `runtime/storage_cache` (content-addressed by URI hash).
3. S3: downloaded to `runtime/storage_cache` via AWS SDK (`boto3`) and cached by URI hash.

## Gateway attachment storage modes

1. `local` (dev/testing):
   - writes files to local directory (`--attachments-dir`).
2. `s3` (staging/prod strict profiles):
   - uploads attachments to configured bucket.
   - emits `s3://...` URIs into package manifests.

## Strict profile requirements (`staging` / `prod`)

1. Gateway ingress: Postmark only.
2. Attachment storage mode: `s3`.
3. S3 bucket: required.
4. Internal processing: `llm` extraction mode only.
5. Ingest rejects unsupported storage URIs.
6. Process fails closed if evidence sources cannot be materialized.

## Operational notes

1. Do not rely on host-local paths for cross-service deployments.
2. Keep object-store IAM scoped to required prefix/bucket.
3. Rotate object-store credentials per security runbook.
