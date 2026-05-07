---
sidebar_position: 5
---

# Storage API

Trex bundles a fork of [Supabase Storage](https://github.com/supabase/storage)
as a plugin (`@trex/storage`). It exposes an S3-compatible object-storage
surface plus a richer "bucket / object" REST API used by the Supabase JS / CLI
clients.

The storage plugin runs as an edge-function worker. The core server proxies
`${BASE_PATH}/storage/v1/*` to the worker, rewriting paths to `/storage-api/*`
before the worker sees them.

## Endpoint

```
${BASE_PATH}/storage/v1/*
```

With the default `BASE_PATH=/trex`, that's `/trex/storage/v1/*`.

## Authentication

Storage uses the same JWT access tokens as the rest of the management surface
(see [APIs → Auth](auth)) plus its own row-level security model on the
storage tables (`storage.buckets`, `storage.objects`). Pass the access token
as a Bearer header:

```
Authorization: Bearer <jwt-or-api-key>
```

The plugin honors the `app.user_id` and `app.user_role` GUCs set by the
`authContext` middleware.

## Buckets

| Method | Path | Description |
|--------|------|-------------|
| GET | `/bucket` | List buckets the caller can see. |
| POST | `/bucket` | Create a bucket. Body: `{ id, name?, public?, file_size_limit?, allowed_mime_types? }`. |
| GET | `/bucket/:bucket_id` | Get bucket metadata. |
| PUT | `/bucket/:bucket_id` | Update bucket settings (public flag, size limits, allowed mime types). |
| DELETE | `/bucket/:bucket_id` | Delete a bucket (must be empty unless `?force=true`). |
| POST | `/bucket/:bucket_id/empty` | Remove every object in the bucket. |

## Objects

| Method | Path | Description |
|--------|------|-------------|
| POST | `/object/:bucket/*` | Upload an object. Body is the raw object content; `Content-Type` sets the stored MIME. |
| PUT | `/object/:bucket/*` | Replace an existing object. |
| GET | `/object/:bucket/*` | Download a private object (auth required). |
| GET | `/object/public/:bucket/*` | Download from a public bucket (no auth). |
| GET | `/object/authenticated/:bucket/*` | Download with auth required, regardless of bucket public flag. |
| GET | `/object/info/:bucket/*` | Object metadata (size, mime type, owner, timestamps). |
| DELETE | `/object/:bucket/*` | Delete an object. |
| POST | `/object/copy` | Copy an object. Body: `{ bucketId, sourceKey, destinationKey, destinationBucket? }`. |
| POST | `/object/move` | Move (copy + delete). Body shape mirrors `copy`. |
| POST | `/object/list/:bucket` | List objects in a bucket. Body: `{ prefix?, limit?, offset?, search?, sortBy? }`. |

### Signed URLs

| Method | Path | Description |
|--------|------|-------------|
| POST | `/object/sign/:bucket/*` | Create a time-limited signed URL for download. Body: `{ expiresIn }` (seconds). |
| POST | `/object/sign/:bucket` | Create signed URLs for multiple objects in one request. |
| POST | `/object/upload/sign/:bucket/*` | Create a time-limited signed URL for *upload* (resumable). |
| GET | `/object/sign/:bucket/*?token=…` | Resolve a signed-URL token. Used by `/object/public/...` redirects. |

## S3-compatible API

Trex Storage exposes an S3-compatible surface at:

```
${BASE_PATH}/storage/v1/s3
```

Use any S3 SDK with the endpoint set to that URL. Authentication uses the
storage anon / service-role keys via SigV4. The S3 surface supports
`PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `ListObjectsV2`,
`CopyObject`, multipart upload (`CreateMultipartUpload`,
`UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`), and
presigned URLs.

## TUS resumable uploads

For chunked uploads of large files, Trex Storage implements the
[TUS protocol](https://tus.io/) at:

```
${BASE_PATH}/storage/v1/upload/resumable
```

Use a TUS-compatible client (e.g. `tus-js-client`).

## Image transformation

Disabled by default. Enable via the management API:

```bash
curl -X PATCH http://localhost:8001/trex/v1/projects/.../config/storage \
  -H "Authorization: Bearer trex_..." \
  -H "Content-Type: application/json" \
  -d '{"features":{"imageTransformation":{"enabled":true}}}'
```

When enabled, `?width=…&height=…&resize=cover|contain&quality=…&format=webp`
query parameters on object download URLs trigger transformation through the
imgproxy backend.

## CDN integration

If `STORAGE_BACKEND=s3` is configured to point at an external S3 / R2
backend, the storage plugin can serve objects directly from the upstream CDN
URL via a `?cdn=true` query parameter. Local backends ignore this flag.

## Health check

```
GET ${BASE_PATH}/storage/v1/status
```

Returns `200 OK` with backend / database health.

## Configuration

The storage plugin reads its config from the management API
(`/v1/projects/.../config/storage`) and from environment variables on the
Trex container. Key vars (passed through to the storage worker):

| Variable | Description |
|----------|-------------|
| `STORAGE_BACKEND` | `file` (local disk) or `s3` (external bucket). |
| `STORAGE_FILE_BACKEND_PATH` | Disk path for `file` backend. |
| `STORAGE_S3_BUCKET` | Upstream bucket name for `s3` backend. |
| `STORAGE_S3_ENDPOINT` | Upstream S3 endpoint (e.g. R2, MinIO). |
| `STORAGE_S3_REGION` | Upstream region. |
| `FILE_SIZE_LIMIT` | Per-object byte cap (default 50 MiB). |

See `plugins/storage/supabase-storage/.env.sample` in the repo for the full
list.

## Compatibility

The plugin is API-compatible with upstream Supabase Storage at the wire level.
The Supabase JS client (`@supabase/supabase-js` → `.storage.from(...)`)
works unmodified against a Trex deployment.

## Next steps

- [APIs → Edge Functions](functions) — invoke edge functions that read or
  write storage objects.
- [APIs → Auth](auth) — the JWT model that storage authorization runs on.
