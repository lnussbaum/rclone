# High-Performance Nextcloud Uploads (Pilot Implementation)

> **Status: AI-generated proof-of-concept (POC).**
>
> This is an experimental, AI-generated implementation. It has NOT been
> reviewed by the rclone maintainers and is NOT part of upstream rclone. It
> is provided for evaluation only. Use it against real data/testing
> environments at your own risk, and understand every line before relying on
> it. It is **not** ready for a pull request as-is.

This document explains everything that has been changed relative to
`origin/master` in this repository, how to build the custom rclone, and how
to use the new feature.

---

## 1. What problem this solves

rclone's existing Nextcloud *chunked upload* support uses the **V1
(streaming)** protocol:

1. The file is cut into chunks named by byte offset (e.g. `000000000000000-0000010485759`).
2. The chunks are uploaded into a temporary `uploads/` directory.
3. On completion, rclone issues a `MOVE` that makes the server **read back
   and re-stream the entire file** through the Nextcloud PHP application to
   assemble it.

For large files this V1 merge step downloads the whole file out of the
object store and re-uploads it, which is slow and wastes bandwidth.

The **V2 (multipart)** protocol can avoid that: chunks are uploaded as real
multipart parts and the underlying object store (S3/Azure) assembles them
server-side, without the file ever being streamed through PHP.

The pilot adds optional support for V2.

---

## 2. All changes since `origin/master`

All changes are confined to the webdav backend (`backend/webdav/`):

| File | What changed |
|------|--------------|
| `backend/webdav/webdav.go` | New exported option `nextcloud_chunked_upload_v2` (default **on**), new `Options.ChunkedUploadV2` field, new `Fs` state fields, and a `ChunkWriterDoesntSeek` feature flag. |
| `backend/webdav/chunking.go` | V2 probe, cached detection, protocol selection, and V1/V2 branching throughout the upload pipeline. |
| `backend/webdav/chunking_internal_test.go` | **New** unit test for the V2 part-limit / fallback decision logic. |

### 2.1 New config option

```
nextcloud_chunked_upload_v2 (bool)     Default: true  (advanced)
```

- **V2 (multipart) is now the default** for Nextcloud chunked uploads.
- Set it to `false` to force the legacy **V1 (streaming)** protocol.
- It is an **advanced** option, set per-remote via `rclone config`.

### 2.2 Visibility / detection (new in `chunking.go`)

Because a Nextcloud server **silently falls back to a plain upload directory**
when V2 protocol requests are not backed by a multipart-capable object store,
rclone can't trust the option blindly. It therefore:

- **Probes** the server once per process (on the first chunked transfer) by
  starting a throwaway V2 upload session and checking for the `.target` file
  that a real V2 session creates.
- **Caches** the probe result (`chunkedUploadV2Wanted/Checked/V2`) so it only
  happens once.
- **Errors**, rather than silently downgrading, if the server does not
  actually support V2.

> **Design decision:** previously the probe logged a warning and transparently
> fell back to V1. In this pilot, if V2 is requested (the default) but the
> server does not support it, the upload **fails with a clear error** telling
> you to either configure the server forV2 or set
> `nextcloud_chunked_upload_v2 = false`. There is no automatic fallback; the
> only way to get V1 is to explicitly opt out.

### 2.3 V2 vs V1 behaviour (in `chunking.go`)

The changes branch every stage of the upload between the two protocols:

| Stage | V1 (streaming) | V2 (multipart) |
|-------|----------------|----------------|
| Start session `MKCOL` | no `Destination` header | `Destination` header to final target |
| Part file naming | byte offset: `%015d-%015d` | 1-based part number: `%d` |
| Each part `PUT` | no `Destination` header | `Destination` header |
| Part count limit | no practical limit | Nextcloud cap of **10,000 parts** |
| Assembly | server streams whole file | object store assembles parts |

- `updateChunked` (single-stream path) and the multi-threaded
  `chunkWriter` / `OpenChunkWriter` path both apply the new V2 logic.
- Because V2 numbers at most 10,000 parts, files that would exceed that limit
  (e.g. >100 GB at the default 10 MiB chunk size) are rejected with a helpful
  error suggesting a larger chunk size or disabling V2 — again, no silent
  fallback.

### 2.4 Throughput / memory tweaks

- `OpenChunkWriter` concurrency now follows the user's `--multi-thread-streams`
  setting instead of a hard-coded `4`.
- The backend now advertises `ChunkWriterDoesntSeek`. The `chunkWriter` already
  buffers each chunk in memory so it can be replayed on HTTP retry, so this
  tells the multi-thread machinery it doesn't need to pre-buffer too — halving
  peak memory per chunk.

---

## 3. Building the custom rclone

### Prerequisites

- Go (1.4+ requirement of rclone; a recent stable Go is fine).
- A network connection for module downloads on first build.

### Build from source

```bash
# from the repository root
make
```

Or, if you just want the binary without the version-embedded build step:

```bash
go build
```

The resulting binary is written as `rclone` in the repository root
(`rclone.exe` on Windows).

> Verify it's your custom build and inspect the new option:
>
> ```bash
> ./rclone version
> ./rclone help flags | grep -i nextcloud
> ```

### Run the unit tests

```bash
# Unit tests for the webdav backend package
go test ./backend/webdav/ -v

# just the new V2 decision logic
go test ./backend/webdav/ -run TestFsUseChunkedUploadV2 -v

# full rclone unit suite (no cloud credentials needed)
make quicktest
```

### Lint (if you have golangci-lint)

```bash
golangci-lint run ./backend/webdav/
```

---

## 4. How to use it

### 4.1 Configure a Nextcloud remote

Run configuration and set your endpoint / vendor to Nextcloud as usual. Then
verify the new option:

```bash
rclone config
```

During setup you'll see the advanced option `nextcloud_chunked_upload_v2`
(default `true`).

You can also set it explicitly in `~/.config/rclone/rclone.conf`:

```ini
[mynextcloud]
type = webdav
url = https://cloud.example.com/remote.php/dav/files/user/
vendor = nextcloud
user = user
pass = ...encrypted...
nextcloud_chunk_size = 10Mi
nextcloud_chunked_upload_v2 = true
```

### 4.2 Upload

```bash
# Standard copy (large files trigger chunked upload)
./rclone copy /big/file.bin mynextcloud:/big/

# Force multi-threaded chunked upload
./rclone copy /big/file.bin mynextcloud:/big/ \
    --multi-thread-streams 8 \
    --multi-thread-cutoff 100M
```

### 4.3 Switching back to V1

If your server doesn't support V2 (or you hit the 10,000-part error), disable
it:

```ini
nextcloud_chunked_upload_v2 = false
```

```
./rclone copy /big/file.bin mynextcloud:/big/ --webdav-nextcloud-chunked-upload-v2=false
```

### 4.4 What to expect

- On the first chunked transfer rclone logs which protocol was detected, e.g.
  `Using Nextcloud V2 (multipart) chunked upload protocol`.
- If V2 is default-enabled but the server does not support it, the upload
  **errors** instead of silently downgrading.
- For files exceeding the 10,000-part bound, rclone errors and recommends a
  larger `nextcloud_chunk_size` or disabling V2.

---

## 5. Requirements for V2 to work on the server

The V2 protocol is only available on a Nextcloud server with all of:

- A **multipart-capable object store**: S3 or Azure (not the local filesystem
  primary storage).
- A **distributed cache**: Redis or Memcached.

Without these, the server will not create the `.target` session file and
rclone's probe will report V2 unsupported.

---

## 6. Known limitations / caveats

- **POC, not upstream.** Expect the code, option naming, and behaviour to
  change. It has not been tested against real Nextcloud servers or the full
  backend integration suite (`fstest`) in this pilot.
- **No automatic fallback.** If V2 is requested but unavailable, uploads fail
  rather than quietly using V1. Opt out explicitly if you need resilience.
- The probe performs a small amount of throwaway server state
  (create/delete of a probe upload session) on the first transfer.
- The 10,000-part cap is Nextcloud's, and this implementation hard-codes it.
