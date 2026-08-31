package webdav

/*
	chunked update for Nextcloud
	see https://docs.nextcloud.com/server/20/developer_manual/client_apis/WebDAV/chunking.html
*/

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/lib/rest"
)

func (f *Fs) shouldRetryChunkMerge(ctx context.Context, resp *http.Response, err error, sleepTime *time.Duration, wasLocked *bool) (bool, error) {
	// Not found. Can be returned by NextCloud when merging chunks of an upload.
	if resp != nil && resp.StatusCode == 404 {
		if *wasLocked {
			// Assume a 404 error after we've received a 423 error is actually a success
			return false, nil
		}
		return true, err
	}

	// 423 LOCKED
	if resp != nil && resp.StatusCode == 423 {
		*wasLocked = true
		sleep := *sleepTime
		// Increase the wait on the next attempt so the server has longer
		// to finish merging the chunks before giving up.
		*sleepTime *= 2
		// Hand the wait to the pacer rather than blocking here, so that
		// RetryAfter is honoured by the calculator, the retry budget
		// (--low-level-retries) is respected and the goroutine is not held
		// hostage while other work could proceed.
		fs.Logf(f, "Waiting %v for chunks to be merged after 423 error", sleep)
		return true, pacer.RetryAfterError(fmt.Errorf("merging the uploaded chunks failed with 423 LOCKED. This usually happens when the chunks merging is still in progress on NextCloud, but it may also indicate a failed transfer: %w", err), sleep)
	}

	return f.shouldRetry(ctx, resp, err)
}

// set the chunk size for testing
func (f *Fs) setUploadChunkSize(cs fs.SizeSuffix) (old fs.SizeSuffix, err error) {
	old, f.opt.ChunkSize = f.opt.ChunkSize, cs
	return
}

func (o *Object) getChunksUploadDir() (string, error) {
	hasher := md5.New()
	_, err := hasher.Write([]byte(o.filePath()))
	if err != nil {
		return "", fmt.Errorf("chunked upload couldn't hash URL: %w", err)
	}
	uploadDir := "rclone-chunked-upload-" + hex.EncodeToString(hasher.Sum(nil))
	return uploadDir, nil
}

func (f *Fs) getChunksUploadURL() (string, error) {
	submatch := nextCloudURLRegex.FindStringSubmatch(f.endpointURL)
	if submatch == nil {
		return "", errors.New("the remote url looks incorrect. Note that nextcloud chunked uploads require you to use the /dav/files/USER endpoint instead of /webdav. Please check 'rclone config show remotename' to verify that the url field ends in /dav/files/USERNAME")
	}

	baseURL, user := submatch[1], submatch[2]
	chunksUploadURL := fmt.Sprintf("%s/dav/uploads/%s/", baseURL, user)

	return chunksUploadURL, nil
}

func (o *Object) shouldUseChunkedUpload(src fs.ObjectInfo) bool {
	return o.fs.canChunk && o.fs.opt.ChunkSize > 0 && src.Size() > int64(o.fs.opt.ChunkSize)
}

func (o *Object) updateChunked(ctx context.Context, in0 io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	var uploadDir string

	// see https://docs.nextcloud.com/server/24/developer_manual/client_apis/WebDAV/chunking.html#starting-a-chunked-upload
	uploadDir, err = o.createChunksUploadDirectory(ctx)
	if err != nil {
		return err
	}

	partObj := &Object{
		fs: o.fs,
	}

	// see https://docs.nextcloud.com/server/24/developer_manual/client_apis/WebDAV/chunking.html#uploading-chunks
	err = o.uploadChunks(ctx, in0, src.Size(), partObj, uploadDir, options)
	if err != nil {
		return err
	}

	// see https://docs.nextcloud.com/server/24/developer_manual/client_apis/WebDAV/chunking.html#assembling-the-chunks
	err = o.mergeChunks(ctx, uploadDir, options, src)
	if err != nil {
		return err
	}

	return nil
}

func (o *Object) uploadChunks(ctx context.Context, in0 io.Reader, size int64, partObj *Object, uploadDir string, options []fs.OpenOption) error {
	chunkSize := int64(partObj.fs.opt.ChunkSize)

	// TODO: upload chunks in parallel for faster transfer speeds
	for offset := int64(0); offset < size; offset += chunkSize {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Last chunk may be smaller
		contentLength := min(size-offset, chunkSize)

		endOffset := offset + contentLength - 1

		partObj.remote = fmt.Sprintf("%s/%015d-%015d", uploadDir, offset, endOffset)
		// Enable low-level HTTP 2 retries.
		// 2022-04-28 15:59:06 ERROR : stuff/video.avi: Failed to copy: uploading chunk failed: Put "https://censored.com/remote.php/dav/uploads/Admin/rclone-chunked-upload-censored/000006113198080-000006123683840": http2: Transport: cannot retry err [http2: Transport received Server's graceful shutdown GOAWAY] after Request.Body was written; define Request.GetBody to avoid this error

		buf := make([]byte, chunkSize)
		in := readers.NewRepeatableLimitReaderBuffer(in0, buf, chunkSize)

		getBody := func() (io.ReadCloser, error) {
			// RepeatableReader{} plays well with accounting so rewinding doesn't make the progress buggy
			if _, err := in.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}

			return io.NopCloser(in), nil
		}

		err := partObj.updateSimple(ctx, in, getBody, partObj.remote, contentLength, "application/x-www-form-urlencoded", nil, o.fs.chunksUploadURL, options...)
		if err != nil {
			return fmt.Errorf("uploading chunk failed: %w", err)
		}
	}
	return nil
}

func (o *Object) createChunksUploadDirectory(ctx context.Context) (string, error) {
	uploadDir, err := o.getChunksUploadDir()
	if err != nil {
		return uploadDir, err
	}

	err = o.purgeUploadedChunks(ctx, uploadDir)
	if err != nil {
		return "", fmt.Errorf("chunked upload couldn't purge upload directory: %w", err)
	}

	opts := rest.Opts{
		Method:     "MKCOL",
		Path:       uploadDir + "/",
		NoResponse: true,
		RootURL:    o.fs.chunksUploadURL,
	}
	err = o.fs.pacer.CallNoRetry(func() (bool, error) {
		resp, err := o.fs.srv.Call(ctx, &opts)
		return o.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return "", fmt.Errorf("making upload directory failed: %w", err)
	}
	return uploadDir, err
}

func (o *Object) mergeChunks(ctx context.Context, uploadDir string, options []fs.OpenOption, src fs.ObjectInfo) error {
	var resp *http.Response

	// see https://docs.nextcloud.com/server/24/developer_manual/client_apis/WebDAV/chunking.html?highlight=chunk#assembling-the-chunks
	opts := rest.Opts{
		Method:     "MOVE",
		Path:       path.Join(uploadDir, ".file"),
		NoResponse: true,
		Options:    options,
		RootURL:    o.fs.chunksUploadURL,
	}
	destinationURL, err := rest.URLJoin(o.fs.endpoint, o.filePath())
	if err != nil {
		return fmt.Errorf("finalize chunked upload couldn't join URL: %w", err)
	}
	opts.ExtraHeaders = o.extraHeaders(ctx, src)
	opts.ExtraHeaders["Destination"] = destinationURL.String()
	sleepTime := 5 * time.Second
	wasLocked := false
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)
		return o.fs.shouldRetryChunkMerge(ctx, resp, err, &sleepTime, &wasLocked)
	})
	if err != nil {
		return fmt.Errorf("finalize chunked upload failed, destinationURL: \"%s\": %w", destinationURL, err)
	}
	return err
}

// chunkWriter uploads the chunks of a file to the Nextcloud chunked
// upload directory in parallel then assembles them with a MOVE.
type chunkWriter struct {
	fs           *Fs
	filePath     string             // final destination below the webdav root
	uploadDir    string             // chunked upload directory
	chunkSize    int64              // size of each chunk
	extraHeaders map[string]string  // headers to apply to the merge request
	options      []fs.OpenOption    // open options
}

// WriteChunk writes the chunk number to the Nextcloud chunked upload
// directory. reader contains exactly the bytes of chunk <chunkNumber>
// (already ranged by the multi-thread copy machinery).
func (w *chunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	if chunkNumber < 0 {
		return -1, fmt.Errorf("invalid chunk number provided: %v", chunkNumber)
	}
	// The reader is a ReadSeeker whose current position gives the chunk
	// contents. Determine where this chunk belongs so the server can sort
	// the chunks before assembling them (v1 chunking protocol uses byte
	// offset based names).
	offset := int64(chunkNumber) * w.chunkSize

	// Read the whole chunk to learn its real size (the last chunk may be
	// smaller than the configured chunk size).
	data, err := io.ReadAll(reader)
	if err != nil {
		return -1, fmt.Errorf("failed to read chunk %d: %w", chunkNumber, err)
	}
	contentLength := int64(len(data))
	if contentLength == 0 {
		// nothing to upload for an empty chunk
		return 0, nil
	}
	endOffset := offset + contentLength - 1

	partRemote := fmt.Sprintf("%s/%015d-%015d", w.uploadDir, offset, endOffset)

	getBody := func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	opts := rest.Opts{
		Method:        "PUT",
		Path:          partRemote,
		Body:          bytes.NewReader(data),
		GetBody:       getBody,
		NoResponse:    true,
		ContentLength: &contentLength,
		ContentType:   "application/x-www-form-urlencoded",
		Options:       w.options,
		RootURL:       w.fs.chunksUploadURL,
	}
	var resp *http.Response
	err = w.fs.pacer.Call(func() (bool, error) {
		resp, err = w.fs.srv.Call(ctx, &opts)
		return w.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return -1, fmt.Errorf("uploading chunk %d failed: %w", chunkNumber, err)
	}
	return contentLength, nil
}

// Close assembles the uploaded chunks into the final file with a MOVE.
func (w *chunkWriter) Close(ctx context.Context) error {
	var resp *http.Response

	// see https://docs.nextcloud.com/server/24/developer_manual/client_apis/WebDAV/chunking.html?highlight=chunk#assembling-the-chunks
	opts := rest.Opts{
		Method:     "MOVE",
		Path:       path.Join(w.uploadDir, ".file"),
		NoResponse: true,
		Options:    w.options,
		RootURL:    w.fs.chunksUploadURL,
	}
	destinationURL, err := rest.URLJoin(w.fs.endpoint, w.filePath)
	if err != nil {
		return fmt.Errorf("finalize chunked upload couldn't join URL: %w", err)
	}
	opts.ExtraHeaders = w.extraHeaders
	opts.ExtraHeaders["Destination"] = destinationURL.String()
	sleepTime := 5 * time.Second
	wasLocked := false
	err = w.fs.pacer.Call(func() (bool, error) {
		resp, err = w.fs.srv.Call(ctx, &opts)
		return w.fs.shouldRetryChunkMerge(ctx, resp, err, &sleepTime, &wasLocked)
	})
	if err != nil {
		return fmt.Errorf("finalize chunked upload failed, destinationURL: \"%s\": %w", destinationURL, err)
	}
	return nil
}

// Abort removes the uploaded chunks from the upload directory.
func (w *chunkWriter) Abort(ctx context.Context) error {
	opts := rest.Opts{
		Method:     "DELETE",
		Path:       w.uploadDir + "/",
		NoResponse: true,
		RootURL:    w.fs.chunksUploadURL,
	}
	var resp *http.Response
	err := w.fs.pacer.Call(func() (bool, error) {
		var err error
		resp, err = w.fs.srv.Call(ctx, &opts)
		return w.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return fmt.Errorf("abort failed to delete upload directory: %w", err)
	}
	return nil
}

// OpenChunkWriter returns the chunk size and a ChunkWriter for a
// nextcloud remote with chunking enabled.
//
// It creates the chunked upload directory (and purges any stale one)
// ready for the chunks to be written in parallel.
func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}

	uploadDir, err := o.createChunksUploadDirectory(ctx)
	if err != nil {
		return info, nil, err
	}

	// Use the hinted chunk size if provided, otherwise the configured one.
	chunkSize := int64(f.opt.ChunkSize)
	for _, option := range options {
		if x, ok := option.(*fs.ChunkOption); ok {
			chunkSize = x.ChunkSize
			break
		}
	}
	if src.Size() != -1 && src.Size() < chunkSize {
		chunkSize = src.Size()
	}

	info = fs.ChunkWriterInfo{
		ChunkSize:   chunkSize,
		Concurrency: 4,
	}

	writer = &chunkWriter{
		fs:           f,
		filePath:     o.filePath(),
		uploadDir:    uploadDir,
		chunkSize:    chunkSize,
		extraHeaders: o.extraHeaders(ctx, src),
		options:      options,
	}
	return info, writer, nil
}

var _ fs.OpenChunkWriter = (*Fs)(nil)
var _ fs.ChunkWriter = (*chunkWriter)(nil)

func (o *Object) purgeUploadedChunks(ctx context.Context, uploadDir string) error {
	// clean the upload directory if it exists (this means that a previous try didn't clean up properly).
	opts := rest.Opts{
		Method:     "DELETE",
		Path:       uploadDir + "/",
		NoResponse: true,
		RootURL:    o.fs.chunksUploadURL,
	}

	err := o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.CallXML(ctx, &opts, nil, nil)

		// directory doesn't exist, no need to purge
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return false, nil
		}

		return o.fs.shouldRetry(ctx, resp, err)
	})

	return err
}
