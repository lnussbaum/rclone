package webdav

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFsUseChunkedUploadV2(t *testing.T) {
	ctx := context.Background()
	const chunkSize = 10 * 1024 * 1024

	f := &Fs{chunkedUploadV2Wanted: true, chunkedUploadV2: true, chunkedUploadV2Checked: true}

	// Disabled
	got, err := (&Fs{}).useChunkedUploadV2(ctx, 100, chunkSize)
	require.NoError(t, err)
	assert.False(t, got)

	// Small file within the part limit
	got, err = f.useChunkedUploadV2(ctx, 100, chunkSize)
	require.NoError(t, err)
	assert.True(t, got)
	// Exactly the 10000 part limit (100GB at 10MiB)
	got, err = f.useChunkedUploadV2(ctx, int64(maxChunkedUploadV2Parts)*chunkSize, chunkSize)
	require.NoError(t, err)
	assert.True(t, got)
	// Over the part limit errors rather than silently falling back to V1
	_, err = f.useChunkedUploadV2(ctx, int64(maxChunkedUploadV2Parts)*chunkSize+1, chunkSize)
	require.Error(t, err)

	// Invalid sizes / chunk sizes errors rather than silently falling back to V1
	_, err = f.useChunkedUploadV2(ctx, -1, chunkSize)
	require.Error(t, err)
	_, err = f.useChunkedUploadV2(ctx, 100, 0)
	require.Error(t, err)
	_, err = f.useChunkedUploadV2(ctx, 100, -1)
	require.Error(t, err)

	// V2 wanted but known unsupported errors rather than silently falling back to V1
	f2 := &Fs{chunkedUploadV2Wanted: true, chunkedUploadV2: false, chunkedUploadV2Checked: true}
	_, err = f2.useChunkedUploadV2(ctx, 100, chunkSize)
	require.Error(t, err)
}
