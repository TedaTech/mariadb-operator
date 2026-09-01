package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/backup"
)

func TestBackupCompressors(t *testing.T) {
	content := "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
	processor := backup.NewLogicalBackupProcessor()
	logger := logr.Discard()

	tests := []struct {
		name            string
		newCompressorFn func(basePath string, getUncompressedFilename GetBackupUncompressedFilenameFn, logger logr.Logger) BackupCompressor
		fileName        string
	}{
		{
			name:            "nop",
			newCompressorFn: NewNopBackupCompressor,
			fileName:        "backup.2023-12-18T16:14:00Z.sql",
		},
		{
			name:            "gzip",
			newCompressorFn: NewGzipBackupCompressor,
			fileName:        "backup.2023-12-18T16:14:00Z.sql.gz",
		},
		{
			name:            "bzip2",
			newCompressorFn: NewBzip2BackupCompressor,
			fileName:        "backup.2023-12-18T16:14:00Z.sql.bz2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "backup_test")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			compressor := tt.newCompressorFn(dir, processor.GetUncompressedBackupFile, logger)

			filePath := filepath.Join(dir, tt.fileName)
			if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
				t.Fatalf("Failed to write test file: %v", err)
			}

			if err := compressor.Compress(filePath); err != nil {
				t.Fatalf("Failed to compress test file: %v", err)
			}
			decompressedFileName, err := compressor.Decompress(filePath)
			if err != nil {
				t.Fatalf("Failed to decompress test file: %v", err)
			}

			decompressedContent, err := os.ReadFile(decompressedFileName)
			if err != nil {
				t.Fatalf("Failed to read decompressed file: %v", err)
			}
			if string(decompressedContent) != content {
				t.Errorf("Decompressed content does not match original content:\nGot: %s\nWant: %s", decompressedContent, content)
			}
		})
	}
}

// TestIsAlreadyCompressed pins the magic-byte sniff used to make Compress
// idempotent. xbstream output must never be mistaken for an already-compressed
// file, or a genuine plain backup would be skipped.
func TestIsAlreadyCompressed(t *testing.T) {
	plain := []byte("XBSTCK01")
	for _, tt := range []struct {
		name    string
		content []byte
		want    bool
	}{
		{
			name:    "plain xbstream header is not compressed",
			content: plain,
			want:    false,
		},
		{
			name:    "short plain file is not compressed",
			content: []byte{0x1f},
			want:    false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "backup_test")
			if err != nil {
				t.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			filePath := filepath.Join(dir, "backup.xb")
			if err := os.WriteFile(filePath, tt.content, 0644); err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}
			got, err := isAlreadyCompressed(filePath)
			if err != nil {
				t.Fatalf("isAlreadyCompressed() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("isAlreadyCompressed() = %v, want %v", got, tt.want)
			}
		})
	}

	for _, tt := range []struct {
		name       string
		compressor Compressor
	}{
		{name: "gzip", compressor: &GzipCompressor{}},
		{name: "bzip2", compressor: &Bzip2Compressor{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "backup_test")
			if err != nil {
				t.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			filePath := filepath.Join(dir, "backup.xb")
			file, err := os.Create(filePath)
			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}
			if err := tt.compressor.Compress(context.Background(), file, bytes.NewReader(plain)); err != nil {
				t.Fatalf("failed to compress fixture: %v", err)
			}
			file.Close()

			got, err := isAlreadyCompressed(filePath)
			if err != nil {
				t.Fatalf("isAlreadyCompressed() error = %v", err)
			}
			if !got {
				t.Errorf("isAlreadyCompressed() = false, want true for a %s stream", tt.name)
			}
		})
	}
}

// TestCompressFileIdempotent covers the failure mode that shipped a corrupted
// backup to object storage on 2026-08-18: a retried backup command re-ran
// Compress over the file the previous attempt had already replaced in place,
// producing gzip(gzip(xbstream)) that restore surfaced as 'wrong chunk magic'.
// The second Compress must be a no-op, leaving a single-layer stream.
func TestCompressFileIdempotent(t *testing.T) {
	content := []byte("XBSTCK01Lorem ipsum dolor sit amet")
	logger := logr.Discard()

	dir, err := os.MkdirTemp("", "backup_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	filePath := filepath.Join(dir, "backup.xb")
	compressor := &GzipCompressor{}

	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	if err := compressFile(dir, filepath.Base(filePath), logger, compressor); err != nil {
		t.Fatalf("first Compress() error = %v", err)
	}

	// The command retry: same path, already compressed.
	if err := compressFile(dir, filepath.Base(filePath), logger, compressor); err != nil {
		t.Fatalf("second Compress() error = %v", err)
	}

	// A single unwrap must yield the original content; a double-compressed
	// object would unwrap to another gzip stream instead.
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open compressed file: %v", err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("file is not single-layer gzip after two Compress calls: %v", err)
	}
	unwrapped, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read gzip stream: %v", err)
	}
	if !bytes.Equal(unwrapped, content) {
		t.Errorf("unwrapped content does not match original after two Compress calls:\nGot: %q\nWant: %q", unwrapped, content)
	}
}
