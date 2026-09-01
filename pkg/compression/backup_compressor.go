package compression

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
)

var (
	gzipMagicBytes  = []byte{0x1f, 0x8b}
	bzip2MagicBytes = []byte{'B', 'Z', 'h'}
)

type BackupCompressor interface {
	Compress(fileName string) error
	Decompress(fileName string) (string, error)
}

type GetBackupUncompressedFilenameFn func(compressedFilename string) (string, error)

func NewBackupCompressor(calg mariadbv1alpha1.CompressAlgorithm, basePath string,
	getUncompressedFilename GetBackupUncompressedFilenameFn, logger logr.Logger) (BackupCompressor, error) {
	switch calg {
	case mariadbv1alpha1.CompressNone:
		return NewNopBackupCompressor(basePath, getUncompressedFilename, logger.WithName("nop-compressor")), nil
	case mariadbv1alpha1.CompressGzip:
		return NewGzipBackupCompressor(basePath, getUncompressedFilename, logger.WithName("gzip-compressor")), nil
	case mariadbv1alpha1.CompressBzip2:
		return NewBzip2BackupCompressor(basePath, getUncompressedFilename, logger.WithName("bzip2-compressor")), nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %v", calg)
	}
}

type NopBackupCompressor struct {
	basePath string
}

func NewNopBackupCompressor(basePath string, getUncompressedFilename GetBackupUncompressedFilenameFn, logger logr.Logger) BackupCompressor {
	return &NopBackupCompressor{
		basePath: basePath,
	}
}

func (c *NopBackupCompressor) Compress(fileName string) error {
	return nil
}

func (c *NopBackupCompressor) Decompress(fileName string) (string, error) {
	return getFilePath(c.basePath, fileName), nil
}

type GzipBackupCompressor struct {
	compressor              *GzipCompressor
	basePath                string
	getUncompressedFilename GetBackupUncompressedFilenameFn
	logger                  logr.Logger
}

func NewGzipBackupCompressor(basePath string, getUncompressedFilename GetBackupUncompressedFilenameFn,
	logger logr.Logger) BackupCompressor {
	return &GzipBackupCompressor{
		compressor:              &GzipCompressor{},
		basePath:                basePath,
		getUncompressedFilename: getUncompressedFilename,
		logger:                  logger,
	}
}

func (c *GzipBackupCompressor) Compress(fileName string) error {
	return compressFile(c.basePath, fileName, c.logger, c.compressor)
}

func (c *GzipBackupCompressor) Decompress(fileName string) (string, error) {
	return decompressFile(c.basePath, fileName, c.logger, c.getUncompressedFilename, c.compressor)
}

type Bzip2BackupCompressor struct {
	compressor              *Bzip2Compressor
	basePath                string
	getUncompressedFilename GetBackupUncompressedFilenameFn
	logger                  logr.Logger
}

func NewBzip2BackupCompressor(basePath string, getUncompressedFilename GetBackupUncompressedFilenameFn,
	logger logr.Logger) BackupCompressor {
	return &Bzip2BackupCompressor{
		compressor:              &Bzip2Compressor{},
		basePath:                basePath,
		getUncompressedFilename: getUncompressedFilename,
		logger:                  logger,
	}
}

func (c *Bzip2BackupCompressor) Compress(fileName string) error {
	return compressFile(c.basePath, fileName, c.logger, c.compressor)
}

func (c *Bzip2BackupCompressor) Decompress(fileName string) (string, error) {
	return decompressFile(c.basePath, fileName, c.logger, c.getUncompressedFilename, c.compressor)
}

// isAlreadyCompressed reports whether filePath already holds a gzip or bzip2
// stream. compressFile replaces the plain file in place, so when the backup
// command is retried (RestartPolicy: OnFailure restarts the container, which
// re-runs Compress over the same file) it re-encounters the stream it produced
// on the previous attempt. Compressing that would ship gzip(gzip(xbstream)) to
// object storage; restore unwraps a single layer, so mariadb-backup would fail
// with 'wrong chunk magic at offset 0x0'. A plain mariadb-backup stream starts
// with its own header, never either magic, so a genuine plain file is never
// skipped.
func isAlreadyCompressed(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	head := make([]byte, 3)
	n, err := io.ReadFull(file, head)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return false, err
	}
	head = head[:n]
	return bytes.HasPrefix(head, gzipMagicBytes) || bytes.HasPrefix(head, bzip2MagicBytes), nil
}

func compressFile(path, fileName string, logger logr.Logger, compressor Compressor) error {
	filePath := getFilePath(path, fileName)
	logger.Info("compressing file", "file", filePath)

	alreadyCompressed, err := isAlreadyCompressed(filePath)
	if err != nil {
		return err
	}
	if alreadyCompressed {
		logger.V(1).Info("file already compressed, skipping", "file", filePath)
		return nil
	}

	compressedFilePath := filePath + ".tmp"

	// compressedFilePath must be closed before renaming. See: https://github.com/mariadb-operator/mariadb-operator/issues/1007
	if err := func() error {
		plainFile, err := os.Open(filePath)
		if err != nil {
			return err
		}
		defer plainFile.Close()

		compressedFile, err := os.Create(compressedFilePath)
		if err != nil {
			return err
		}
		defer compressedFile.Close()

		// @PERF: Potential improvement here if we want this to be cancellable, can change to Background if we don't want to
		return compressor.Compress(context.TODO(), compressedFile, plainFile)
	}(); err != nil {
		var errBundle *multierror.Error
		errBundle = multierror.Append(errBundle, err)

		if err := os.Remove(compressedFilePath); err != nil && !os.IsNotExist(err) {
			errBundle = multierror.Append(errBundle, err)
		}
		return errBundle
	}

	if err := os.Remove(filePath); err != nil {
		return err
	}
	if err := os.Rename(compressedFilePath, filePath); err != nil {
		return err
	}
	return nil
}

func decompressFile(path, fileName string, logger logr.Logger, getUncompressedFilename GetBackupUncompressedFilenameFn,
	compressor Compressor) (string, error) {
	filePath := getFilePath(path, fileName)
	logger.Info("decompressing file", "file", filePath)

	compressedFile, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer compressedFile.Close()

	plainFileName, err := getUncompressedFilename(fileName)
	if err != nil {
		return "", err
	}
	plainFilePath := getFilePath(path, plainFileName)
	plainFile, err := os.Create(plainFilePath)
	if err != nil {
		return "", err
	}
	defer plainFile.Close()

	if err := compressor.Decompress(context.TODO(), plainFile, compressedFile); err != nil {
		return "", err
	}

	return plainFilePath, nil
}

func getFilePath(basePath, fileName string) string {
	if filepath.IsAbs(fileName) {
		return fileName
	}
	return filepath.Join(basePath, fileName)
}
