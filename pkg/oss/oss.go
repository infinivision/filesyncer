package oss

import (
	"io"
)

// ObjectStorage object storage
type ObjectStorage interface {
	PutObject(bucketName, objectName string, reader io.Reader, objectSize int64) error
}
