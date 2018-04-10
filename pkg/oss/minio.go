package oss

import (
	"fmt"
	"io"

	"github.com/minio/minio-go"
)

type minioStorage struct {
	cli *minio.Client
}

// NewMinioStorage returns a minio implementation
func NewMinioStorage(addr string, key, secretKey string, useSSL bool) (ObjectStorage, error) {
	cli, err := minio.New(addr, key, secretKey, useSSL)
	if err != nil {
		return nil, err
	}

	return &minioStorage{
		cli: cli,
	}, nil
}

func (store *minioStorage) PutObject(bucketName, objectName string, reader io.Reader, objectSize int64) error {
	opts := minio.PutObjectOptions{
		ContentType: "image/png",
	}

	n, err := store.cli.PutObject(bucketName, objectName, reader, objectSize, opts)
	if err != nil {
		return err
	}

	if n != objectSize {
		return fmt.Errorf("minio put object expect %d but %d", objectSize, n)
	}

	return nil
}
