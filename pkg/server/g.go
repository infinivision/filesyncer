package server

import (
	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/oss"
)

var (
	fileMgr     *fileManager
	objectStore oss.ObjectStorage
	bucketName  string
)

func initG(cfg *Cfg, imgCh chan<- ImgMsg) {
	bucketName = cfg.Oss.BucketName
	initFileManager(cfg.Retry, imgCh)
	initObjectStore(cfg.Oss)
}

func initFileManager(cfg RetryCfg, imgCh chan<- ImgMsg) {
	fileMgr = newFileManager(cfg, imgCh)
}

func initObjectStore(cfg OssCfg) {
	var err error
	objectStore, err = oss.NewMinioStorage(cfg.Server, cfg.Key, cfg.SecretKey, cfg.UseSSL)
	if err != nil {
		log.Fatalf("init oss store failed with %+v, errors: %+v", cfg, err)
	}
}
