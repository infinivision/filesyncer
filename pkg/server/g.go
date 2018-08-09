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

func initG(cfg *Cfg, cmdb *CmdbApi, imgCh chan<- ImgMsg) {
	bucketName = cfg.Oss.BucketName
	initFileManager(cfg.Retry, cmdb, imgCh)
	initObjectStore(cfg.Oss)
}

func initFileManager(cfg RetryCfg, cmdb *CmdbApi, imgCh chan<- ImgMsg) {
	fileMgr = newFileManager(cfg, cmdb, imgCh)
}

func initObjectStore(cfg OssCfg) {
	var err error
	objectStore, err = oss.NewMinioStorage(cfg.Server, cfg.Key, cfg.SecretKey, cfg.UseSSL)
	if err != nil {
		log.Fatalf("init oss store failed with %+v, errors: %+v", cfg, err)
	}
}
