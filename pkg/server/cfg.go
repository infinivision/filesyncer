package server

import (
	"time"
)

// Cfg the file server cfg
type Cfg struct {
	Addr           string
	SessionTimeout time.Duration
	Oss            OssCfg
	Retry          RetryCfg
	EurekaAddr     string
	EurekaApp      string
}

// OssCfg oss cfg
type OssCfg struct {
	Server     string
	Key        string
	SecretKey  string
	UseSSL     bool
	BucketName string
}

// RetryCfg retry cfg
type RetryCfg struct {
	MaxTimes      int
	RetryInterval time.Duration
	RetryFactor   int
}
