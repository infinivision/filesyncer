// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/infinivision/filesyncer/pkg/version"
)

var (
	addr    = flag.String("addr", "127.0.0.1:80", "Addr: file server listen at")
	ossAddr = flag.String("addr-oss", "127.0.0.1:9000", "Addr: oss server")
	pprof   = flag.String("addr-pprof", "", "Addr: pprof http server address")

	ossKey       = flag.String("oss-key", "PA1H2FSS60OKVFHD8D4Z", "oss client access key")
	ossSecretKey = flag.String("oss-secret-key", "Ad2VqYkv4R4KIDnk5GRMn09mOCAUv535zKe8R6oh", "oss client access secret key")
	ossUseSSL    = flag.Bool("oss-ssl", false, "oss client use ssl")
	ossBucket    = flag.String("oss-bucket", "images", "oss bucket name")

	sessionTimeoutSec   = flag.Int("timeout-session", 30, "Timeout(sec): timeout that not received msg from client")
	retryMaxRetryTimes  = flag.Int("retry-max-times", 3, "Max: retry times of put file to the oss server")
	retryIntervalSec    = flag.Int("retry-interval", 10, "Interval(sec): interval seconds between two retries")
	retryIntervalFactor = flag.Int("retry-interval-factor", 2, "Factor: retry interval factor")

	showVer = flag.Bool("version", false, "Show version and quit.")
)

func main() {
	flag.Parse()

	if *showVer {
		version.ShowVersion()
		os.Exit(0)
	}

	log.InitLog()

	if "" != *pprof {
		log.Infof("start pprof at %s", *pprof)
		go func() {
			log.Fatalf("start pprof failed, errors: %+v",
				http.ListenAndServe(*pprof, nil))
		}()
	}

	s := server.NewFileServer(parseCfg(), nil)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go s.Start()

	for {
		sig := <-sc
		retVal := 0
		if sig != syscall.SIGTERM {
			retVal = 1
		}
		log.Infof("exit with signal=<%d>.", sig)
		s.Stop()
		log.Infof(" bye :-).")
		os.Exit(retVal)
	}
}

func parseCfg() *server.Cfg {
	cfg := &server.Cfg{}
	cfg.Addr = *addr
	cfg.SessionTimeout = time.Second * time.Duration(*sessionTimeoutSec)

	cfg.Oss.Server = *ossAddr
	cfg.Oss.Key = *ossKey
	cfg.Oss.SecretKey = *ossSecretKey
	cfg.Oss.UseSSL = *ossUseSSL
	cfg.Oss.BucketName = *ossBucket

	cfg.Retry.MaxTimes = *retryMaxRetryTimes
	cfg.Retry.RetryFactor = *retryIntervalFactor
	cfg.Retry.RetryInterval = time.Second * time.Duration(*retryIntervalSec)

	return cfg
}
