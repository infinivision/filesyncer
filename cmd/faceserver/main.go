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
	"context"
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr       = flag.String("addr", "127.0.0.1:80", "Addr: file server listen at")
	ossAddr    = flag.String("addr-oss", "127.0.0.1:9000", "Addr: oss server")
	metricAddr = flag.String("metric-addr", ":8000", "The address to listen on for metric pull requests.")
	pprof      = flag.String("addr-pprof", "", "Addr: pprof http server address")

	ossKey       = flag.String("oss-key", "PA1H2FSS60OKVFHD8D4Z", "oss client access key")
	ossSecretKey = flag.String("oss-secret-key", "Ad2VqYkv4R4KIDnk5GRMn09mOCAUv535zKe8R6oh", "oss client access secret key")
	ossUseSSL    = flag.Bool("oss-ssl", false, "oss client use ssl")
	ossBucket    = flag.String("oss-bucket", "images", "oss bucket name")

	sessionTimeoutSec   = flag.Int("timeout-session", 30, "Timeout(sec): timeout that not received msg from client")
	retryMaxRetryTimes  = flag.Int("retry-max-times", 3, "Max: retry times of put file to the oss server")
	retryIntervalSec    = flag.Int("retry-interval", 10, "Interval(sec): interval seconds between two retries")
	retryIntervalFactor = flag.Int("retry-interval-factor", 2, "Factor: retry interval factor")

	predictServURL = flag.String("predict-serv-url", "http://127.0.0.1/r50/predict", "Face predict server url")
	ageServURL     = flag.String("age-serv-url", "http://127.0.0.1/ga/predict", "Face age and gender predict server url")
	nsqlookupdURLs = flag.String("nsqlookupd-urls", "http://127.0.0.1:4161", "List of URLs of nsqlookupd.")
	topic          = flag.String("topic", "visits", "NSQ topic.")

	identifyBatchSize = flag.Int("identify-batch-size", 200, "Batch size of search vectodb.")
	identifyDisThr    = flag.Float64("identify-distance-threshold", 0.05, "Distance threshold of search vectodb.")
	identifyFlatThr   = flag.Int("identify-flat-threshold", 1000, "Allowed max flat size when udpate vectodb index.")
	identifyWorkDir   = flag.String("identify-work-dir", "/data", "Work directory of vectodb.")
	identifyDim       = flag.Int("identify-dim", 512, "Dimension of vectors inside vectodb.")

	adminAddr     = flag.String("admin-addr", "127.0.0.1:8080", "admin database host and port.")
	adminUsername = flag.String("admin-username", "username", "admin database username.")
	adminPassword = flag.String("admin-password", "password", "admin database password.")
	adminDatabase = flag.String("admin-database", "iot", "admin database.")
	adminTable    = flag.String("admin-table", "iot_terminal", "admin database table.")

	showVer = flag.Bool("version", false, "Show version and quit.")
)

type VecMsg struct {
	Shop int64
	Img  []byte
	Vec  []float32
}

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

	// Expose the registered metrics via HTTP.
	log.Infof("exposed metric at %s", *metricAddr)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(*metricAddr, nil))
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	imgCh := make(chan server.ImgMsg, 10000)
	vecCh := make(chan VecMsg, 10000)
	visitCh := make(chan *Visit, 10000)
	s := server.NewFileServer(parseCfg(), imgCh)
	pred := NewPredictor(*predictServURL, imgCh, vecCh, 3)

	iden := NewIdentifier(vecCh, visitCh, 3, *identifyBatchSize, float32(*identifyDisThr), *identifyFlatThr, *identifyWorkDir, *identifyDim, *ageServURL)
	recd, err := NewRecorder(*nsqlookupdURLs, *topic, visitCh)
	if err != nil {
		log.Fatalf("+v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go s.Start()
	go pred.Serve(ctx)
	go iden.Serve(ctx)
	go recd.Serve(ctx)

	for {
		sig := <-sc
		retVal := 0
		if sig != syscall.SIGTERM {
			retVal = 1
		}
		log.Infof("exit with signal=<%d>.", sig)
		s.Stop()
		cancel()
		time.Sleep(5 * time.Second)
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

	cfg.Admin.Addr = *adminAddr
	cfg.Admin.Username = *adminUsername
	cfg.Admin.Password = *adminPassword
	cfg.Admin.Database = *adminDatabase
	cfg.Admin.Table = *adminTable
	return cfg
}
