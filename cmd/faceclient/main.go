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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/monitor"
	"github.com/infinivision/filesyncer/pkg/version"
)

var (
	discovery        = flag.String("discovery", "", "Get real addresses of file server from discovery server.")
	backupServers    = flag.String("backup", "upload.ai-cloud.io:8090", "Backup servers if discovery server is not available, multi server split by ','.")
	target           = flag.String("target", "/opt/dev_keeper/faces", "Dir: monitor target dir.")
	chunk            = flag.Int64("chunk", 1024, "Chunk size: bytes")
	limitTraffic     = flag.Int64("limit-traffic", 512, "Limit(KB): upload traffic limit.")
	refreshInterval  = flag.Int("refresh-interval", 86400, "Interval(sec): Refresh file servers.")
	monitorInterval  = flag.Int("monitor-interval", 10, "Interval(sec): monitor the target dir.")
	batchFetch       = flag.Int("batch-fetch", 10, "Batch: fetch number Of files in target each.")
	retriesPerServer = flag.Int("retries-per-server", 3, "Max retries send per server.")
	retriesInterval  = flag.Int("retries-interval", 100, "Interval(ms): retry interval in ms.")
	disableRetry     = flag.Bool("retry-disable", false, "Disable retry.")
	timeoutRead      = flag.Int("timeout-read", 30, "Timeout(sec): timeout read from server.")
	timeoutWrite     = flag.Int("timeout-write", 15, "Timeout(sec): timeout write heartbeat msg to server.")
	timeoutConnect   = flag.Int("timeout-connect", 10, "Timeout(sec): timeout connect to server.")
	showVer          = flag.Bool("version", false, "Show version and quit.")
)

func main() {
	flag.Parse()

	if *showVer {
		version.ShowVersion()
		os.Exit(0)
	}

	log.InitLog()

	s := monitor.NewMonitor(parseCfg())
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

func parseCfg() *monitor.Cfg {
	if *target == "" {
		log.Fatalf("the monitor dir must set")
	}

	if *backupServers == "" {
		log.Fatalf("the backup servers must set")
	}

	var mac string
	if mac = GetNicMAC(); len(mac) == 0 {
		log.Fatalf("failed to determine MAC")
	} else {
		log.Infof("MAC: %s", mac)
	}

	cfg := &monitor.Cfg{}
	cfg.Target = *target
	cfg.ID = mac
	cfg.Discovery = *discovery
	cfg.Backups = strings.Split(*backupServers, ",")
	cfg.MonitorInterval = time.Second * time.Duration(*monitorInterval)
	cfg.BatchFetch = *batchFetch
	cfg.RefreshInterval = time.Second * time.Duration(*refreshInterval)
	cfg.LimitTraffic = *limitTraffic * 1024
	cfg.Chunk = *chunk
	cfg.TimeoutRead = time.Second * time.Duration(*timeoutRead)
	cfg.TimeoutWrite = time.Second * time.Duration(*timeoutWrite)
	cfg.TimeoutConnect = time.Second * time.Duration(*timeoutConnect)
	cfg.DiableRetry = *disableRetry
	cfg.RetriesInterval = time.Second * time.Duration(*retriesInterval)
	cfg.RetriesPerServer = *retriesPerServer

	return cfg
}
