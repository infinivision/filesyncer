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
	"bytes"
	"context"
	"flag"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	runPprof "runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fagongzi/log"
	"github.com/go-redis/redis"
	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/infinivision/filesyncer/pkg/version"
	"github.com/infinivision/hyena/pkg/proxy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	replayAddr      = flag.String("addr-replay", "", "Addr: replay visit records from the given Redis queue")
	replayDateStart = flag.String("replay-date-start", "", "Datatime: date start in RFC3339 format. For example: 2019-03-01T00:00:00+08:00")
	replayDateEnd   = flag.String("replay-date-end", "", "Datatime: date end in RFC3339 format.")
	addr            = flag.String("addr", "127.0.0.1:80", "Addr: file server listen at")
	ossAddr         = flag.String("addr-oss", "127.0.0.1:9000", "Addr: oss server")
	metricAddr      = flag.String("metric-addr", ":8000", "The address to listen on for metric pull requests.")
	pprof           = flag.String("addr-pprof", "", "Addr: pprof http server address")

	ossKey       = flag.String("oss-key", "PA1H2FSS60OKVFHD8D4Z", "oss client access key")
	ossSecretKey = flag.String("oss-secret-key", "Ad2VqYkv4R4KIDnk5GRMn09mOCAUv535zKe8R6oh", "oss client access secret key")
	ossUseSSL    = flag.Bool("oss-ssl", false, "oss client use ssl")
	ossBucket    = flag.String("oss-bucket", "images", "oss bucket name")

	sessionTimeoutSec   = flag.Int("timeout-session", 30, "Timeout(sec): timeout that not received msg from client")
	retryMaxRetryTimes  = flag.Int("retry-max-times", 3, "Max: retry times of put file to the oss server")
	retryIntervalSec    = flag.Int("retry-interval", 10, "Interval(sec): interval seconds between two retries")
	retryIntervalFactor = flag.Int("retry-interval-factor", 2, "Factor: retry interval factor")

	hyenaMqAddr    = flag.String("hyena-mq-addr", "172.19.0.107:9092", "List of hyena-mq addr.")
	hyenaPdAddr    = flag.String("hyena-pd-addr", "172.19.0.101:9529,172.19.0.103:9529,172.19.0.104:9529", "List of hyena-pd addr.")
	predictServURL = flag.String("predict-serv-url", "http://172.19.0.104:8081/", "Face predict server url")

	identifyDisThr2 = flag.Float64("identify-distance-threshold2", 0.6, "Distance threshold of merging new vector.")
	identifyDisThr3 = flag.Float64("identify-distance-threshold3", 0.8, "Distance threshold of discarding new vector.")

	redisAddr = flag.String("redis-addr", "127.0.0.1:6379", "Addr: redis address")

	eurekaAddr = flag.String("eureka-addr", "http://127.0.0.1:8761/eureka", "eureka server address list, seperated by comma.")
	eurekaApp  = flag.String("eureka-app", "iot-backend", "CMDB service name which been registered with eureka.")

	showVer = flag.Bool("version", false, "Show version and quit.")
)

const (
	InferBatchSize int = 5
)

type VecMsg struct {
	Shop     uint64
	Position uint32
	ModTime  int64
	ObjID    string
	Img      []byte
	Vec      []float32
	Age      int
	Gender   int
	Quality  float32
}

func handleImgMsgs(iden3 *Identifier3, recorder *Recorder, imgMsgs []server.ImgMsg) {
	var visits []*server.Visit
	var err error
	if visits, err = iden3.DoBatch(imgMsgs); err != nil {
		log.Errorf("got error: %+v", err)
		return
	}
	if err = recorder.Record(visits); err != nil {
		log.Errorf("got error: %+v", err)
		return
	}
	return
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
		syscall.SIGQUIT,
		syscall.SIGUSR1,
		syscall.SIGUSR2,
	)

	imgCh := make(chan server.ImgMsg, 10000)
	mqs := strings.Split(*hyenaMqAddr, ",")
	prophets := strings.Split(*hyenaPdAddr, ",")
	var vdb proxy.Proxy
	var err error
	if vdb, err = proxy.NewMQBasedProxy("hyena", mqs, prophets, proxy.WithSearchTimeout(time.Duration(HyenaSearchTimeout)*time.Second)); err != nil {
		log.Fatalf("got error %+v", err)
	}

	iden3 := NewIdentifier3(vdb, float32(*identifyDisThr2), float32(*identifyDisThr3), *predictServURL, *redisAddr)
	var recorder *Recorder
	if recorder, err = NewRecorder(mqs, "visits3"); err != nil {
		log.Errorf("got error: %+v", err)
		return
	}
	if *replayAddr != "" {
		if err = replayVisitRecords(iden3, recorder); err != nil {
			log.Errorf("got error: %+v", err)
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := server.NewFileServer(parseCfg(), imgCh)
	go s.Start()
	go func() {
		var imgMsgs []server.ImgMsg
		tickCh := time.Tick(50 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				log.Infof("image process goroutine exited")
				return
			case img := <-imgCh:
				imgMsgs = append(imgMsgs, img)
				if len(imgMsgs) >= InferBatchSize {
					handleImgMsgs(iden3, recorder, imgMsgs)
					imgMsgs = make([]server.ImgMsg, 0)
				}
			case <-tickCh:
				if len(imgMsgs) != 0 {
					handleImgMsgs(iden3, recorder, imgMsgs)
					imgMsgs = make([]server.ImgMsg, 0)
				}
			}
		}
	}()

	for {
		sig := <-sc
		switch sig {
		case syscall.SIGUSR1:
			buf := bytes.NewBuffer([]byte{})
			_ = runPprof.Lookup("goroutine").WriteTo(buf, 1)
			log.Infof("got signal=<%d>.", sig)
			log.Infof(buf.String())
			continue
		case syscall.SIGUSR2:
			log.Infof("got signal=<%d>.", sig)
			if log.GetLogLevel() != log.LogDebug {
				log.Info("changed log level to debug")
				log.SetLevel(log.LogDebug)
			} else {
				log.Info("changed log level to info")
				log.SetLevel(log.LogInfo)
			}
			continue
		default:
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

	cfg.EurekaAddr = *eurekaAddr
	cfg.EurekaApp = *eurekaApp
	return cfg
}

func s3Get(srv *s3.S3, key string) (value []byte, err error) {
	var out *s3.GetObjectOutput
	out, err = srv.GetObject(&s3.GetObjectInput{
		Bucket: ossBucket,
		Key:    &key,
	})
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if value, err = ioutil.ReadAll(out.Body); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func replayVisitRecords(iden3 *Identifier3, recorder *Recorder) (err error) {
	log.Infof("replaying visit records from %v...", *replayAddr)
	que := "visit_queue"
	var sess *session.Session
	sess = session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(*ossKey, *ossSecretKey, ""),
		Endpoint:         aws.String(*ossAddr),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String("default"),
	}))
	srv := s3.New(sess)
	rcli := redis.NewClient(&redis.Options{
		Addr:     *replayAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	var idxStart, idxEnd int64
	if idxStart, idxEnd, err = server.GetVisitIdxRange(rcli, que, *replayDateStart, *replayDateEnd); err != nil {
		log.Fatal(err)
	}

	for idxCur := idxStart; idxCur < idxEnd; idxCur += int64(InferBatchSize) {
		var recs []string
		if recs, err = rcli.LRange(que, idxCur, idxCur+int64(InferBatchSize)-1).Result(); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		var imgMsgs []server.ImgMsg
		var img []byte
		for _, rec := range recs {
			var visit server.Visit
			if err = visit.Unmarshal([]byte(rec)); err != nil {
				err = errors.Wrapf(err, "")
				return
			}

			objID := visit.PictureId
			if img, err = s3Get(srv, objID); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			imgMsg := server.ImgMsg{
				Shop:     visit.Shop,
				Position: visit.Position,
				ModTime:  int64(visit.VisitTime),
				ObjID:    objID,
				Img:      img,
			}
			imgMsgs = append(imgMsgs, imgMsg)
		}
		handleImgMsgs(iden3, recorder, imgMsgs)
	}
	log.Infof("replayed %v visit records from %v...", idxEnd-idxStart, *replayAddr)
	return
}
