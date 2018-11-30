package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	predOnce        sync.Once
	predRpcDuration prometheus.Histogram
)

type Predictor struct {
	servURL string
	hc      *http.Client
}

type PredResp struct {
	Vec []float32 `json:"prediction"`
}

func NewPredictor(servURL string) (pred *Predictor) {
	pred = &Predictor{
		servURL: servURL,
		hc:      &http.Client{Timeout: time.Second * 10},
	}

	predOnce.Do(func() {
		predRpcDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "predication_rpc_duration_seconds",
			Help:      "predication RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})
		prometheus.MustRegister(predRpcDuration)
	})
	return
}

func (this *Predictor) Predictate(img []byte) (pr *PredResp, err error) {
	pr = &PredResp{}
	var duration time.Duration
	if duration, err = PostFile(this.hc, this.servURL, img, pr); err != nil {
		return
	}
	predRpcDuration.Observe(duration.Seconds())
	return
}
