package main

import (
	"net/http"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

type Predictor struct {
	servURL     string
	imgCh       <-chan server.ImgMsg
	vecCh       chan<- VecMsg
	parallel    int
	hc          *http.Client
	rpcDuration prometheus.Histogram
}

type PredResp struct {
	Vec []float32 `json:"prediction"`
}

func NewPredictor(servURL string, imgCh <-chan server.ImgMsg, vecCh chan<- VecMsg, parallel int) (pred *Predictor) {
	pred = &Predictor{
		servURL:  servURL,
		imgCh:    imgCh,
		vecCh:    vecCh,
		hc:       &http.Client{Timeout: time.Second * 10},
		parallel: parallel,
		rpcDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "predication_rpc_duration_seconds",
			Help:    "predication RPC latency distributions.",
			Buckets: prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		}),
	}
	prometheus.MustRegister(pred.rpcDuration)
	return
}

func (this *Predictor) Serve(ctx context.Context) {
	for i := 0; i < this.parallel; i++ {
		go func() {
			var pr *PredResp
			var err error
			for {
				select {
				case <-ctx.Done():
					return
				case img := <-this.imgCh:
					if pr, err = this.do(img.Img); err != nil {
						log.Errorf("%+v", err)
						continue
					}
					log.Debugf("sent vecMsg: %+v", pr)
					this.vecCh <- VecMsg{Shop: img.Shop, Img: img.Img, Vec: pr.Vec}
				}
			}

		}()
	}
}

func (this *Predictor) do(img []byte) (pr *PredResp, err error) {
	pr = &PredResp{}
	var duration time.Duration
	if duration, err = PostFile(this.hc, this.servURL, img, pr); err != nil {
		return
	}
	this.rpcDuration.Observe(duration.Seconds())
	return
}
