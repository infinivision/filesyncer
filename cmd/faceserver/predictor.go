package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/pkg/errors"
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
	ctx         context.Context
	cancel      context.CancelFunc
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
			Buckets: prometheus.LinearBuckets(0, 0.05, 20), //20 buckets, each is 50 ms.
		}),
	}
	prometheus.MustRegister(pred.rpcDuration)
	return
}

func (this *Predictor) Start() {
	if this.ctx != nil {
		return
	}
	this.ctx, this.cancel = context.WithCancel(context.Background())
	for i := 0; i < this.parallel; i++ {
		go func() {
			var pr *PredResp
			var err error
			for {
				select {
				case <-this.ctx.Done():
					return
				case img := <-this.imgCh:
					if pr, err = this.do(img.Img); err != nil {
						log.Errorf("%+v", err)
						continue
					}
					log.Debugf("sent vecMsg: %+v", pr)
					this.vecCh <- VecMsg{Mac: img.Mac, Vec: pr.Vec}
				}
			}

		}()

	}
}

func (this *Predictor) Stop() {
	if this.ctx == nil {
		return
	}
	this.cancel()
	this.ctx = nil
	this.cancel = nil
}

func (this *Predictor) do(img io.Reader) (pr *PredResp, err error) {
	var resp *http.Response
	var respBody []byte
	t0 := time.Now()
	reqBody := &bytes.Buffer{}
	writer := multipart.NewWriter(reqBody)
	//part, err := writer.CreateFormFile("data", "image.jpg") //uses "Content-Type: application/octet-stream"
	partHeader := textproto.MIMEHeader{}
	partHeader.Add("Content-Disposition", `form-data; name="data"; filename="image.jpg"`)
	partHeader.Add("Content-Type", "image/jpeg")
	part, err := writer.CreatePart(partHeader)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if _, err = io.Copy(part, img); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	writer.Close()
	req, err := http.NewRequest("POST", this.servURL, reqBody)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	if resp, err = this.hc.Do(req); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	duration := time.Since(t0).Seconds()
	this.rpcDuration.Observe(duration)
	respBody, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	pr = &PredResp{}
	if err = json.Unmarshal(respBody, pr); err != nil {
		err = errors.Wrapf(err, "respBody: %+v", string(respBody))
		return
	}
	return
}
