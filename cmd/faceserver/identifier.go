package main

import (
	fmt "fmt"
	"net/http"
	"sync/atomic"
	"time"

	cache "github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"

	"github.com/fagongzi/log"
	"github.com/infinivision/vectodb"
)

const (
	metricType     = 0
	indexKey       = "IVF4096,PQ32"
	queryParams    = "nprobe=256,ht=256"
	ageCacheWindow = 30 * 60 //cache age lookup result for 30 minutes
)

type Identifier struct {
	vecCh     <-chan VecMsg
	visitCh   chan<- *Visit
	parallel  int
	batchSize int
	distThr   float32
	flatThr   int
	vdb       *vectodb.VectoDB
	nextXid   int64

	ageServURL string
	hc         *http.Client
	ageCache   *cache.Cache

	searchDuration prometheus.Histogram
	addDuration    prometheus.Histogram
	updateDuration prometheus.Histogram
}

type AgeGender struct {
	Age    int `json:"age"`
	Gender int `json:"gender"`
}

type AgePred struct {
	Prediction AgeGender `json:"prediction"`
}

func NewIdentifier(vecCh <-chan VecMsg, visitCh chan<- *Visit, parallel int, batchSize int, distThr float32, flatThr int, workDir string, dim int, ageServURL string) (iden *Identifier) {
	iden = &Identifier{
		vecCh:     vecCh,
		visitCh:   visitCh,
		parallel:  parallel,
		batchSize: batchSize,
		distThr:   distThr,
		flatThr:   flatThr,

		ageServURL: ageServURL,
		hc:         &http.Client{Timeout: time.Second * 10},
		ageCache:   cache.New(time.Second*time.Duration(ageCacheWindow), time.Minute),

		searchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_search_duration_seconds",
			Help:      "identify RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		}),
		addDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_add_duration_seconds",
			Help:      "identify RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		}),
		updateDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_update_duration_seconds",
			Help:      "identify RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		}),
	}
	prometheus.MustRegister(iden.searchDuration)
	prometheus.MustRegister(iden.addDuration)
	prometheus.MustRegister(iden.updateDuration)
	var err error
	if iden.vdb, err = vectodb.NewVectoDB(workDir, dim, metricType, indexKey, queryParams, distThr, flatThr); err != nil {
		log.Fatalf("%+v", err)
	}
	var ntotal int
	if ntotal, err = iden.vdb.GetTotal(); err != nil {
		log.Fatalf("%+v", err)
	}
	iden.nextXid = int64(ntotal)
	return
}

func (this *Identifier) Serve(ctx context.Context) {
	go this.builderLoop(ctx)
	for i := 0; i < this.parallel; i++ {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			vecMsgs := make([]VecMsg, 0)
			var err error
			for {
				select {
				case <-ctx.Done():
					return
				case vecMsg := <-this.vecCh:
					log.Debugf("received vecMsg for image, length %d, vec %v", len(vecMsg.Img), vecMsg.Vec)
					vecMsgs = append(vecMsgs, vecMsg)
					if len(vecMsgs) >= this.batchSize {
						if err = this.doBatch(vecMsgs); err != nil {
							log.Errorf("%+v", err)
						}
						vecMsgs = nil
					}
				case <-ticker.C:
					if len(vecMsgs) != 0 {
						if err = this.doBatch(vecMsgs); err != nil {
							log.Errorf("%+v", err)
						}
						vecMsgs = nil
					}
				}
			}
		}()
	}
}

func (this *Identifier) builderLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err = this.vdb.UpdateIndex(); err != nil {
				log.Errorf("%+v", err)
			}
		}
	}
}

func (this *Identifier) allocateXid() (xid int64) {
	xid = atomic.AddInt64(&this.nextXid, 1) - 1
	return
}

func (this *Identifier) doBatch(vecMsgs []VecMsg) (err error) {
	//TODO: query distributed vectodb
	nq := len(vecMsgs)
	log.Debugf("(*Identifier).doBatch %d", nq)
	xq := make([]float32, 0)
	distances := make([]float32, nq)
	xids := make([]int64, nq)
	for _, vecMsg := range vecMsgs {
		xq = append(xq, vecMsg.Vec...)
	}
	t0 := time.Now()
	var ntotal int
	if ntotal, err = this.vdb.Search(xq, distances, xids); err != nil {
		return
	}
	duration := time.Since(t0).Seconds()
	this.searchDuration.Observe(duration)

	var newXid int64
	var newXb []float32
	var newXids []int64
	var extXb []float32
	var extXids []int64
	if ntotal == 0 {
		newXb = xq
		for i := 0; i < nq; i++ {
			newXids = append(newXids, this.allocateXid())
		}
	} else {
		for i := 0; i < nq; i++ {
			if xids[i] == int64(-1) {
				newXid = this.allocateXid()
				xids[i] = newXid
				newXb = append(newXb, vecMsgs[i].Vec...)
				newXids = append(newXids, newXid)
			} else {
				extXb = append(extXb, vecMsgs[i].Vec...)
				extXids = append(extXids, xids[i])
			}
		}
	}
	log.Debugf("vectodb search result: hit %d, miss %d, ntotal %d, distances %v", len(extXids), len(newXids), ntotal, distances)
	if newXb != nil {
		t0 := time.Now()
		if err = this.vdb.AddWithIds(newXb, newXids); err != nil {
			return
		}
		duration := time.Since(t0).Seconds()
		this.addDuration.Observe(duration)
	}
	if extXb != nil {
		t0 := time.Now()
		if err = this.vdb.UpdateWithIds(extXb, extXids); err != nil {
			return
		}
		duration := time.Since(t0).Seconds()
		this.updateDuration.Observe(duration)
	}

	var found bool
	for i := 0; i < nq; i++ {
		strUid := fmt.Sprintf("%s", xids[i])
		var ag *AgeGender
		var val interface{}
		if val, found = this.ageCache.Get(strUid); found {
			ag = val.(*AgeGender)
		} else {
			agePred := &AgePred{}
			_, err = PostFile(this.hc, this.ageServURL, vecMsgs[i].Img, agePred)
			if err != nil {
				log.Errorf("%+v", err)
				continue
			}
			ag = &agePred.Prediction
		}
		this.ageCache.SetDefault(strUid, ag)
		visit := &Visit{
			Uid:       uint64(xids[i]),
			VisitTime: uint64(vecMsgs[i].ModTime),
			Shop:      uint64(vecMsgs[i].Shop),
			Position:  uint32(vecMsgs[i].Position),
			Age:       uint32(ag.Age),
		}
		if ag.Gender != 0 {
			visit.IsMale = true
		}
		log.Infof("objID: %+v, visit: %+v", vecMsgs[i].ObjID, visit)
		this.visitCh <- visit
		return
	}
	return
}
