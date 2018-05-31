package main

import (
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"

	"github.com/fagongzi/log"
	"github.com/infinivision/vectodb"
)

const (
	metricType  = 0
	indexKey    = "IVF4096,PQ32"
	queryParams = "nprobe=256,ht=256"
)

type Identifier struct {
	vecCh          <-chan VecMsg
	visitCh        chan<- *Visit
	parallel       int
	batchSize      int
	distThr        float32
	flatThr        int
	vdb            *vectodb.VectoDB
	nextXid        int64
	searchDuration prometheus.Histogram
	addDuration    prometheus.Histogram
	updateDuration prometheus.Histogram
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewIdentifier(vecCh <-chan VecMsg, visitCh chan<- *Visit, parallel int, batchSize int, distThr float32, flatThr int, workDir string, dim int) (iden *Identifier) {
	iden = &Identifier{
		vecCh:     vecCh,
		visitCh:   visitCh,
		parallel:  parallel,
		batchSize: batchSize,
		distThr:   distThr,
		flatThr:   flatThr,
		searchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "idendify_search_duration_seconds",
			Help:    "identify RPC latency distributions.",
			Buckets: prometheus.LinearBuckets(0, 0.05, 20), //20 buckets, each is 50 ms.
		}),
		addDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "idendify_add_duration_seconds",
			Help:    "identify RPC latency distributions.",
			Buckets: prometheus.LinearBuckets(0, 0.05, 20), //20 buckets, each is 50 ms.
		}),
		updateDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "idendify_update_duration_seconds",
			Help:    "identify RPC latency distributions.",
			Buckets: prometheus.LinearBuckets(0, 0.05, 20), //20 buckets, each is 50 ms.
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

func (this *Identifier) Start() {
	if this.ctx != nil {
		return
	}
	this.ctx, this.cancel = context.WithCancel(context.Background())
	go this.builderLoop()
	for i := 0; i < this.parallel; i++ {
		go func() {
			doneCh := this.ctx.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			vecMsgs := make([]VecMsg, 0)
			var err error
			for {
				select {
				case <-doneCh:
					return
				case vecMsg := <-this.vecCh:
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

func (this *Identifier) Stop() {
	if this.ctx == nil {
		return
	}
	this.cancel()
	this.ctx = nil
	this.cancel = nil
}

func (this *Identifier) builderLoop() {
	doneCh := this.ctx.Done()
	ticker := time.NewTicker(10 * time.Second)
	var err error
	for {
		select {
		case <-doneCh:
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
	//TODO: convert vecMsg.Mac to location via CMDB?
	nq := len(vecMsgs)
	xq := make([]float32, 0)
	distances := make([]float32, nq)
	xids := make([]int64, nq)
	var ntotal int
	for _, vecMsg := range vecMsgs {
		xq = append(xq, vecMsg.Vec...)
	}
	t0 := time.Now()
	if ntotal, err = this.vdb.Search(nq, xq, distances, xids); err != nil || ntotal == 0 {
		return
	}
	duration := time.Since(t0).Seconds()
	this.searchDuration.Observe(duration)

	var newXid int64
	var newXb []float32
	var newXids []int64
	var extXb []float32
	var extXids []int64
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
	if newXb != nil {
		t0 := time.Now()
		if err = this.vdb.AddWithIds(len(newXids), newXb, newXids); err != nil {
			return
		}
		duration := time.Since(t0).Seconds()
		this.addDuration.Observe(duration)
	}
	if extXb != nil {
		t0 := time.Now()
		if err = this.vdb.UpdateWithIds(len(extXids), extXb, extXids); err != nil {
			return
		}
		duration := time.Since(t0).Seconds()
		this.updateDuration.Observe(duration)
	}

	for i := 0; i < nq; i++ {

		var mac []byte
		if mac, err = hex.DecodeString(vecMsgs[i].Mac); err != nil {
			err = errors.Wrapf(err, "mac: %+v", vecMsgs[i].Mac)
			return
		}
		if len(mac) > 8 {
			mac = mac[:8]
		} else {
			for i := 0; i < 8-len(vecMsgs[i].Mac); i++ {
				mac = append(mac, 0)
			}
		}
		location := binary.BigEndian.Uint64(mac)
		visit := &Visit{
			Uid:       uint64(xids[i]),
			VisitTime: uint64(time.Now().Unix()),
			Location:  location,
			Age:       99, //TODO: determine Age and IsMale
			IsMale:    false,
		}
		this.visitCh <- visit
		return
	}
	return
}
