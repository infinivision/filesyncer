package main

import (
	"encoding/base64"
	"fmt"
	"hash"
	math "math"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/fagongzi/log"
	"github.com/go-redis/redis"
	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/infinivision/hyena/pkg/proxy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	SIZEOF_FLOAT32     int = 4
	HyenaSearchTimeout int = 2 //in seconds
	HttpRRTimeout      int = 2 //in seconds
)

var (
	idenOnce           sync.Once
	idenPredDuration   prometheus.Histogram
	idenSearchDuration prometheus.Histogram
	idenAddDuration    prometheus.Histogram
	idenUpdateDuration prometheus.Histogram
)

type AgeGender struct {
	Age    int `json:"age"`
	Gender int `json:"gender"`
}

type RspPred struct {
	FileName  string `json:"filename"`
	Embedding string `json:"embedding"`
	Age       int    `json:"age"`
	Gender    int    `json:"gender"`
	PoseType  int    `json:"post_type"`
	State     int    `json:"state"`
}

type Identifier3 struct {
	distThr2 float32
	distThr3 float32
	flatThr  int
	vdb      proxy.Proxy
	h64      hash.Hash64

	servURL string
	hc      *http.Client
	rcli    *redis.Client
}

func NewIdentifier3(vdb proxy.Proxy, distThr2, distThr3 float32, servURL, redisAddr string) (iden *Identifier3) {
	iden = &Identifier3{
		distThr2: distThr2,
		distThr3: distThr3,
		vdb:      vdb,
		h64:      xxhash.New(),

		servURL: servURL,
		hc:      &http.Client{Timeout: time.Duration(HttpRRTimeout) * time.Second},
	}
	var err error

	iden.rcli = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if _, err = iden.rcli.Ping().Result(); err != nil {
		err = errors.Wrap(err, "")
		log.Errorf("got error %+v", err)
	}

	idenOnce.Do(func() {
		idenPredDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_predication_duration_seconds",
			Help:      "predication RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})
		idenSearchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_search_duration_seconds",
			Help:      "hyena search RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})
		idenAddDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_add_duration_seconds",
			Help:      "hyena add RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})
		idenUpdateDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_update_duration_seconds",
			Help:      "hyena update RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})

		prometheus.MustRegister(idenPredDuration)
		prometheus.MustRegister(idenSearchDuration)
		prometheus.MustRegister(idenAddDuration)
		prometheus.MustRegister(idenUpdateDuration)
	})
	return
}

func (this *Identifier3) allocateUid() (uid int64, err error) {
	if uid, err = this.rcli.Incr("faceserver_next_uid").Result(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	log.Infof("allocated uid %v", uid)
	return
}

func (this *Identifier3) getUid(xid int64) (uid int64, err error) {
	var strUid string
	keyXid := fmt.Sprintf("xid_%016x", uint64(xid))
	if strUid, err = this.rcli.Get(keyXid).Result(); err != nil {
		err = errors.Wrapf(err, "keyXid %v", keyXid)
		return
	}
	if uid, err = strconv.ParseInt(strUid, 10, 64); err != nil {
		err = errors.Wrapf(err, "strUid %v", strUid)
		return
	}
	return
}

func (this *Identifier3) getXidsLen(uid int64) (xl int64, err error) {
	keyUid := fmt.Sprintf("uid_%v", uid)
	if xl, err = this.rcli.LLen(keyUid).Result(); err != nil {
		err = errors.Wrapf(err, "keyUid %v", keyUid)
		return
	}
	return
}

func (this *Identifier3) associateUidXid(uid, xid int64) (err error) {
	keyUid := fmt.Sprintf("uid_%v", uid)
	keyXid := fmt.Sprintf("xid_%016x", uint64(xid))
	strUid := strconv.FormatInt(uid, 10)
	strXid := fmt.Sprintf("%016x", uint64(xid))
	if err = this.rcli.Set(keyXid, strUid, 0).Err(); err != nil {
		err = errors.Wrapf(err, "keyXid %v", keyXid)
		return
	}
	if err = this.rcli.LPush(keyUid, strXid).Err(); err != nil {
		err = errors.Wrapf(err, "keyUid %v", keyUid)
		return
	}
	log.Infof("associated xid %v with uid %v", strXid, strUid)
	return
}

// allocateXid uses hash of vec as xid. This also helps to deduplicate vectors per content.
func (this *Identifier3) allocateXid(vec []float32) (xid int64) {
	// https://stackoverflow.com/questions/11924196/convert-between-slices-of-different-types
	// Get the slice header
	vec2 := vec
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&vec2))
	// The length and capacity of the slice are different.
	header.Len *= SIZEOF_FLOAT32
	header.Cap *= SIZEOF_FLOAT32
	// Convert slice header to an []byte
	data := *(*[]byte)(unsafe.Pointer(&header))

	this.h64.Reset()
	this.h64.Write(data)
	xid = int64(this.h64.Sum64())
	log.Infof("allocated xid %016x", uint64(xid))
	return
}

func normalize(vec []float32) {
	var prod float64
	for i := 0; i < len(vec); i++ {
		prod += float64(vec[i]) * float64(vec[i])
	}
	prod = math.Sqrt(prod)
	for i := 0; i < len(vec); i++ {
		vec[i] = float32(float64(vec[i]) / prod)
	}
	return
}

func (this *Identifier3) DoBatch(imgMsgs []server.ImgMsg) (visits []*Visit, err error) {
	var imgs [][]byte
	var visit *Visit
	var duration time.Duration
	for _, img := range imgMsgs {
		imgs = append(imgs, img.Img)
	}
	rspPreds := make([]RspPred, len(imgMsgs))
	if duration, err = PostFiles(this.hc, this.servURL, imgs, rspPreds); err != nil {
		log.Errorf("got error %+v", err)
		return
	}
	idenPredDuration.Observe(duration.Seconds())
	for i, rspPred := range rspPreds {
		var data []byte
		if data, err = base64.StdEncoding.DecodeString(rspPred.Embedding); err != nil {
			err = errors.Wrapf(err, "base64 decode error")
			log.Errorf("got error %+v", err)
			return
		}
		if len(data)%SIZEOF_FLOAT32 != 0 {
			log.Errorf("rspPred.Embedding length is incorrect, want times of %d, have %d", SIZEOF_FLOAT32, len(data))
			return
		}
		header := *(*reflect.SliceHeader)(unsafe.Pointer(&data))
		// The length and capacity of the slice are different.
		header.Len /= SIZEOF_FLOAT32
		header.Cap /= SIZEOF_FLOAT32
		// Convert slice header to an []float32
		vec := *(*[]float32)(unsafe.Pointer(&header))
		//predict result needs normalization
		normalize(vec)

		imgMsg := imgMsgs[i]
		vecMsg := VecMsg{Shop: imgMsg.Shop, Position: imgMsg.Position, ModTime: imgMsg.ModTime, ObjID: imgMsg.ObjID, Img: imgMsg.Img, Vec: vec, Age: rspPred.Age, Gender: rspPred.Gender}
		if visit, err = this.Identify(vecMsg); err != nil {
			log.Errorf("got error %+v", err)
			return
		}
		visits = append(visits, visit)
	}
	return
}

func (this *Identifier3) Identify(vecMsg VecMsg) (visit *Visit, err error) {
	var uid int64
	var dbs []uint64
	var distances []float32
	var xids []int64
	t0 := time.Now()
	if dbs, distances, xids, err = this.vdb.Search(vecMsg.Vec); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	duration := time.Since(t0).Seconds()
	idenSearchDuration.Observe(duration)
	log.Infof("hyena search result: dbs %v, distances %v, xids %v", dbs, distances, xids)

	var cnt1, cnt2, cnt3, cnt4 int
	var newXid int64
	var newXids []int64
	if xids[0] == int64(-1) {
		cnt1++
		newXid = this.allocateXid(vecMsg.Vec)
		if uid, err = this.allocateUid(); err != nil {
			return
		}
		if err = this.associateUidXid(uid, newXid); err != nil {
			return
		}
		newXids = append(newXids, newXid)
	} else {
		if uid, err = this.getUid(xids[0]); err != nil {
			return
		}
		if distances[0] < this.distThr2 {
			cnt2++
			var xl int64
			if xl, err = this.getXidsLen(uid); err != nil {
				return
			}
			if xl < 8 {
				newXid = this.allocateXid(vecMsg.Vec)
				if err = this.associateUidXid(uid, newXid); err != nil {
					return
				}
				newXids = append(newXids, newXid)
			}
		} else if distances[0] < this.distThr3 {
			cnt3++
		} else {
			cnt4++
		}
	}
	if len(newXids) != 0 {
		t0 = time.Now()
		log.Infof("hyena added xids %+v, %016x", newXids, uint64(newXids[0]))
		if err = this.vdb.AddWithIds(vecMsg.Vec, newXids); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		duration = time.Since(t0).Seconds()
		idenAddDuration.Observe(duration)
	}
	if cnt4 != 0 {
		t0 = time.Now()
		log.Infof("hyena updated xids %+v, %016x", xids[0], uint64(xids[0]))
		if err = this.vdb.UpdateWithIds(dbs[0], xids[0], vecMsg.Vec); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		duration = time.Since(t0).Seconds()
		idenUpdateDuration.Observe(duration)
	}

	visit = &Visit{
		Uid:       uint64(uid),
		VisitTime: uint64(vecMsg.ModTime),
		Shop:      uint64(vecMsg.Shop),
		Position:  uint32(vecMsg.Position),
		Age:       uint32(vecMsg.Age),
	}
	if vecMsg.Gender != 0 {
		visit.IsMale = true
	}
	log.Infof("objID: %+v, visit3: %+v", vecMsg.ObjID, visit)
	return
}
