package main

import (
	"fmt"
	"hash"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/fagongzi/log"
	"github.com/go-redis/redis"
	"github.com/infinivision/hyena/pkg/proxy"
	cache "github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	SIZEOF_FLOAT32     int = 4
	ageCacheWindow     int = 30 * 60 //cache age lookup result for 30 minutes
	HyenaSearchTimeout int = 2       //in seconds
	HttpRRTimeout      int = 2       //in seconds
)

var (
	idenOnce           sync.Once
	idenSearchDuration prometheus.Histogram
	idenAddDuration    prometheus.Histogram
	idenUpdateDuration prometheus.Histogram
)

type AgeGender struct {
	Age    int `json:"age"`
	Gender int `json:"gender"`
}

type AgePred struct {
	Prediction AgeGender `json:"prediction"`
}

type Identifier3 struct {
	distThr2 float32
	distThr3 float32
	flatThr  int
	vdb      proxy.Proxy
	h64      hash.Hash64

	ageServURL string
	hc         *http.Client
	ageCache   *cache.Cache
	rcli       *redis.Client
}

func NewIdentifier3(vdb proxy.Proxy, distThr2, distThr3 float32, ageServURL, redisAddr string) (iden *Identifier3) {
	iden = &Identifier3{
		distThr2: distThr2,
		distThr3: distThr3,
		vdb:      vdb,
		h64:      xxhash.New(),

		ageServURL: ageServURL,
		hc:         &http.Client{Timeout: time.Duration(HttpRRTimeout) * time.Second},
		ageCache:   cache.New(time.Second*time.Duration(ageCacheWindow), time.Minute),
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
		idenSearchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_search_duration_seconds",
			Help:      "identify RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})
		idenAddDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_add_duration_seconds",
			Help:      "identify RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})
		idenUpdateDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "idendify_update_duration_seconds",
			Help:      "identify RPC latency distributions.",
			Buckets:   prometheus.LinearBuckets(0, 0.01, 100), //100 buckets, each is 10 ms.
		})

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
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&vec))
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
	if cnt1 != 0 || cnt2 != 0 {
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

	var found bool
	keyUid := fmt.Sprintf("%s", uid)
	var ag *AgeGender
	var val interface{}
	if val, found = this.ageCache.Get(keyUid); found {
		ag = val.(*AgeGender)
	} else {
		agePred := &AgePred{}
		if _, err = PostFile(this.hc, this.ageServURL, vecMsg.Img, agePred); err != nil {
			return
		}
		ag = &agePred.Prediction
	}
	this.ageCache.SetDefault(keyUid, ag)
	visit = &Visit{
		Uid:       uint64(uid),
		VisitTime: uint64(vecMsg.ModTime),
		Shop:      uint64(vecMsg.Shop),
		Position:  uint32(vecMsg.Position),
		Age:       uint32(ag.Age),
	}
	if ag.Gender != 0 {
		visit.IsMale = true
	}
	log.Infof("objID: %+v, visit3: %+v", vecMsg.ObjID, visit)
	return
}
