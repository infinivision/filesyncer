package main

import (
	fmt "fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	cache "github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/fagongzi/log"
	"github.com/go-redis/redis"
	"github.com/infinivision/vectodb"
)

type Identifier3 struct {
	vecCh     <-chan VecMsg
	visitCh   chan<- *Visit
	parallel  int
	batchSize int
	distThr1  float32
	distThr2  float32
	distThr3  float32
	flatThr   int
	vdb       *vectodb.VectoDB
	nextXid   int64

	ageServURL string
	hc         *http.Client
	ageCache   *cache.Cache
	rcli       *redis.Client
}

func NewIdentifier3(vecCh <-chan VecMsg, visitCh chan<- *Visit, parallel int, batchSize int, distThr1, distThr2, distThr3 float32, flatThr int, workDir string, dim int, ageServURL, redisAddr string) (iden *Identifier3) {
	iden = &Identifier3{
		vecCh:     vecCh,
		visitCh:   visitCh,
		parallel:  parallel,
		batchSize: batchSize,
		distThr1:  distThr1,
		distThr2:  distThr2,
		distThr3:  distThr3,
		flatThr:   flatThr,

		ageServURL: ageServURL,
		hc:         &http.Client{Timeout: time.Second * 10},
		ageCache:   cache.New(time.Second*time.Duration(ageCacheWindow), time.Minute),
	}
	var err error
	if iden.vdb, err = vectodb.NewVectoDB(workDir, dim, metricType, indexKey, queryParams, distThr1, flatThr); err != nil {
		log.Fatalf("%+v", err)
	}
	var ntotal int
	if ntotal, err = iden.vdb.GetTotal(); err != nil {
		log.Fatalf("%+v", err)
	}
	iden.nextXid = int64(ntotal)

	iden.rcli = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if _, err = iden.rcli.Ping().Result(); err != nil {
		err = errors.Wrap(err, "")
		log.Errorf("got error %+v", err)
	}
	return
}

func (this *Identifier3) Serve(ctx context.Context) {
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

func (this *Identifier3) builderLoop(ctx context.Context) {
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

func (this *Identifier3) allocateUid() (uid int64, err error) {
	if uid, err = this.rcli.Incr("faceserver_next_uid").Result(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (this *Identifier3) getUid(xid int64) (uid int64, err error) {
	var strUid string
	if strUid, err = this.rcli.Get(fmt.Sprintf("xid_%v", xid)).Result(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if uid, err = strconv.ParseInt(strUid, 10, 64); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (this *Identifier3) getXidsLen(uid int64) (xl int64, err error) {
	if xl, err = this.rcli.LLen(fmt.Sprintf("uid_%v", uid)).Result(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (this *Identifier3) assoicateUidXid(uid, xid int64) (err error) {
	if err = this.rcli.Set(fmt.Sprintf("xid_%v", xid), strconv.FormatInt(uid, 10), 0).Err(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if err = this.rcli.LPush(fmt.Sprintf("uid_%v", uid), strconv.FormatInt(xid, 10)).Err(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (this *Identifier3) allocateXid() (xid int64) {
	xid = atomic.AddInt64(&this.nextXid, 1) - 1
	return
}

func (this *Identifier3) doBatch(vecMsgs []VecMsg) (err error) {
	//TODO: query distributed vectodb
	nq := len(vecMsgs)
	log.Debugf("(*Identifier).doBatch %d", nq)
	xq := make([]float32, 0)
	distances := make([]float32, nq)
	uids := make([]int64, nq)
	xids := make([]int64, nq)
	for _, vecMsg := range vecMsgs {
		xq = append(xq, vecMsg.Vec...)
	}
	var ntotal int
	if ntotal, err = this.vdb.Search(xq, distances, xids); err != nil {
		return
	}

	var cnt1, cnt2, cnt3, cnt4 int
	var newXid int64
	var newXb []float32
	var newXids []int64
	var extXb []float32
	var extXids []int64
	if ntotal == 0 {
		newXb = xq
		for i := 0; i < nq; i++ {
			newXids = append(newXids, this.allocateXid())
			if uids[i], err = this.allocateUid(); err != nil {
				return
			}
		}
	} else {
		for i := 0; i < nq; i++ {
			if xids[i] == int64(-1) {
				cnt1++
				newXid = this.allocateXid()
				if uids[i], err = this.allocateUid(); err != nil {
					return
				}
				if err = this.assoicateUidXid(uids[i], newXid); err != nil {
					return
				}
				newXb = append(newXb, vecMsgs[i].Vec...)
				newXids = append(newXids, newXid)
			} else {
				if uids[i], err = this.getUid(xids[i]); err != nil {
					return
				}
				if vectodb.VectodbCompareDistance(metricType, this.distThr2, distances[i]) {
					cnt2++
					var xl int64
					if xl, err = this.getXidsLen(uids[i]); err != nil {
						return
					}
					if xl < 8 {
						newXid = this.allocateXid()
						if err = this.assoicateUidXid(uids[i], newXid); err != nil {
							return
						}
						newXb = append(newXb, vecMsgs[i].Vec...)
						newXids = append(newXids, newXid)
					}
				} else if vectodb.VectodbCompareDistance(metricType, this.distThr3, distances[i]) {
					cnt3++
				} else {
					cnt4++
					extXb = append(extXb, vecMsgs[i].Vec...)
					extXids = append(extXids, xids[i])
				}
			}
		}
	}
	log.Debugf("vectodb search result: cnt1 %d, cnt2 %d, cnt3 %d, cnt4 %d, ntotal %d, distances %v", cnt1, cnt2, cnt3, cnt4, ntotal, distances)
	if newXb != nil {
		if err = this.vdb.AddWithIds(newXb, newXids); err != nil {
			return
		}
	}
	if extXb != nil {
		if err = this.vdb.UpdateWithIds(extXb, extXids); err != nil {
			return
		}
	}

	var found bool
	for i := 0; i < nq; i++ {
		strUid := fmt.Sprintf("%s", uids[i])
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
			Uid:       uint64(uids[i]),
			VisitTime: uint64(vecMsgs[i].ModTime),
			Shop:      uint64(vecMsgs[i].Shop),
			Position:  uint32(vecMsgs[i].Position),
			Age:       uint32(ag.Age),
		}
		if ag.Gender != 0 {
			visit.IsMale = true
		}
		log.Infof("objID: %+v, visit3: %+v", vecMsgs[i].ObjID, visit)
		this.visitCh <- visit
		return
	}
	return
}
