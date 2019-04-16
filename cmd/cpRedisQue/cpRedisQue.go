package main

import (
	"flag"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	fromRedisAddr = flag.String("from-redis-addr", "172.19.0.103:6379", "Addr: from redis address")
	toRedisAddr   = flag.String("to-redis-addr", "172.19.0.101:6379", "Addr: to redis address")
	que           = "visit_queue"
	batchSize     = int64(1000)
)

func main() {
	flag.Parse()

	var err error

	rcli1 := redis.NewClient(&redis.Options{
		Addr:     *fromRedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	rcli2 := redis.NewClient(&redis.Options{
		Addr:     *toRedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	var qLen int64
	if qLen, err = rcli1.LLen(que).Result(); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatal(err)
	}
	if _, err = rcli2.Del(que).Result(); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatal(err)
	}

	var idxStart int64
	for idxStart = 0; idxStart < qLen; idxStart += batchSize {
		var recs []string
		if recs, err = rcli1.LRange(que, int64(idxStart), int64(idxStart+batchSize-1)).Result(); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		if _, err = rcli2.RPush(que, recs).Result(); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
	}

	log.Infof("Copied %v elements from %v to %v", qLen, *fromRedisAddr, *toRedisAddr)
	return
}
