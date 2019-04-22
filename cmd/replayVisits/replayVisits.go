package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	redisAddr  = flag.String("redis-addr", "", "Addr: redis address")
	destPgUrl  = flag.String("dest-pg-url", "", "PostgreSQL datasource in format\"%s:%s@%s:%s/%s\", username, password, host, port, database.")
	destMqAddr = flag.String("dest-mq-addr", "", "List of mq addr.")
	dateStart  = flag.String("date-start", "", "Datatime: date start in RFC3339 format. For example: 2019-03-01T00:00:00+08:00")
	dateEnd    = flag.String("date-end", "", "Datatime: date end in RFC3339 format")
	uids       = flag.String("uids", "", "interested user ids. Empyt means all. For example: 1226,3495")
)

func main() {
	var err error
	flag.Parse()
	intUids := make(map[uint64]int)
	var intUid uint64
	for _, sUid := range strings.Split(*uids, ",") {
		if sUid == "" {
			continue
		}
		if intUid, err = strconv.ParseUint(sUid, 10, 64); err != nil {
			err = errors.Errorf("invalid uid: %v", sUid)
			log.Fatal(err)
		}
		intUids[intUid] = 0
	}

	var db *sqlx.DB
	var mqProd sarama.SyncProducer

	if *destPgUrl != "" {
		// this Pings the database trying to connect, panics on error
		// use sqlx.Open() for sql.Open() semantics
		db, err = sqlx.Connect("postgres", *destPgUrl)
		if err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		db.SetMaxOpenConns(1)
	} else if *destMqAddr != "" {
		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 10
		config.Producer.Return.Successes = true
		mqAddrs := strings.Split(*destMqAddr, ",")
		if mqProd, err = sarama.NewSyncProducer(mqAddrs, config); err != nil {
			err = errors.Wrap(err, "")
			log.Fatal(err)
		}
	} else {
		fmt.Println("requires one of dest-pg-url and dest-mq-addr")
		return
	}

	rcli := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	que := "visit_queue"
	var idxStart, idxEnd int64
	if idxStart, idxEnd, err = server.GetVisitIdxRange(rcli, que, *dateStart, *dateEnd); err != nil {
		log.Fatal(err)
	}

	numFetched := 0
	batchSize := int64(1000)
	for idxCur := idxStart; idxCur < idxEnd; idxCur += batchSize {
		var recs []string
		if recs, err = rcli.LRange(que, idxCur, idxCur+batchSize-1).Result(); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		for _, rec := range recs {
			var visit server.Visit
			if err = visit.Unmarshal([]byte(rec)); err != nil {
				err = errors.Wrapf(err, "")
				log.Fatal(err)
			}
			if len(intUids) != 0 {
				if _, found := intUids[visit.Uid]; !found {
					continue
				}
			}
			numFetched += 1
			fmt.Printf("\rfetched %d", numFetched)

			if db != nil {
				vt := time.Unix(int64(visit.VisitTime), 0).Format(time.RFC3339)
				if _, err = db.Query("SELECT insert_user($1, $2, $3, $4, $5, $6)", visit.Uid, visit.PictureId, visit.Quality, visit.Age, visit.Gender, vt); err != nil {
					err = errors.Wrapf(err, "")
					log.Fatal(err)
				}
				if _, err = db.Query("SELECT insert_visit_event($1, $2, $3, $4)", visit.Shop, visit.Uid, visit.Position, vt); err != nil {
					err = errors.Wrapf(err, "")
					log.Fatal(err)
				}
			}

			if mqProd != nil {
				_, _, err = mqProd.SendMessage(&sarama.ProducerMessage{
					Topic: "visits3",
					Value: sarama.ByteEncoder([]byte(rec)),
				})
				if err != nil {
					err = errors.Wrap(err, "")
					log.Fatal(err)
				}
			}
		}
	}

	log.Infof("Replayed %v visit records", numFetched)
	return
}
