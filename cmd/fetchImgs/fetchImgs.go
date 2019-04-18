package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-redis/redis"
	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	redisAddr    = flag.String("redis-addr", "127.0.0.1:6379", "Addr: redis address")
	ossAddr      = flag.String("addr-oss", "127.0.0.1:9000", "Addr: oss server")
	ossKey       = flag.String("oss-key", "HELLO", "oss client access key")
	ossSecretKey = flag.String("oss-secret-key", "WORLD", "oss client access secret key")
	ossUseSSL    = flag.Bool("oss-ssl", false, "oss client use ssl")
	ossBucket    = flag.String("oss-bucket", "images", "oss bucket name")
	dateStart    = flag.String("date-start", "", "Datatime: date start in RFC3339 format. For example: 2019-03-01T00:00:00+08:00")
	dateEnd      = flag.String("date-end", "", "Datatime: date end in RFC3339 format")
	uids         = flag.String("uids", "", "interested user ids. Empyt means all. For example: 1226,3495")
	output       = flag.String("output", "", "output directory")
)

func s3Get(srv *s3.S3, key string) (value []byte, err error) {
	var out *s3.GetObjectOutput
	out, err = srv.GetObject(&s3.GetObjectInput{
		Bucket: ossBucket,
		Key:    &key,
	})
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if value, err = ioutil.ReadAll(out.Body); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func main() {
	var err error
	flag.Parse()
	if *output == "" {
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		*output = usr.HomeDir
	}
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

	var sess *session.Session
	sess = session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(*ossKey, *ossSecretKey, ""),
		Endpoint:         aws.String(*ossAddr),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String("default"),
	}))
	srv := s3.New(sess)

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

			uid := visit.Uid
			visitTime := visit.VisitTime
			gender := visit.Gender
			age := visit.Age
			objID := visit.PictureId
			fp := fmt.Sprintf("%v/%d_%d_%d_%d_%v.jpg", *output, uid, visitTime, gender, age, objID)
			if _, err = os.Stat(fp); err == nil {
				log.Infof("%v already exist", fp)
				continue
			}
			var img []byte
			if img, err = s3Get(srv, objID); err != nil {
				log.Fatalf("got error %+v", err)
			}
			var jpg *os.File
			if jpg, err = os.OpenFile(fp, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0755); err != nil {
				err = errors.Wrap(err, "")
				log.Fatalf("got error %+v", err)
			}
			if _, err = jpg.Write(img); err != nil {
				err = errors.Wrap(err, "")
				log.Fatalf("got error %+v", err)
			}
			jpg.Close()
		}
	}

	log.Infof("Fetched %v pictures", numFetched)
	return
}
