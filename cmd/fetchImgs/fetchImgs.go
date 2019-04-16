package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"sort"
	"time"

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

	output = flag.String("output", "", "output directory")
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
	flag.Parse()
	if *output == "" {
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		*output = usr.HomeDir
	}

	var err error
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

	var tsStart int64
	tsEnd := time.Now().Unix()
	if *dateStart != "" {
		var tmpT time.Time
		if tmpT, err = time.Parse(time.RFC3339, *dateStart); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		tsStart = tmpT.Unix()
	}
	if *dateEnd != "" {
		var tmpT time.Time
		if tmpT, err = time.Parse(time.RFC3339, *dateEnd); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		tsEnd = tmpT.Unix()
	}
	if tsStart >= tsEnd {
		err = errors.Errorf("invalid time range: %s - %s", *dateStart, *dateEnd)
		log.Fatal(err)
	}

	que := "visit_queue"
	var qLen int64
	if qLen, err = rcli.LLen(que).Result(); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatal(err)
	}

	idxStart := sort.Search(int(qLen), func(i int) bool {
		var recs []string
		if recs, err = rcli.LRange(que, int64(i), int64(i)).Result(); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		var visit server.Visit
		if err = visit.Unmarshal([]byte(recs[0])); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		return int64(visit.VisitTime) >= tsStart
	})
	idxEnd := sort.Search(int(qLen), func(i int) bool {
		var recs []string
		if recs, err = rcli.LRange(que, int64(i), int64(i)).Result(); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		var visit server.Visit
		if err = visit.Unmarshal([]byte(recs[0])); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}
		return int64(visit.VisitTime) >= tsEnd
	})
	if idxEnd <= idxStart || int64(idxStart) >= qLen {
		log.Infof("There's no pictures in the given time window.")
		return
	} else {
		log.Infof("There are %v pictures in the given time window.", idxEnd-idxStart)
	}

	var recs []string
	if recs, err = rcli.LRange(que, int64(idxStart), int64(idxEnd-1)).Result(); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatal(err)
	}
	for _, rec := range recs {
		var visit server.Visit
		if err = visit.Unmarshal([]byte(rec)); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatal(err)
		}

		objID := visit.PictureId
		uid := visit.Uid
		age := visit.Age
		gender := visit.Gender
		fp := fmt.Sprintf("%v/%v_%v_%v_%v.jpg", *output, uid, gender, age, objID)
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

	log.Infof("Fetched %v pictures", len(recs))
	return
}
