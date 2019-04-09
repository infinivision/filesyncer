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

type AgeGender struct {
	Age    int
	IsMale int
}

type User struct {
	Uid        string
	Pictures   int
	AgeMean    float64
	AgeVar     float64
	GenderMean float64
	GenderVar  float64
}

type UserSorter struct {
	Users []User
}

// Less is part of sort.Interface.
func (s *UserSorter) Less(i, j int) bool {
	if s.Users[i].GenderVar < s.Users[j].GenderVar {
		return true
	}
	if s.Users[j].GenderVar < s.Users[i].GenderVar {
		return false
	}
	if s.Users[i].AgeVar < s.Users[j].AgeVar {
		return true
	}
	if s.Users[j].AgeVar < s.Users[i].AgeVar {
		return false
	}
	if s.Users[i].Pictures < s.Users[j].Pictures {
		return true
	}
	if s.Users[j].Pictures < s.Users[i].Pictures {
		return false
	}
	if s.Users[i].Uid < s.Users[j].Uid {
		return true
	}
	return false
}

var (
	redisAddr    = flag.String("redis-addr", "127.0.0.1:6379", "Addr: redis address")
	ossAddr      = flag.String("addr-oss", "127.0.0.1:9000", "Addr: oss server")
	ossKey       = flag.String("oss-key", "HELLO", "oss client access key")
	ossSecretKey = flag.String("oss-secret-key", "WORLD", "oss client access secret key")
	ossUseSSL    = flag.Bool("oss-ssl", false, "oss client use ssl")
	ossBucket    = flag.String("oss-bucket", "images", "oss bucket name")
	dayStart     = flag.String("day-start", "", "Datatime: day start")
	dayEnd       = flag.String("day-end", "", "Datatime: day end")

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
	if *dayStart != "" {
		var tmpT time.Time
		if tmpT, err = time.Parse(time.RFC3339, *dayStart); err != nil {
			log.Fatal(err)
		}
		tsStart = tmpT.Unix()
	}
	if *dayEnd != "" {
		var tmpT time.Time
		if tmpT, err = time.Parse(time.RFC3339, *dayEnd); err != nil {
			log.Fatal(err)
		}
		tsEnd = tmpT.Unix()
	}
	if tsStart >= tsEnd {
		log.Fatal("invalid time range")
	}

	que := "visit_queue"
	var qLen int64
	if qLen, err = rcli.LLen(que).Result(); err != nil {
		log.Fatal(err)
	}

	idxStart := sort.Search(int(qLen), func(i int) bool {
		var recs []string
		if recs, err = rcli.LRange(que, int64(i), int64(i)).Result(); err != nil {
			log.Fatal(err)
		}
		var visit server.Visit
		if err = visit.Unmarshal([]byte(recs[0])); err != nil {
			log.Fatal(err)
		}
		return int64(visit.VisitTime) >= tsStart
	})
	idxEnd := sort.Search(int(qLen), func(i int) bool {
		var recs []string
		if recs, err = rcli.LRange(que, int64(i), int64(i)).Result(); err != nil {
			log.Fatal(err)
		}
		var visit server.Visit
		if err = visit.Unmarshal([]byte(recs[0])); err != nil {
			log.Fatal(err)
		}
		return int64(visit.VisitTime) >= tsEnd
	})
	if idxEnd <= idxStart {
		log.Infof("fetched no pictures")
		return
	}

	var recs []string
	if recs, err = rcli.LRange(que, int64(idxStart), int64(idxEnd-1)).Result(); err != nil {
		log.Fatal(err)
	}
	for _, rec := range recs {
		var visit server.Visit
		if err = visit.Unmarshal([]byte(rec)); err != nil {
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

	log.Infof("fetched %v pictures", len(recs))
	return
}
