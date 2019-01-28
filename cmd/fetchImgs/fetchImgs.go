package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	ossAddr      = flag.String("addr-oss", "127.0.0.1:9000", "Addr: oss server")
	ossKey       = flag.String("oss-key", "HELLO", "oss client access key")
	ossSecretKey = flag.String("oss-secret-key", "WORLD", "oss client access secret key")
	ossUseSSL    = flag.Bool("oss-ssl", false, "oss client use ssl")
	ossBucket    = flag.String("oss-bucket", "images", "oss bucket name")

	input  = flag.String("input", "faceserver.log", "faceserver log file")
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

	uids := make(map[string]int)
	//2018/11/22 11:30:16.183633 [info] objID: a631f8a83ef140aa96ccc42836cb61f5, visit3: &Visit{Uid:3885,VisitTime:1542857407,Shop:1,Position:1,Age:27,IsMale:true,}
	myExp := regexp.MustCompile("objID: (?P<objID>[0-9a-f]+), visit3:.*Uid:(?P<Uid>[0-9]+),")
	var logf *os.File
	if logf, err = os.Open(*input); err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("got error %+v", err)
	}
	scanner := bufio.NewScanner(logf)
	for scanner.Scan() {
		match := myExp.FindStringSubmatch(scanner.Text())
		if match != nil {
			log.Infof("%v, %v", match[1], match[2])
			objID := match[1]
			uid := match[2]
			if _, found := uids[uid]; !found {
				uids[uid] = 1
				if err = os.MkdirAll(fmt.Sprintf("%v/uid_%v", *output, uid), 0755); err != nil {
					err = errors.Wrap(err, "")
					log.Fatalf("got error %+v", err)
				}
			}
			fp := fmt.Sprintf("%v/uid_%v/%v.jpg", *output, uid, objID)
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

	if err = scanner.Err(); err != nil {
		err = errors.Wrap(err, "")
		log.Fatal(err)
	}
	return
}
