package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"regexp"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/montanaflynn/stats"
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

	uids := make(map[string][]AgeGender)
	//2018/11/22 11:30:16.183633 [info] objID: a631f8a83ef140aa96ccc42836cb61f5, visit3: &Visit{Uid:3885,VisitTime:1542857407,Shop:1,Position:1,Age:27,IsMale:true,}
	myExp := regexp.MustCompile("objID: (?P<objID>[0-9a-f]+), visit3:.*Uid:(?P<Uid>[0-9]+),.*Age:(?P<Age>[0-9]+),IsMale:(?P<IsMale>true|false)")
	var logf *os.File
	if logf, err = os.Open(*input); err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("got error %+v", err)
	}
	scanner := bufio.NewScanner(logf)
	for scanner.Scan() {
		match := myExp.FindStringSubmatch(scanner.Text())
		if match != nil {
			log.Infof("%v, %v, %v, %v", match[1], match[2], match[3], match[4])
			objID := match[1]
			uid := match[2]
			var age int
			if age, err = strconv.Atoi(match[3]); err != nil {
				err = errors.Wrap(err, "")
				log.Fatalf("got error %+v", err)
			}
			var isMale int
			if match[4] == "true" {
				isMale = 1
			}
			ag := AgeGender{
				Age:    age,
				IsMale: isMale,
			}
			var ags []AgeGender
			var found bool
			if ags, found = uids[uid]; !found {
				if err = os.MkdirAll(fmt.Sprintf("%v/uid_%v", *output, uid), 0755); err != nil {
					err = errors.Wrap(err, "")
					log.Fatalf("got error %+v", err)
				}
			}
			ags = append(ags, ag)
			uids[uid] = ags
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

	var users []User
	fmt.Println("uid, pictures, age mean, age variance, gender mean, gender variance")
	for uid, ags := range uids {
		var ages []float64
		var isMales []float64
		for _, ag := range ags {
			ages = append(ages, float64(ag.Age))
			isMales = append(isMales, float64(ag.IsMale))
		}
		ageMean, _ := stats.Mean(ages)
		ageVar, _ := stats.Variance(ages)
		genderMean, _ := stats.Mean(isMales)
		genderVar, _ := stats.Variance(isMales)
		//fmt.Printf("%v\t%v\t%.2f\t%.2f\t%.2f\t%.2f\n", uid, len(ags), ageMean, ageVar, genderMean, genderVar)
		user := User{
			Uid:        uid,
			Pictures:   len(ags),
			AgeMean:    ageMean,
			AgeVar:     ageVar,
			GenderMean: genderMean,
			GenderVar:  genderVar,
		}
		users = append(users, user)
	}
	userSorter := UserSorter{
		Users: users,
	}
	sort.Slice(userSorter.Users, userSorter.Less)
	for _, user := range userSorter.Users {
		fmt.Printf("%v\t%v\t%.2f\t%.2f\t%.2f\t%.2f\n", user.Uid, user.Pictures, user.AgeMean, user.AgeVar, user.GenderMean, user.GenderVar)
	}
	return
}
