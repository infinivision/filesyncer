package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	io "io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sort"
	"time"

	"github.com/fagongzi/log"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

func PostFiles(hc *http.Client, servURL string, filenames []string, imgs [][]byte, respObj interface{}) (duration time.Duration, err error) {
	var resp *http.Response
	var part io.Writer
	t0 := time.Now()
	reqBody := &bytes.Buffer{}
	writer := multipart.NewWriter(reqBody)
	for i, img := range imgs {
		//part, err := writer.CreateFormFile("data", "image.jpg") //generates "Content-Type: application/octet-stream"
		partHeader := textproto.MIMEHeader{}
		disposition := fmt.Sprintf("form-data; name=\"data\"; filename=\"%s\"", filenames[i])
		log.Debugf("disposition: %s", disposition)
		partHeader.Add("Content-Disposition", disposition)
		partHeader.Add("Content-Type", "image/jpeg")
		if part, err = writer.CreatePart(partHeader); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		if _, err = part.Write(img); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	writer.Close()
	req, err := http.NewRequest("POST", servURL, reqBody)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	if resp, err = hc.Do(req); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	duration = time.Since(t0)
	var respBody []byte
	respBody, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if err = json.Unmarshal(respBody, respObj); err != nil {
		err = errors.Wrapf(err, "failed to decode respBody: %+v", string(respBody))
		return
	}
	return
}

func GetVisitIdxRange(rcli *redis.Client, que, dateStart, dateEnd string) (idxStart, idxEnd int64, err error) {
	var tsStart int64
	tsEnd := time.Now().Unix()
	if dateStart != "" {
		var tmpT time.Time
		if tmpT, err = time.Parse(time.RFC3339, dateStart); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		tsStart = tmpT.Unix()
	}
	if dateEnd != "" {
		var tmpT time.Time
		if tmpT, err = time.Parse(time.RFC3339, dateEnd); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		tsEnd = tmpT.Unix()
	}
	if tsStart >= tsEnd {
		err = errors.Errorf("invalid time range: %s - %s", dateStart, dateEnd)
		return
	}
	var qLen int64
	if qLen, err = rcli.LLen(que).Result(); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	idxEnd = qLen

	// determine index range with binary search
	if dateStart != "" {
		idxTmp := sort.Search(int(qLen), func(i int) bool {
			var recs []string
			if recs, err = rcli.LRange(que, int64(i), int64(i)).Result(); err != nil {
				err = errors.Wrapf(err, "")
				log.Fatal(err)
			}
			var visit Visit
			if err = visit.Unmarshal([]byte(recs[0])); err != nil {
				err = errors.Wrapf(err, "")
				log.Fatal(err)
			}
			return int64(visit.VisitTime) >= tsStart
		})
		idxStart = int64(idxTmp)
	}
	if dateEnd != "" {
		idxTmp := sort.Search(int(qLen), func(i int) bool {
			var recs []string
			if recs, err = rcli.LRange(que, int64(i), int64(i)).Result(); err != nil {
				err = errors.Wrapf(err, "")
				log.Fatal(err)
			}
			var visit Visit
			if err = visit.Unmarshal([]byte(recs[0])); err != nil {
				err = errors.Wrapf(err, "")
				log.Fatal(err)
			}
			return int64(visit.VisitTime) >= tsEnd
		})
		idxEnd = int64(idxTmp)
	}
	return
}
