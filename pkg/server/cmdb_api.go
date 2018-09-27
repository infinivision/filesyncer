package server

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fagongzi/log"
	"github.com/hudl/fargo"
	"github.com/pkg/errors"
)

const (
	MacLen = 12
)

type Hardware struct {
	Ip   string `json:"ip"`
	Meta string `json:"Meta"`
}

type Item struct {
	DeviceId  string     `json:"deviceId"`
	AreaId    string     `json:"areaId"`
	Hardwares []Hardware `json:"hardwares"`
}

type Data struct {
	Items []Item `json:"items"`
	Total string `json:"total"`
}

type TermsRespBody struct {
	Code string `json:"code"`
	Data `json:"data"`
}

type CmdbApi struct {
	eurekaAddr string
	eurekaApp  string
	conn       fargo.EurekaConnection
	app        *fargo.Application
	hc         *http.Client
}

func NewCmdbApi(eurekaAddr, eurekaApp string) (ca *CmdbApi, err error) {
	ca = &CmdbApi{
		eurekaAddr: eurekaAddr,
		eurekaApp:  eurekaApp,
		hc:         &http.Client{Timeout: time.Duration(10) * time.Second},
	}
	addrs := strings.Split(eurekaAddr, ",")
	ca.conn = fargo.NewConn(addrs...)
	if ca.app, err = ca.conn.GetApp(eurekaApp); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	regInfo, _ := json.Marshal(ca.app)
	log.Infof("Application %v in Eureka: %+v", eurekaApp, string(regInfo))
	// starts a goroutine that updates the application on poll interval
	ca.conn.PollInterval = 30
	ca.conn.UpdateApp(ca.app)
	return
}

func (ca *CmdbApi) GetShop(mac string) (shop uint64, found bool, err error) {
	var terms *TermsRespBody
	if terms, found, err = ca.getTerm(mac); err != nil || !found {
		return
	}
	if shop, err = strconv.ParseUint(terms.Data.Items[0].AreaId, 10, 64); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

func (ca *CmdbApi) GetPosition(mac, cameraIp string) (shop uint64, pos uint32, found bool, err error) {
	var terms *TermsRespBody
	if terms, found, err = ca.getTerm(mac); err != nil || !found {
		return
	}
	if shop, err = strconv.ParseUint(terms.Data.Items[0].AreaId, 10, 64); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	found = false
	for _, hw := range terms.Data.Items[0].Hardwares {
		if hw.Ip == cameraIp {
			found = true
			fields := strings.Split(hw.Meta, ",")
			for _, field := range fields {
				kv := strings.Split(field, "=")
				if len(kv) == 2 && kv[0] == "position" {
					var pos2 uint64
					if pos2, err = strconv.ParseUint(kv[1], 10, 64); err != nil {
						err = errors.Wrapf(err, "")
						return
					}
					pos = uint32(pos2)
				}
			}
		}

	}
	if !found {
		log.Debugf("terms %+v", terms)
	}
	return
}

func (ca *CmdbApi) getTerm(mac string) (terms *TermsRespBody, found bool, err error) {
	// TODO: read-write race condition of *ca.app?
	instances := ca.app.Instances
	if len(instances) == 0 {
		err = errors.Errorf("%s instances are empty", ca.eurekaApp)
		return
	}
	ins := instances[rand.Int()%len(instances)]

	var servURL string
	if servURL, err = JoinURL(ins.HomePageUrl, "/terminals"); err != nil {
		return
	}
	req, err := http.NewRequest("GET", servURL, nil)
	// https://stackoverflow.com/questions/30652577/go-doing-a-get-request-and-building-the-querystring/30657518
	q := req.URL.Query()
	q.Set("deviceId", mac)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("__no_auth__", "foo")
	log.Debugf("request url: %+v", req.URL.String())
	var resp *http.Response
	var respBody []byte
	if resp, err = ca.hc.Do(req); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer resp.Body.Close()
	if respBody, err = ioutil.ReadAll(resp.Body); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	log.Debugf("respBody: %+v", string(respBody))
	terms = &TermsRespBody{}
	if err = json.Unmarshal(respBody, terms); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if terms.Data.Total != "0" {
		if terms.Data.Total != "1" {
			log.Errorf("there are multiple terminals in respBody %+v", string(respBody))
		} else if terms.Data.Items[0].DeviceId != mac {
			log.Errorf("incorrect MAC, want %v, have %v, respBody %+v", mac, terms.Data.Items[0].DeviceId, string(respBody))
		} else {
			found = true
		}
	}
	log.Debugf("respBody parsed as: %+v", terms)
	return
}

//https://stackoverflow.com/questions/34668012/combine-url-paths-with-path-join
func JoinURL(base, ref string) (u string, err error) {
	var baseURL, refURL *url.URL
	if baseURL, err = url.Parse(base); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if refURL, err = url.Parse(ref); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	u = baseURL.ResolveReference(refURL).String()
	return
}
