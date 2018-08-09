package server

import (
	"math/rand"
	"strings"

	"github.com/fagongzi/log"
	"github.com/hudl/fargo"
	"github.com/pkg/errors"
)

const (
	MacLen = 12
)

type CmdbApi struct {
	eurekaAddr string
	eurekaApp  string
	conn       fargo.EurekaConnection
	app        *fargo.Application
}

func NewCmdbApi(eurekaAddr, eurekaApp string) (ca *CmdbApi, err error) {
	ca = &CmdbApi{
		eurekaAddr: eurekaAddr,
		eurekaApp:  eurekaApp,
	}
	addrs := strings.Split(eurekaAddr, ",")
	ca.conn = fargo.NewConn(addrs...)
	if ca.app, err = ca.conn.GetApp(eurekaApp); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	// starts a goroutine that updates the application on poll interval
	ca.conn.UpdateApp(ca.app)
	return
}

func (ca *CmdbApi) GetShop(mac string) (shop uint64, found bool, err error) {
	// TODO: read-write race condition of *ca.app?
	instances := ca.app.Instances
	if len(instances) == 0 {
		err = errors.Errorf("%s instances are empty", ca.eurekaApp)
		return
	}
	ins := instances[rand.Int()%len(instances)]
	// /terminals?deviceId=xxx
	log.Debugf("ins %+v", ins)

	//	err = errors.Wrap(err, "")
	return
}

func (ca *CmdbApi) GetPosition(mac, cameraIp string) (shop uint64, pos uint32, found bool, err error) {
	// TODO: read-write race condition of *ca.app?
	instances := ca.app.Instances
	if len(instances) == 0 {
		err = errors.Errorf("%s instances are empty", ca.eurekaApp)
		return
	}
	ins := instances[rand.Int()%len(instances)]
	// /terminals?deviceId=xxx
	log.Debugf("ins %+v", ins)

	//	err = errors.Wrap(err, "")
	return
}
