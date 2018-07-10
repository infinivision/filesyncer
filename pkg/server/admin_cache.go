package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const (
	MacLen = 12
)

type AdminCache struct {
	rwlock  sync.RWMutex
	shopMap map[string]uint64   //mac -> shop id
	cameMap map[string][]Camera //mac -> cameras
	posMap  map[string]uint32   //mac + camera name -> position

	db        *sqlx.DB
	termSql   string
	cameraSql string
}

type terminal struct {
	Mac     string
	Shop_id uint64
}

type Camera struct {
	Mac_id   string
	Name     string
	Username string
	Password string
	Position uint32
}

func NewAdminCache(addr, username, password, database string) (ac *AdminCache, err error) {
	var db *sqlx.DB
	dataSource := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, addr, database)
	if db, err = sqlx.Connect("mysql", dataSource); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	ac = &AdminCache{
		db:        db,
		termSql:   fmt.Sprintf("SELECT mac, shop_id FROM iot_terminal"),
		cameraSql: fmt.Sprintf("SELECT mac_id, name, username, password, position FROM iot_camera"),
	}
	err = ac.flush()
	return
}

func (this *AdminCache) UpdateLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err = this.flush(); err != nil {
				log.Errorf("%+v", err)
			}
		}
	}

}

// This shall be invoked regularly
func (this *AdminCache) flush() (err error) {
	terms := []terminal{}
	cames := []Camera{}
	if err = this.db.Select(&terms, this.termSql); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if err = this.db.Select(&cames, this.cameraSql); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	shopMap2 := make(map[string]uint64)
	for _, term := range terms {
		shopMap2[term.Mac] = term.Shop_id
	}
	cameMap2 := make(map[string][]Camera)
	posMap2 := make(map[string]uint32)
	var found bool
	for _, came := range cames {
		var cameras []Camera
		if cameras, found = cameMap2[came.Mac_id]; found {
			cameras = append(cameras, came)
		} else {
			cameras = []Camera{came}
		}
		cameMap2[came.Mac_id] = cameras
		posMap2[fmt.Sprintf("%s-%s", came.Mac_id, came.Name)] = came.Position
	}
	this.rwlock.Lock()
	this.shopMap = shopMap2
	this.cameMap = cameMap2
	this.posMap = posMap2
	this.rwlock.Unlock()
	log.Infof("got %d terminals, %d cameras", len(terms), len(cames))
	return
}

func (this *AdminCache) GetShop(macs string) (shopId uint64, mac string, found bool) {
	this.rwlock.RLock()
	macsLen := len(macs)
	for j := 0; j < macsLen; j += MacLen {
		if j+MacLen > macsLen {
			continue
		}
		mac = macs[j : j+MacLen]
		if shopId, found = this.shopMap[mac]; found {
			break
		}
	}
	this.rwlock.RUnlock()
	return
}

func (this *AdminCache) GetCameras(mac string) (cameras []Camera, found bool) {
	this.rwlock.RLock()
	cameras, found = this.cameMap[mac]
	this.rwlock.RUnlock()
	return
}

func (this *AdminCache) GetPosition(mac, name string) (position uint32, found bool) {
	cameraKey := fmt.Sprintf("%s-%s", mac, name)
	this.rwlock.RLock()
	position, found = this.posMap[cameraKey]
	this.rwlock.RUnlock()
	return
}
