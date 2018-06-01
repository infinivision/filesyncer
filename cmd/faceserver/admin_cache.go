package main

import (
	"fmt"
	"sync"

	"github.com/fagongzi/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type AdminCache struct {
	rwlock  sync.RWMutex
	termMap map[string]int64

	db  *sqlx.DB
	sql string
}

type terminal struct {
	Mac     string
	Shop_id int64
}

func NewAdminCache(addr, username, password, database, table string) (ac *AdminCache, err error) {
	var db *sqlx.DB
	dataSource := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, addr, database)
	if db, err = sqlx.Connect("mysql", dataSource); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	ac = &AdminCache{
		db:  db,
		sql: fmt.Sprintf("SELECT mac, shop_id FROM %s", table),
	}
	err = ac.Flush()
	return
}

// This shall be invoked regularly
func (this *AdminCache) Flush() (err error) {
	terms := []terminal{}
	if err = this.db.Select(&terms, this.sql); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	termMap2 := make(map[string]int64)
	for _, term := range terms {
		termMap2[term.Mac] = term.Shop_id
	}
	this.rwlock.Lock()
	this.termMap = termMap2
	this.rwlock.Unlock()
	log.Infof("Flush %d terminals", len(terms))
	return
}

func (this *AdminCache) Get(mac string) (shopId int64, found bool) {
	this.rwlock.RLock()
	shopId, found = this.termMap[mac]
	this.rwlock.RUnlock()
	return
}
