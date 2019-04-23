package main

import (
	"time"

	"github.com/infinivision/filesyncer/pkg/server"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type Recorder struct {
	destPgUrl string
	db        *sqlx.DB
}

func NewRecorder(destPgUrl string) (rcd *Recorder, err error) {
	// this Pings the database trying to connect, panics on error
	// use sqlx.Open() for sql.Open() semantics
	var db *sqlx.DB
	db, err = sqlx.Connect("postgres", destPgUrl)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	db.SetMaxOpenConns(1)
	rcd = &Recorder{
		destPgUrl: destPgUrl,
		db:        db,
	}
	return
}

func (this *Recorder) Record(visits []*server.Visit) (err error) {
	for _, visit := range visits {
		vt := time.Unix(int64(visit.VisitTime), 0).Format(time.RFC3339)
		// Note: If db.Query is used, then the connection will not be released to pool since the cursor is not closed.
		if _, err = this.db.Exec("SELECT insert_user($1, $2, $3, $4, $5, $6)", visit.Uid, visit.PictureId, visit.Quality, visit.Gender, visit.Age, vt); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if _, err = this.db.Exec("SELECT insert_visit_event($1, $2, $3, $4, $5, $6)", visit.Shop, visit.Uid, visit.Position, visit.Gender, visit.Age, vt); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}
