package server

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/codec"
)

type ImgMsg struct {
	Shop     uint64
	Position uint32
	ModTime  int64
	ObjID    string
	Img      []byte
}

// FileServer file server
type FileServer struct {
	sync.RWMutex

	cfg       *Cfg
	sessions  map[int64]*session
	tcpServer *goetty.Server

	ctx    context.Context
	cancel context.CancelFunc
	cmdb   *CmdbApi
	imgCh  chan<- ImgMsg
}

// NewFileServer create a file server
// The file server will received files via tcp protocol,
// and support resume data from break point.
func NewFileServer(cfg *Cfg, imgCh chan<- ImgMsg) *FileServer {
	cmdb, err := NewCmdbApi(cfg.EurekaAddr, cfg.EurekaApp)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	initG(cfg, cmdb, imgCh)
	ctx, cancel := context.WithCancel(context.Background())

	return &FileServer{
		cfg:      cfg,
		sessions: make(map[int64]*session),
		tcpServer: goetty.NewServer(cfg.Addr,
			goetty.WithServerDecoder(codec.SyncDecoder),
			goetty.WithServerEncoder(codec.SyncEncoder),
			goetty.WithServerMiddleware(goetty.NewSyncProtocolServerMiddleware(codec.FileDecoder, codec.FileEncoder, func(conn goetty.IOSession, msg interface{}) error {
				return conn.WriteAndFlush(msg)
			}))),
		ctx:    ctx,
		cancel: cancel,
		cmdb:   cmdb,
		imgCh:  imgCh,
	}
}

// Start start the file server
func (fs *FileServer) Start() error {
	return fs.tcpServer.Start(fs.doConnection)
}

// Stop stop the file server
func (fs *FileServer) Stop() {
	fs.cancel()
	fs.tcpServer.Stop()
}

var (
	rnd = rand.New(rand.NewSource(time.Now().Unix()))
)

func (fs *FileServer) doConnection(conn goetty.IOSession) error {
	addr := conn.RemoteAddr()
	log.Debugf("net: %s is connected", addr)

	s := newSession(conn)
	fs.addSession(s)

	defer func() {
		fs.removeSession(s)
		s.close()
		log.Debugf("net: %s is closed", addr)
	}()

	// read loop
	for {
		value, err := conn.ReadTimeout(fs.cfg.SessionTimeout)
		if err != nil {
			if err == io.EOF {
				return nil
			}

			log.Errorf("net: %s read failed, errors: %+v",
				addr,
				err)
			return err
		}

		log.Debugf("net: %s read (%T)",
			addr,
			value)

		if err := s.onReq(value); err != nil {
			return err
		}
	}
}

func (fs *FileServer) addSession(s *session) {
	fs.Lock()
	defer fs.Unlock()

	fs.sessions[s.id] = s
}

func (fs *FileServer) removeSession(s *session) {
	fs.Lock()
	defer fs.Unlock()

	delete(fs.sessions, s.id)
}
