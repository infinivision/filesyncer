package server

import (
	"fmt"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/codec"
	"github.com/infinivision/filesyncer/pkg/pb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type session struct {
	addr string
	id   int64
	fid  int32
	conn goetty.IOSession

	mac string

	//metrics per session
	heartbeatCount    prometheus.Counter
	filesizeHistogram prometheus.Histogram
}

func newSession(conn goetty.IOSession) *session {
	return &session{
		addr: conn.RemoteAddr(),
		id:   conn.ID().(int64),
		conn: conn,
	}
}

func (s *session) close() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.heartbeatCount != nil {
		prometheus.Unregister(s.heartbeatCount)
		prometheus.Unregister(s.filesizeHistogram)
	}
}

func (s *session) onReq(msg interface{}) (error){
	if req, ok := msg.(*pb.Handshake); ok {
		return s.handshake(req)
	} else if req, ok := msg.(*pb.InitUploadReq); ok {
		if s.filesizeHistogram != nil {
			s.filesizeHistogram.Observe(float64(req.ContentLength))
		}
		s.initUpload(req)
	} else if req, ok := msg.(*pb.UploadReq); ok {
		s.upload(req)
	} else if req, ok := msg.(*pb.UploadContinue); ok {
		s.uploadContinue(req)
	} else if req, ok := msg.(*pb.UploadCompleteReq); ok {
		s.uploadComplete(req)
	} else if msg == codec.HB {
		if s.heartbeatCount != nil {
			s.heartbeatCount.Inc()
		}
		s.doRsp(msg)
	}
	return nil
}

func (s *session) handshake(req *pb.Handshake) (err error){
	var shop uint64
	var mac string
	var cameras []string
	var found bool
	if shop, mac, found = fileMgr.adminCache.GetShop(req.Mac); !found {
		err = errors.Errorf("cannot determine shop id for mac %s", req.Mac)
		return
	}
	if cameras, found = fileMgr.adminCache.GetCameras(mac); !found {
		err = errors.Errorf("cannot determine cameras for mac %s", req.Mac);
		return
	}
	if s.heartbeatCount == nil {
		s.heartbeatCount = prometheus.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("terminal_heartbeat_%s", req.Mac),
			Help: "terminal hearbeat count",
		})
		s.filesizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    fmt.Sprintf("terminal_filesize_%s", req.Mac),
			Help:    "terminal filesize distributions.",
			Buckets: prometheus.LinearBuckets(0, 10240, 100), //100 buckets, each is 10K.
		})
		prometheus.MustRegister(s.heartbeatCount)
		prometheus.MustRegister(s.filesizeHistogram)
	}
	s.mac = mac
	s.doRsp(&pb.HandshakeRsp{
		Shop:  shop,
		Mac:   mac,
		Cameras: cameras,
	})
	return
}

func (s *session) initUpload(req *pb.InitUploadReq) {
	s.doRsp(&pb.InitUploadRsp{
		Seq:  req.Seq,
		ID:   fileMgr.addFile(req),
		Code: pb.CodeSucc,
	})
}

func (s *session) upload(req *pb.UploadReq) {
	s.doRsp(&pb.UploadRsp{
		ID:    req.ID,
		Index: req.Index,
		Code:  fileMgr.appendFile(req),
	})
}

func (s *session) uploadContinue(req *pb.UploadContinue) {
	exists, idx := fileMgr.continueUpload(req.ID)
	if !exists {
		s.doRsp(&pb.UploadRsp{
			ID:   req.ID,
			Code: pb.CodeMissing,
		})
		return
	}

	s.doRsp(&pb.UploadRsp{
		ID:    req.ID,
		Index: idx,
		Code:  pb.CodeSucc,
	})
}

func (s *session) uploadComplete(req *pb.UploadCompleteReq) {
	s.doRsp(&pb.UploadCompleteRsp{
		ID:   req.ID,
		Code: fileMgr.completeFile(req, s.mac),
	})
}

func (s *session) doRsp(rsp interface{}) {
	log.Debugf("net: %s sent (%T)%+v",
		s.addr,
		rsp,
		rsp)

	s.conn.WriteAndFlush(rsp)
}
