package server

import (
	"fmt"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/codec"
	"github.com/infinivision/filesyncer/pkg/pb"
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
}

func (s *session) onReq(msg interface{}) {
	if req, ok := msg.(*pb.Handshake); ok {
		s.handshake(req)
	} else if req, ok := msg.(*pb.InitUploadReq); ok {
		s.filesizeHistogram.Observe(float64(req.ContentLength))
		s.initUpload(req)
	} else if req, ok := msg.(*pb.UploadReq); ok {
		s.upload(req)
	} else if req, ok := msg.(*pb.UploadContinue); ok {
		s.uploadContinue(req)
	} else if req, ok := msg.(*pb.UploadCompleteReq); ok {
		s.uploadComplete(req)
	} else if msg == codec.HB {
		s.heartbeatCount.Inc()
		s.doRsp(msg)
	}
}

func (s *session) handshake(req *pb.Handshake) {
	s.mac = req.Mac
	s.heartbeatCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: fmt.Sprintf("terminal_heartbeat_%s", s.mac),
		Help: "terminal hearbeat count",
	})
	s.filesizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    fmt.Sprintf("terminal_filesize_%s", s.mac),
		Help:    "terminal filesize distributions.",
		Buckets: prometheus.LinearBuckets(0, 10240, 100), //100 buckets, each is 10K.
	})
	prometheus.MustRegister(s.heartbeatCount)
	prometheus.MustRegister(s.filesizeHistogram)
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
