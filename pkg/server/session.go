package server

import (
	"fmt"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/codec"
	"github.com/infinivision/filesyncer/pkg/pb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	//metrics per terminal
	termHeartbeatCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      "term_heartbeat",
			Help:      "terminal hearbeat count",
		}, []string{"shop", "mac"})
	termFilesizeHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mcd",
			Subsystem: "faceserver",
			Name:      fmt.Sprintf("terminal_filesize"),
			Help:      "terminal filesize distributions.",
			Buckets:   prometheus.LinearBuckets(0, 10240, 100), //100 buckets, each is 10K.
		}, []string{"shop", "mac"})
	termMetricOnce sync.Once
)

func initMetricsForTerms() {
	prometheus.MustRegister(termHeartbeatCountVec)
	prometheus.MustRegister(termFilesizeHistogramVec)
}

type session struct {
	addr string
	id   int64
	fid  int32
	conn goetty.IOSession

	shop string
	mac  string
}

func newSession(conn goetty.IOSession) *session {
	termMetricOnce.Do(initMetricsForTerms)
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

func (s *session) onReq(msg interface{}) error {
	if req, ok := msg.(*pb.Handshake); ok {
		return s.handshake(req)
	} else if req, ok := msg.(*pb.InitUploadReq); ok {
		termFilesizeHistogramVec.WithLabelValues(s.shop, s.mac).Observe(float64(req.ContentLength))
		s.initUpload(req)
	} else if req, ok := msg.(*pb.UploadReq); ok {
		s.upload(req)
	} else if req, ok := msg.(*pb.UploadContinue); ok {
		s.uploadContinue(req)
	} else if req, ok := msg.(*pb.UploadCompleteReq); ok {
		s.uploadComplete(req)
	} else if msg == codec.HB {
		termHeartbeatCountVec.WithLabelValues(s.shop, s.mac).Inc()
		s.doRsp(msg)
	}
	return nil
}

func (s *session) handshake(req *pb.Handshake) (err error) {
	var shop uint64
	var found bool
	if shop, found, err = fileMgr.cmdb.GetShop(req.Mac); err != nil {
		return
	} else if !found {
		err = errors.Errorf("cannot determine shop id for mac %s", req.Mac)
		return
	}
	s.shop = fmt.Sprintf("%d", shop)
	s.mac = req.Mac
	s.doRsp(&pb.HandshakeRsp{})
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
