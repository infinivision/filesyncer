package monitor

import (
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/codec"
	"github.com/infinivision/filesyncer/pkg/pb"
)

func (m *Monitor) connFactory(addr string) goetty.IOSession {
	return goetty.NewConnector(addr,
		goetty.WithClientConnectTimeout(m.cfg.TimeoutConnect),
		goetty.WithClientDecoder(codec.SyncDecoder),
		goetty.WithClientEncoder(codec.SyncEncoder),
		goetty.WithClientWriteTimeoutHandler(m.cfg.TimeoutWrite, func(addr string, conn goetty.IOSession) {
			defer func() {
				if err := recover(); err != nil {
					log.Warnf("net: %s is closed", err)
				}
			}()

			log.Debugf("net: sent HB to %s", addr)
			conn.WriteAndFlush(codec.HB)
		}, m.tw),
		goetty.WithClientMiddleware(goetty.NewSyncProtocolClientMiddleware(codec.FileDecoder, codec.FileEncoder, m.sendRaw, 3)))
}

func (m *Monitor) sendRaw(conn goetty.IOSession, msg interface{}) error {
	return conn.WriteAndFlush(msg)
}

// ConnectFailed pool status handler
func (m *Monitor) ConnectFailed(addr string, err error) {
	log.Errorf("net: %s connect failed, errors:%+v", addr, err)
}

// Connected pool status handler
func (m *Monitor) Connected(addr string, conn goetty.IOSession) {
	log.Infof("net: %s connected %p", addr, conn)
	go m.startReadLoop(addr, conn)
}

func (m *Monitor) startReadLoop(addr string, conn goetty.IOSession) {
	for {
		msg, err := conn.ReadTimeout(m.cfg.TimeoutRead)
		if err != nil {
			log.Errorf("net: %s %p read failed, errors:%+v",
				addr,
				conn,
				err)

			if m.pool.RemoveConnIfMatches(addr, conn) {
				m.retryPrepareConnectionClosed(addr)
				m.retryUploadingsConnectionClosed(addr)
			}

			return
		}

		log.Debugf("net: %s read (%T)%+v",
			addr,
			msg,
			msg)

		if value, ok := msg.(*pb.HandshakeRsp); ok {
			m.handleHandshakeRsp(value)
		} else if value, ok := msg.(*pb.InitUploadRsp); ok {
			m.handleInitUploadRsp(value)
		} else if value, ok := msg.(*pb.UploadRsp); ok {
			m.handleUploadRsp(value)
		} else if value, ok := msg.(*pb.UploadCompleteRsp); ok {
			m.handleUploadCompleteRsp(value)
		}
	}
}

func (m *Monitor) doSend(to string, msg interface{}) error {
	conn, err := m.pool.GetConn(to)
	if err != nil {
		log.Errorf("net: %s conn get failed, errors:%+v",
			to,
			err)
		return err
	}

	if !m.handShakeSent {
		m.handShakeSent = true
		handshake := &pb.Handshake{Mac: m.cfg.ID}
		if err = m.doSend(to, handshake); err != nil {
			log.Errorf("net: %s failed to send handshake, errors:%+v",
				to,
				err)
			return err
		}
	}

	err = conn.WriteAndFlush(msg)
	if err != nil {
		log.Errorf("net: %s sent (%T)%+v failed, errors:%+v",
			to,
			msg,
			msg,
			err)
		// If send failed, close connection, and this connection will reconnect when retry
		conn.Close()
		return err
	}

	log.Debugf("net: %s sent (%T)",
		to,
		msg)
	return nil
}
