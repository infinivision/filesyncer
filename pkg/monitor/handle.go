package monitor

import (
	"os"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/pb"
)

func (m *Monitor) addFile(file string) {
	log.Debugf("upload: new file add: %s", file)
	m.readyC <- file
	log.Debugf("upload: new file added: %s", file)
}

func (m *Monitor) handlePrepare(file string) {
	info, err := os.Stat(file)
	if err != nil {
		log.Errorf("upload-pre: stat %s failed, errors:%+v",
			file,
			err)
		return
	}

	fd, err := os.OpenFile(file, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		log.Errorf("upload-pre: open %s failed, errors:%+v",
			file,
			err)
		return
	}

	fileSize := info.Size()
	cnt := fileSize / int64(m.cfg.Chunk)
	if fileSize%int64(m.cfg.Chunk) > 0 {
		cnt++
	}

	seq := m.fileSeq.Incr()
	stat := &status{
		file: file,
		fd:   fd,
		prepare: &pb.InitUploadReq{
			Seq:           seq,
			ContentLength: fileSize,
			ChunkCount:    int32(cnt),
		},
		step: prepare,
		to:   m.nextAvailable(),
	}

	m.prepares.Store(seq, stat)
	m.sendInit(stat.prepare)
}

func (m *Monitor) handleInitUploadRsp(msg *pb.InitUploadRsp) {
	stat := m.getPrepareStat(msg.Seq)
	m.prepares.Delete(msg.Seq)

	stat.id = msg.ID
	stat.step = uploading
	m.uploadings.Store(stat.id, stat)

	m.handleNextChunk(stat)
}

func (m *Monitor) handleUploadRsp(msg *pb.UploadRsp) {
	if msg.Code == pb.CodeInvalidChunk {
		log.Fatal("bug: invalid chunk index")
	} else if msg.Code == pb.CodeMissing {
		stat := m.getUploadingStat(msg.ID)
		m.uploadings.Delete(msg.ID)
		stat.close(false)

		// retry with init upload, and choose another server
		m.addFile(stat.file)
		return
	}

	stat := m.getUploadingStat(msg.ID)
	stat.adjustChunkIdx(m.cfg.Chunk, msg.Index+1)

	if stat.isComplete() {
		m.sendUploading(stat.id, &pb.UploadCompleteReq{
			ID: stat.id,
		})
		return
	}

	m.handleNextChunk(stat)
}

func (m *Monitor) handleUploadCompleteRsp(msg *pb.UploadCompleteRsp) {
	stat := m.getUploadingStat(msg.ID)
	m.uploadings.Delete(msg.ID)

	if msg.Code == pb.CodeOSSError ||
		msg.Code == pb.CodeMaxRetries ||
		msg.Code == pb.CodeMissing {
		stat.close(false)
		// retry with init upload, and choose another server
		m.addFile(stat.file)
		return
	}

	stat.close(true)
	m.completeNotify()
}

func (m *Monitor) handleNextChunk(stat *status) {
	data, idx, err := stat.read(m.cfg.Chunk)
	if err != nil {
		m.uploadings.Delete(stat.id)
		return
	}

	m.sendUploading(stat.id, &pb.UploadReq{
		ID:    stat.id,
		Index: idx,
		Data:  data,
	})
}

func (m *Monitor) sendInit(msg *pb.InitUploadReq) {
	stat := m.getPrepareStat(msg.Seq)
	err := m.doSend(stat.to, msg)
	if err != nil {
		// retry and rechoose a server immediate
		m.addFile(stat.file)
		return
	}
}

func (m *Monitor) sendUploading(id uint64, msg interface{}) {
	stat := m.getUploadingStat(id)
	for {
		err := m.doSend(stat.to, msg)
		if err == nil {
			break
		}

		if stat.retries > m.cfg.RetriesPerServer {
			log.Errorf("write-upload: %s retries %d times, ignore",
				stat.file,
				stat.retries)
			m.uploadings.Delete(id)
			stat.close(false)

			// retry with init upload, and choose another server
			m.addFile(stat.file)
			break
		}

		stat.retry()
		time.Sleep(m.cfg.RetriesInterval)
	}
}

func (m *Monitor) retryPrepareConnectionClosed(addr string) {
	var retries []*status
	var removed []interface{}
	m.prepares.Range(func(key, value interface{}) bool {
		if stat := value.(*status); stat.to == addr {
			removed = append(removed, key)
			retries = append(retries, stat)
		}

		return true
	})

	for _, key := range removed {
		m.prepares.Delete(key)
	}

	for _, stat := range retries {
		// retry and rechoose a server immediate
		m.addFile(stat.file)
	}
}

func (m *Monitor) retryUploadingsConnectionClosed(addr string) {
	var retries []*status
	m.uploadings.Range(func(key, value interface{}) bool {
		if stat := value.(*status); stat.to == addr {
			retries = append(retries, stat)
		}

		return true
	})

	for _, stat := range retries {
		// try continue
		m.sendUploading(stat.id, &pb.UploadContinue{
			ID: stat.id,
		})
	}
}
