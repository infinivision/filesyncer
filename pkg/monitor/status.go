package monitor

import (
	"io"
	"os"

	"github.com/fagongzi/log"
	"github.com/infinivision/filesyncer/pkg/pb"
)

type step int

var (
	prepare   = step(0)
	uploading = step(1)
	complete  = step(2)
)

type status struct {
	id      uint64
	prepare *pb.InitUploadReq
	to      string
	file    string
	fd      *os.File
	step    step
	nextIdx int32
	retries int
}

func (stat *status) retry() {
	stat.retries++
}

func (stat *status) adjustChunkIdx(chunkSize int64, idx int32) {
	if idx == stat.nextIdx {
		return
	}

	if idx > stat.nextIdx {
		log.Fatalf("bug: failed chunk idx %d, expect <=%d",
			idx,
			stat.nextIdx)
		return
	}

	// Because we driven by uploadRsp, so at most seek 1 chunkSize
	stat.fd.Seek(-chunkSize, 1)
	stat.nextIdx = idx
}

func (stat *status) read(chunkSize int64) ([]byte, int32, error) {
	data := make([]byte, chunkSize, chunkSize)
	n, err := stat.fd.Read(data)
	if err != nil && err != io.EOF {
		log.Errorf("read %s for %d chunk failed, errors:%+v",
			stat.file,
			stat.nextIdx)
		return nil, 0, err
	}

	idx := int32(stat.nextIdx)
	stat.nextIdx++
	return data[0:n], idx, nil
}

func (stat *status) isComplete() bool {
	return int32(stat.nextIdx) == stat.prepare.ChunkCount
}

func (stat *status) close(remove bool) {
	if stat.fd != nil {
		stat.fd.Close()
		log.Debugf("fd closed %s",
			stat.file)
		if remove {
			err := os.Remove(stat.file)
			if err != nil {
				log.Errorf("remove %s failed, errors:%+v",
					stat.file,
					err)
			} else {
				log.Debugf("removed %s",
					stat.file)
			}
		}
	}
}
