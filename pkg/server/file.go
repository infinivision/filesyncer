package server

import (
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/fagongzi/log"
	"github.com/fagongzi/util/uuid"
	"github.com/infinivision/filesyncer/pkg/pb"
	"github.com/pkg/errors"
)

type fileManager struct {
	sync.RWMutex

	cfg   RetryCfg
	allc  uint64
	files map[uint64]*file

	cmdb  *CmdbApi
	imgCh chan<- ImgMsg
}

func newFileManager(cfg RetryCfg, cmdb *CmdbApi, imgCh chan<- ImgMsg) *fileManager {
	return &fileManager{
		files: make(map[uint64]*file, 1024),
		cfg:   cfg,
		cmdb:  cmdb,
		imgCh: imgCh,
	}
}

func (mgr *fileManager) addFile(req *pb.InitUploadReq) uint64 {
	log.Debugf("addFile init %d", req.Seq)
	mgr.Lock()
	fid := mgr.allc
	mgr.files[fid] = newFile(fid, req)
	mgr.allc++
	mgr.Unlock()

	log.Infof("file-%d: added with %d bytes and %d chunks",
		fid,
		req.ContentLength,
		req.ChunkCount)
	return fid
}

func (mgr *fileManager) appendFile(req *pb.UploadReq) pb.Code {
	log.Debugf("file-%d: append file", req.ID)
	mgr.Lock()

	if f, ok := mgr.files[req.ID]; ok {
		f.last = req.Index
		code := f.append(req)
		mgr.Unlock()
		log.Debugf("file-%d: append file complete", req.ID)
		return code
	}

	mgr.Unlock()
	log.Debugf("file-%d: missing", req.ID)
	return pb.CodeMissing
}

func (mgr *fileManager) continueUpload(id uint64) (bool, int32) {
	log.Debugf("file-%d: continue file", id)
	mgr.RLock()

	if f, ok := mgr.files[id]; ok {
		idx := f.last
		mgr.RUnlock()
		log.Debugf("file-%d: continue file complete", id)
		return true, idx
	}

	mgr.RUnlock()
	log.Debugf("file-%d: continue file with missing", id)
	return false, 0
}

func (mgr *fileManager) completeFile(req *pb.UploadCompleteReq) pb.Code {
	fid := req.ID

	log.Debugf("file-%d: complete file", req.ID)
	mgr.RLock()
	if f, ok := mgr.files[fid]; ok {
		times := 0
		duration := mgr.cfg.RetryInterval

		for {
			if times > 0 {
				log.Infof("file-%d: retry the %d times",
					fid,
					times)
			}

			log.Debugf("file-%d: complete file start push to oss", req.ID)
			objID, code := f.complete(req)
			log.Debugf("file-%d: complete file end push to oss", req.ID)
			if code != pb.CodeOSSError {
				if mgr.imgCh != nil {
					var shop uint64
					var position uint32
					var found bool
					var err error
					log.Debugf("file-%d: complete file start call position", req.ID)
					if shop, position, found, err = mgr.cmdb.GetPosition(f.meta.Mac, f.meta.Camera); err != nil {
						log.Warnf("GetPosition(%s, %s) failed with error %+v", f.meta.Mac, f.meta.Camera, err)
					} else if !found {
						log.Warnf("GetPosition(%s, %s) didn't find", f.meta.Mac, f.meta.Camera)
					} else {
						var img []byte
						var err error
						f.readed = 0
						if img, err = ioutil.ReadAll(f); err != nil {
							err = errors.Wrap(err, "")
							log.Errorf("%+v", err)
						} else {
							log.Debugf("got an image from shop %v, mac %v, camera %v", shop, f.meta.Mac, f.meta.Camera)
							mgr.imgCh <- ImgMsg{Shop: shop, Position: position, ModTime: f.meta.ModTime, ObjID: objID, Img: img}
							log.Debugf("got an image from shop %v, mac %v, camera %v complete", shop, f.meta.Mac, f.meta.Camera)
						}
						log.Debugf("file-%d: complete file end call position", req.ID)
					}
					log.Debugf("file-%d: complete file end call position", req.ID)
				}
				mgr.remove(fid)
				mgr.RUnlock()
				log.Debugf("file-%d: complete file end", req.ID)
				return code
			}

			if times > 0 {
				duration = time.Duration(mgr.cfg.RetryFactor) * duration
			}

			times++
			if times >= mgr.cfg.MaxTimes {
				log.Warnf("file-%d: retry failed in %d times",
					fid,
					times)
				mgr.remove(fid)
				mgr.RUnlock()
				log.Debugf("file-%d: complete file end with over max times", req.ID)
				return pb.CodeMaxRetries
			}

			time.Sleep(duration)
		}
	}

	mgr.RUnlock()
	log.Debugf("file-%d: complete file with missing", req.ID)
	return pb.CodeMissing
}

func (mgr *fileManager) remove(id uint64) {
	delete(mgr.files, id)
	log.Infof("file-%d: removed", id)
}

type file struct {
	id     uint64
	meta   *pb.InitUploadReq
	chunks [][]byte
	readed int
	last   int32
}

func newFile(id uint64, meta *pb.InitUploadReq) *file {
	return &file{
		id:     id,
		meta:   meta,
		chunks: make([][]byte, meta.ChunkCount, meta.ChunkCount),
		last:   -1,
	}
}

func (f *file) append(req *pb.UploadReq) pb.Code {
	if req.Index >= f.meta.ChunkCount {
		log.Errorf("file-%d: append with invalid chunk idx %d",
			req.ID,
			req.Index)
		return pb.CodeInvalidChunk
	}

	if data := f.chunks[req.Index]; len(data) > 0 {
		log.Errorf("file-%d: already append with chunk idx %d",
			req.ID,
			req.Index)
		return pb.CodeSucc
	}

	f.chunks[req.Index] = req.Data
	log.Debugf("file-%d: append %d bytes with chunk idx %d",
		req.ID,
		len(req.Data),
		req.Index)
	return pb.CodeSucc
}

func (f *file) complete(req *pb.UploadCompleteReq) (objID string, code pb.Code) {
	objID = uuid.NewID()
	f.readed = 0
	err := objectStore.PutObject(bucketName, objID, f, f.meta.ContentLength)
	if err != nil {
		log.Errorf("file-%d: complete with oss errors: %+v",
			req.ID,
			err)
		code = pb.CodeOSSError
		return
	}

	log.Infof("file-%d: complete succ with object %s, size %d",
		req.ID,
		objID,
		f.meta.ContentLength)

	if f.meta.ContentLength > 50000 {
		// A 112*112*3 raw image is 38KB. The size will be reduced to about 1/10 with JPEG compression.
		log.Warnf("file-%d: file size %d is much bigger than expected", req.ID, f.meta.ContentLength)
	}
	code = pb.CodeSucc
	return
}

func (f *file) Read(p []byte) (int, error) {
	size := len(p)
	pos := 0
	read := 0
	for _, data := range f.chunks {
		cs := len(data)
		pos += cs
		if f.readed < pos {
			unreadIdx := cs - (pos - f.readed)
			n := copy(p[read:], data[unreadIdx:])
			read += n
			f.readed += n
			if read == size {
				return read, nil
			}
		}
	}

	return read, io.EOF
}
