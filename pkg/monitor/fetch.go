package monitor

import (
	"context"
	"sync"

	"github.com/fagongzi/log"
)

func (m *Monitor) startWaittingCompleteTask() {
	m.runner.RunCancelableTask(func(ctx context.Context) {
		log.Infof("task-waitting: started")

		for {
			select {
			case <-ctx.Done():
				log.Infof("task-waitting: stopped")
				return
			case wg := <-m.completeC:
				wg.Wait()
				log.Debugf("task-waitting: last files upload complete")
				m.triggerFetch()
			}
		}
	})
}

func (m *Monitor) triggerFetch() {
	m.tw.Schedule(m.cfg.MonitorInterval, m.doFetchFiles, nil)
}

func (m *Monitor) doFetchFiles(arg interface{}) {
	log.Debugf("fetch: do")
	files, err := m.cfg.getFiles()
	if err != nil {
		log.Errorf("fetch: fetch files failed, errors:%+v", err)
		return
	}

	log.Debugf("fetch: get files: %+v", files)

	// If empty, later retry. Otherwise, wait complete notify.
	// If we always trigger monitor, maybe duplicate upload.
	if len(files) == 0 {
		m.triggerFetch()
		return
	}

	m.resetCompleteWG(len(files))
	for _, file := range files {
		m.addFile(file)
	}
}

func (m *Monitor) resetCompleteWG(count int) {
	m.completeWG = &sync.WaitGroup{}
	m.completeWG.Add(count)
	m.completeC <- m.completeWG
}

func (m *Monitor) completeNotify() {
	if m.completeWG != nil {
		m.completeWG.Done()
	}
}
