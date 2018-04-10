package monitor

import (
	"context"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/atomic"
	"github.com/fagongzi/util/task"
)

const (
	bufC = 128
)

// Monitor monitor the upload files in the spec folder
type Monitor struct {
	sync.RWMutex

	cfg                  *Cfg
	runner               *task.Runner
	tw                   *goetty.TimeoutWheel
	pool                 *goetty.AddressBasedPool
	idx, fileSeq         *atomic.Uint64
	fileServers          []string
	prepares, uploadings *sync.Map
	readyC               chan string
	completeC            chan *sync.WaitGroup
	completeWG           *sync.WaitGroup
}

// NewMonitor create a Monitor
func NewMonitor(cfg *Cfg) *Monitor {
	m := &Monitor{
		cfg: cfg,
	}
	m.init()

	return m
}

// Start start monitor the target dir
func (m *Monitor) Start() {
	m.startRefreshTask()
	m.startPrepareTask()
	m.startWaittingCompleteTask()
	m.triggerFetch()
}

// Stop stop monitor the target dir
func (m *Monitor) Stop() {
	m.runner.Stop()
}

func (m *Monitor) init() {
	m.runner = task.NewRunner()
	m.tw = goetty.NewTimeoutWheel(goetty.WithTickInterval(time.Millisecond * 500))
	m.pool = goetty.NewAddressBasedPool(m.connFactory, m)
	m.idx = &atomic.Uint64{}
	m.fileSeq = &atomic.Uint64{}
	m.prepares = &sync.Map{}
	m.uploadings = &sync.Map{}
	m.readyC = make(chan string, bufC)
	m.completeC = make(chan *sync.WaitGroup)
}

func (m *Monitor) startRefreshTask() {
	m.doRefresh()

	m.runner.RunCancelableTask(func(ctx context.Context) {
		log.Infof("task-refresh: started")
		ticker := time.NewTicker(m.cfg.RefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("task-refresh: stopped")
				return
			case <-ticker.C:
				m.doRefresh()
			}
		}
	})
}

func (m *Monitor) startPrepareTask() {
	m.runner.RunCancelableTask(func(ctx context.Context) {
		log.Infof("task-prepare: started")

		for {
			select {
			case <-ctx.Done():
				log.Infof("task-prepare: stopped")
				return
			case file := <-m.readyC:
				log.Debugf("task-prepare: do %s", file)
				m.handlePrepare(file)
			}
		}
	})
}

func (m *Monitor) getPrepareStat(id uint64) *status {
	value, ok := m.prepares.Load(id)
	if !ok {
		log.Fatalf("bug: missing prepare info")
	}

	return value.(*status)
}

func (m *Monitor) getUploadingStat(id uint64) *status {
	value, ok := m.uploadings.Load(id)
	if !ok {
		log.Fatalf("bug: missing uploadings info")
	}

	return value.(*status)
}
