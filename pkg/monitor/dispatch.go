package monitor

import (
	"github.com/fagongzi/log"
)

func (m *Monitor) doRefresh() {
	m.Lock()
	defer m.Unlock()

	log.Debugf("task-refresh: do")

	if m.cfg.Discovery == "" {
		log.Debugf("task-refresh: discovery is not set, use backup servers")
		m.fileServers = m.cfg.Backups
	}

	log.Debugf("task-refresh: done")
}

func (m *Monitor) nextAvailable() string {
	m.RLock()
	defer m.RUnlock()

	if len(m.fileServers) == 0 {
		return ""
	}

	return m.fileServers[int(m.idx.Incr()%uint64(len(m.fileServers)))]
}

func (m *Monitor) available() int {
	m.RLock()
	defer m.RUnlock()

	return len(m.fileServers)
}
