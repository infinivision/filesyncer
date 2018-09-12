package monitor

import (
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"

	"github.com/infinivision/filesyncer/pkg/pb"
)

var (
	CpuTotal uint64 = 0
)

func (m *Monitor) getSysUsage() (usage *pb.SysUsage, err2 error) {
	usage = &pb.SysUsage{
		Mac: m.cfg.ID,
	}
	if CpuTotal == 0 {
		if cpuinfo, err := cpu.Info(); err != nil {
			err2 = errors.Wrapf(err, "")
			return
		} else {
			CpuTotal = uint64(len(cpuinfo))
			usage.CpuTotal = CpuTotal //number of vCPU
		}
	}
	if cpuPer, err := cpu.Percent(300*time.Millisecond, false); err != nil {
		err2 = errors.Wrapf(err, "")
		return
	} else {
		usage.CpuUsedPercent = uint32(cpuPer[0]) //average of all virtual cpus
	}

	if memInfo, err := mem.VirtualMemory(); err != nil {
		err2 = errors.Wrapf(err, "")
		return
	} else {
		usage.MemTotal = memInfo.Total                     //RAM size in bytes
		usage.MemUsedPercent = uint32(memInfo.UsedPercent) //exclued buffer/cache
	}

	if du, err := disk.Usage("/"); err != nil {
		err2 = errors.Wrapf(err, "")
		return
	} else {
		usage.DiskTotal = du.Total                     //root partition size in bytes
		usage.DiskUsedPercent = uint32(du.UsedPercent) //only root partition
	}
	return
}

func (m *Monitor) reportSysUsage() {
	var err error
	var usage *pb.SysUsage
	if usage, err = m.getSysUsage(); err != nil {
		log.Errorf("GetSysUsage failed with error: %+v", err)
		return
	}
	m.pool.ForEach(func(addr string, conn goetty.IOSession) {
		if !conn.IsConnected() {
			log.Warnf("skipped reporting usage to %v due to broken connection", addr)
		} else {
			if err := conn.WriteAndFlush(usage); err != nil {
				log.Errorf("WriteAndFlush failed with error: %+v", err)
			}
		}
	})
}
