package main

import (
	"encoding/hex"
	"net"
	"path/filepath"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Get MAC of physical NICs on which an non-loopback IPv4 address is alive.
// Returns the smallest one if there are more than one.
func GetNicMAC() (mac string) {
	var macs []string
	// get all the system's or local machine's network interfaces
	interfaces, _ := net.Interfaces()
	var err error
	for _, interf := range interfaces {
		// Skip virtual interfaces.
		symInterf := filepath.Join("/sys/class/net", interf.Name)
		var realInterf string
		if realInterf, err = filepath.EvalSymlinks(symInterf); err != nil {
			log.Warnf("%v is broken, skip", symInterf)
			continue
		}
		if strings.Index(realInterf, "devices/virtual") >= 0 {
			log.Debugf("%v is virtual, skip", symInterf)
			continue
		}

		if addrs, err := interf.Addrs(); err == nil {
			if len(interf.HardwareAddr) == 0 {
				// There's no MAC for tun interfaces.
				log.Debugf("interface %v doesn't have hardware address, skip", interf.Name)
				continue
			}

			// Nil slice: its len is zero, and it's fine to append.
			var currentIP []string
			for _, addr := range addrs {
				// check the address type and if it is not a loopback the display it
				// = GET LOCAL IP ADDRESS
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						currentIP = append(currentIP, ipnet.IP.String())
					}
				}
			}
			if currentIP == nil {
				log.Debugf("interface %v doesn't have non-loopback IPv4 address, skip", interf.Name)
				continue
			}
			log.Debugf("interface %v, MAC %v, IP %v", interf.Name, hex.EncodeToString(interf.HardwareAddr), currentIP)
			macs = append(macs, hex.EncodeToString(interf.HardwareAddr))
		}
	}
	if len(macs) != 0 {
		sort.Strings(macs)
		mac = macs[0]
	}
	return
}
