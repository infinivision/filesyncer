package version

import (
	"fmt"
	"runtime"
)

// report version and Git SHA, inspired by github.com/coreos/etcd/version/version.go
var (
	Version = "1.0-SNAPSHOT"
	// GitSHA and BuildTime will be set during build
	GitSHA    = "Not recorded (use ./build.sh instead of go build)"
	BuildTime = "Not recorded (use ./build.sh instead of go build)"
)

func ShowVersion() {
	fmt.Printf("Version: %s\n", Version)
	fmt.Printf("Git SHA: %s\n", GitSHA)
	fmt.Printf("BuildTime: %s\n", BuildTime)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}
