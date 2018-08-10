package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	EurekaAddr = "http://192.168.150.138:8761/eureka"
	EurekaApp  = "iot-backend"
	MyMac      = "309c233431b2"
	MyCamera   = "192.168.150.243"
)

/*
curl -X GET 'http://192.168.150.138:8000/terminals?deviceId=309c233431b2' -H '__no_auth__: foo'
*/
func TestGetShop(t *testing.T) {
	var err error
	var cmdb *CmdbApi
	var shop uint64
	var found bool
	cmdb, err = NewCmdbApi(EurekaAddr, EurekaApp)
	require.NoError(t, err)
	shop, found, err = cmdb.GetShop(MyMac)
	require.NoError(t, err)
	require.Equal(t, true, found)
	require.Equal(t, uint64(8), shop)
	fmt.Printf("shop %v, found %v\n", shop, found)
}

func TestGetPosition(t *testing.T) {
	var err error
	var cmdb *CmdbApi
	var shop uint64
	var pos uint32
	var found bool
	cmdb, err = NewCmdbApi(EurekaAddr, EurekaApp)
	require.NoError(t, err)
	shop, pos, found, err = cmdb.GetPosition(MyMac, MyCamera)
	require.NoError(t, err)
	require.Equal(t, true, found)
	require.Equal(t, uint64(8), shop)
	require.Equal(t, uint32(1), pos)
}
