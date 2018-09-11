package goetty

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrClosed is the error resulting if the pool is closed via pool.Close().
	ErrClosed = errors.New("pool is closed")
)

// ConnStatusHandler handler for conn status
type ConnStatusHandler interface {
	ConnectFailed(addr string, err error)
	Connected(addr string, conn IOSession)
}

// AddressBasedPool is a address based conn pool.
// Only one conn per address in the pool.
type AddressBasedPool struct {
	sync.RWMutex

	handler ConnStatusHandler
	factory func(string) IOSession
	conns   map[string]IOSession
}

// NewAddressBasedPool returns a AddressBasedPool with a factory fun
func NewAddressBasedPool(factory func(string) IOSession, handler ConnStatusHandler) *AddressBasedPool {
	return &AddressBasedPool{
		handler: handler,
		factory: factory,
		conns:   make(map[string]IOSession),
	}
}

// GetConn returns a IOSession that connected to the address
// Every address has only one connection in the pool
func (pool *AddressBasedPool) GetConn(addr string) (IOSession, error) {
	conn := pool.getConnLocked(addr)
	if err := pool.checkConnect(addr, conn); err != nil {
		return nil, err
	}

	return conn, nil
}

// RemoveConn close the conn, and remove from the pool
func (pool *AddressBasedPool) RemoveConn(addr string) {
	pool.Lock()
	if conn, ok := pool.conns[addr]; ok {
		conn.Close()
		delete(pool.conns, addr)
	}
	pool.Unlock()
}

// RemoveConnIfMatches close the conn, and remove from the pool if the conn in the pool is match the given
func (pool *AddressBasedPool) RemoveConnIfMatches(addr string, target IOSession) bool {
	removed := false

	pool.Lock()
	if conn, ok := pool.conns[addr]; ok && conn == target {
		conn.Close()
		delete(pool.conns, addr)
		removed = true
	}
	pool.Unlock()

	return removed
}

func (pool *AddressBasedPool) getConnLocked(addr string) IOSession {
	pool.RLock()
	conn := pool.conns[addr]
	pool.RUnlock()

	if conn != nil {
		return conn
	}

	return pool.createConn(addr)
}

func (pool *AddressBasedPool) checkConnect(addr string, conn IOSession) error {
	if nil == conn {
		return fmt.Errorf("nil connection")
	}

	pool.Lock()
	if conn.IsConnected() {
		pool.Unlock()
		return nil
	}

	_, err := conn.Connect()
	if err != nil {
		if pool.handler != nil {
			pool.handler.ConnectFailed(addr, err)
		}
		pool.Unlock()
		return err
	}

	if pool.handler != nil {
		pool.handler.Connected(addr, conn)
	}
	pool.Unlock()
	return nil
}

func (pool *AddressBasedPool) createConn(addr string) IOSession {
	pool.Lock()

	// double check
	if conn, ok := pool.conns[addr]; ok {
		pool.Unlock()
		return conn
	}

	conn := pool.factory(addr)
	pool.conns[addr] = conn
	pool.Unlock()
	return conn
}
