// Package redistest has test helpers for code that talks to Redis — owner: Dinesh.
//
// Start gives a test its own in-memory Redis (miniredis) and a client built
// with the same settings as the gateway's:
//
//	mr, c := redistest.Start(t)
//	cache := cache.NewRedis(c)
//	mr.FastForward(time.Minute) // expire TTLs without sleeping
//
// FaultProxy sits between a client and Redis so a test can make Redis slow or
// unreachable, then bring it back (Experiment 4, fail open vs closed):
//
//	mr, _ := redistest.Start(t)
//	p := redistest.NewFaultProxy(t, mr.Addr())
//	c := redistest.NewClient(t, p.Addr())
//	p.SetDelay(200 * time.Millisecond) // every reply is held for 200 ms
//	p.Down()                           // connections dropped, new ones refused
//	p.Up()                             // back to normal
package redistest

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/singhsaravjit/llm-gateway/internal/redisx"
)

// Start runs a fresh miniredis and returns it with a connected client.
// Both are closed when the test ends.
func Start(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	return mr, NewClient(t, mr.Addr())
}

// NewClient builds a client for addr with redisx.New (the gateway's settings)
// and closes it when the test ends.
func NewClient(t testing.TB, addr string) *redis.Client {
	t.Helper()
	c := redisx.New(redisx.Config{Addr: addr})
	t.Cleanup(func() { c.Close() })
	return c
}

// FaultProxy is a TCP proxy in front of Redis that can add delay or go down.
type FaultProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	delay time.Duration
	down  bool
	conns map[net.Conn]struct{}

	wg sync.WaitGroup
}

// NewFaultProxy starts a proxy forwarding to target (host:port). It is
// healthy until SetDelay or Down is called, and is closed when the test ends.
func NewFaultProxy(t testing.TB, target string) *FaultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("redistest: proxy listen: %v", err)
	}
	p := &FaultProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(p.close)
	return p
}

// Addr is the address clients should connect to instead of Redis.
func (p *FaultProxy) Addr() string { return p.ln.Addr().String() }

// SetDelay holds every reply from Redis for d before passing it on (0 = none).
func (p *FaultProxy) SetDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// Down drops every open connection and refuses new ones until Up is called.
func (p *FaultProxy) Down() {
	p.mu.Lock()
	p.down = true
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
}

// Up lets new connections through again.
func (p *FaultProxy) Up() {
	p.mu.Lock()
	p.down = false
	p.mu.Unlock()
}

func (p *FaultProxy) acceptLoop() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return // listener closed
		}
		p.mu.Lock()
		down := p.down
		p.mu.Unlock()
		if down {
			client.Close()
			continue
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			client.Close()
			continue
		}
		if !p.track(client, server) {
			continue
		}
		p.wg.Add(2)
		go p.pipe(server, client, false) // client -> Redis
		go p.pipe(client, server, true)  // Redis -> client, delayed
	}
}

// track registers both ends so Down and close can cut them. It returns false
// (and closes both) if the proxy went down in the meantime.
func (p *FaultProxy) track(a, b net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.down || p.conns == nil {
		a.Close()
		b.Close()
		return false
	}
	p.conns[a] = struct{}{}
	p.conns[b] = struct{}{}
	return true
}

func (p *FaultProxy) pipe(dst, src net.Conn, delayed bool) {
	defer p.wg.Done()
	defer func() {
		dst.Close()
		src.Close()
		p.mu.Lock()
		if p.conns != nil {
			delete(p.conns, dst)
			delete(p.conns, src)
		}
		p.mu.Unlock()
	}()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if delayed {
				p.mu.Lock()
				d := p.delay
				p.mu.Unlock()
				if d > 0 {
					time.Sleep(d)
				}
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil { // EOF, closed by Down/cleanup, or reset: stop piping
			return
		}
	}
}

func (p *FaultProxy) close() {
	p.ln.Close()
	p.mu.Lock()
	for c := range p.conns {
		c.Close()
	}
	p.conns = nil
	p.mu.Unlock()
	p.wg.Wait()
}
