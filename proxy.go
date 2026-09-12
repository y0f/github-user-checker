package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Pool owns every proxy. Each proxy runs its own workers, so a fast proxy
// answers many names while a dead one only wastes its own time. A proxy that
// fails is paused with exponential backoff and probed again later; nothing is
// ever retired for good.
type Pool struct {
	mu   sync.Mutex
	all  []*proxy
	seen map[string]bool

	baseBackoff time.Duration
	maxBackoff  time.Duration
	limitPause  time.Duration
}

type proxy struct {
	scheme string // http, https, socks4, socks5, direct
	host   string // host:port
	user   *url.Userinfo

	cli *http.Client

	mu    sync.Mutex
	fails int       // consecutive failures
	ok    int       // lifetime answers
	until time.Time // paused until this instant
}

func (p *proxy) key() string { return p.scheme + "://" + p.host }

func (p *proxy) String() string {
	if p.scheme == "direct" {
		return "direct"
	}
	return p.key()
}

// pausedFor reports how long the proxy must still wait before its next request.
func (p *proxy) pausedFor() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Until(p.until)
}

// wait sleeps out the proxy's pause, or returns early when ctx ends.
func (p *proxy) wait(ctx context.Context) error {
	for {
		d := p.pausedFor()
		if d <= 0 {
			return ctx.Err()
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// LoadPool reads scheme://[user:pass@]host:port lines. Bare host:port is http.
func LoadPool(path string, timeout, baseBackoff, maxBackoff, limitPause time.Duration) (*Pool, error) {
	pool := &Pool{
		seen:        map[string]bool{},
		baseBackoff: baseBackoff,
		maxBackoff:  maxBackoff,
		limitPause:  limitPause,
	}
	added, err := pool.Reload(path, timeout)
	if err != nil {
		return nil, err
	}
	if len(added) == 0 {
		return nil, fmt.Errorf("no usable proxies in %s", path)
	}
	return pool, nil
}

// Reload reads the list again and returns the proxies not seen before. Known
// entries are untouched: backoff decides when they get another chance.
func (p *Pool) Reload(path string, timeout time.Duration) ([]*proxy, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var added []*proxy
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "://") {
			line = "http://" + line
		}
		u, err := url.Parse(line)
		if err != nil {
			continue
		}
		scheme := strings.ToLower(u.Scheme)
		switch scheme {
		case "http", "https", "socks4", "socks5":
		default:
			continue
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			continue
		}
		px := &proxy{scheme: scheme, host: u.Host, user: u.User}
		p.mu.Lock()
		dup := p.seen[px.key()]
		if !dup {
			p.seen[px.key()] = true
			px.cli = px.client(timeout)
			p.all = append(p.all, px)
		}
		p.mu.Unlock()
		if !dup {
			added = append(added, px)
		}
	}
	return added, sc.Err()
}

// Succeed clears the failure streak.
func (p *Pool) Succeed(px *proxy) {
	px.mu.Lock()
	px.fails, px.ok = 0, px.ok+1
	px.mu.Unlock()
}

// Fail pauses the proxy for base * 2^(fails-1), capped at max. It reports
// whether the proxy had ever answered, so the caller knows the failure says
// something about the request and not only about the proxy.
func (p *Pool) Fail(px *proxy) (proven bool) {
	px.mu.Lock()
	proven = px.ok > 0
	px.fails++
	d := p.baseBackoff << uint(px.fails-1)
	if d > p.maxBackoff || d <= 0 {
		d = p.maxBackoff
	}
	px.until = time.Now().Add(d)
	px.mu.Unlock()
	return proven
}

// Limited pauses a proxy that GitHub rate limited. The proxy works, it just
// needs a breather; the streak is not touched.
func (p *Pool) Limited(px *proxy) {
	px.mu.Lock()
	if t := time.Now().Add(p.limitPause); t.After(px.until) {
		px.until = t
	}
	px.mu.Unlock()
}

// Stats counts proxies ready to send against proxies sitting out a pause.
func (p *Pool) Stats() (live, paused int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for _, px := range p.all {
		px.mu.Lock()
		if px.until.After(now) {
			paused++
		} else {
			live++
		}
		px.mu.Unlock()
	}
	return
}

// Direct is the local connection.
var Direct = &proxy{scheme: "direct", host: "this machine"}

// client builds a one-proxy http.Client. Connections are reused, so the TLS
// handshake through a proxy is paid once, not per name.
func (px *proxy) client(timeout time.Duration) *http.Client {
	tr := &http.Transport{
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
	}
	switch px.scheme {
	case "direct":
	case "http", "https":
		tr.Proxy = http.ProxyURL(&url.URL{Scheme: px.scheme, Host: px.host, User: px.user})
	default:
		tr.DialContext = px.socksDialer(timeout)
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}

// socksDialer speaks just enough SOCKS4/5 to open a TCP tunnel.
func (px *proxy) socksDialer(timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, _, addr string) (net.Conn, error) {
		d := net.Dialer{Timeout: timeout}
		conn, err := d.DialContext(ctx, "tcp", px.host)
		if err != nil {
			return nil, err
		}
		if dl, ok := ctx.Deadline(); ok {
			_ = conn.SetDeadline(dl)
		} else {
			_ = conn.SetDeadline(time.Now().Add(timeout))
		}
		if px.scheme == "socks5" {
			err = socks5Connect(conn, addr, px.user)
		} else {
			err = socks4Connect(conn, addr)
		}
		if err != nil {
			conn.Close()
			return nil, err
		}
		_ = conn.SetDeadline(time.Time{})
		return conn, nil
	}
}

func splitAddr(addr string) (string, uint16, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("bad port %q", portStr)
	}
	return host, uint16(port), nil
}

func socks5Connect(conn net.Conn, addr string, user *url.Userinfo) error {
	host, port, err := splitAddr(addr)
	if err != nil {
		return err
	}
	if len(host) > 255 {
		return fmt.Errorf("host too long")
	}
	greeting := []byte{5, 1, 0} // version 5, methods: no auth
	if user != nil {
		greeting = []byte{5, 2, 0, 2} // no auth, username/password
	}
	if _, err := conn.Write(greeting); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 5 {
		return fmt.Errorf("not a socks5 server, version %d", resp[0])
	}
	switch resp[1] {
	case 0:
	case 2:
		if user == nil {
			return fmt.Errorf("socks5 wants a password, none given")
		}
		if err := socks5Auth(conn, user); err != nil {
			return err
		}
	default:
		return fmt.Errorf("socks5 refused auth method %d", resp[1])
	}

	req := []byte{5, 1, 0, 3, byte(len(host))}
	req = append(req, host...)
	req = binary.BigEndian.AppendUint16(req, port)
	if _, err := conn.Write(req); err != nil {
		return err
	}

	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	if head[1] != 0 {
		return fmt.Errorf("socks5 connect failed, code %d", head[1])
	}
	// drain the bound address so the stream starts clean
	var skip int
	switch head[3] {
	case 1:
		skip = 4
	case 4:
		skip = 16
	case 3:
		l := make([]byte, 1)
		if _, err := io.ReadFull(conn, l); err != nil {
			return err
		}
		skip = int(l[0])
	default:
		return fmt.Errorf("socks5 bad address type %d", head[3])
	}
	_, err = io.ReadFull(conn, make([]byte, skip+2))
	return err
}

// socks5Auth does RFC 1929 username/password authentication.
func socks5Auth(conn net.Conn, user *url.Userinfo) error {
	name := user.Username()
	pass, _ := user.Password()
	if len(name) > 255 || len(pass) > 255 {
		return fmt.Errorf("socks5 credentials too long")
	}
	req := []byte{1, byte(len(name))}
	req = append(req, name...)
	req = append(req, byte(len(pass)))
	req = append(req, pass...)
	if _, err := conn.Write(req); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[1] != 0 {
		return fmt.Errorf("socks5 auth rejected")
	}
	return nil
}

func socks4Connect(conn net.Conn, addr string) error {
	host, port, err := splitAddr(addr)
	if err != nil {
		return err
	}
	req := []byte{4, 1}
	req = binary.BigEndian.AppendUint16(req, port)

	ip := net.ParseIP(host)
	if ip4 := ip.To4(); ip4 != nil {
		req = append(req, ip4...)
		req = append(req, 0) // empty user id
	} else {
		// socks4a: unresolvable address 0.0.0.x tells the proxy to resolve the
		// hostname that follows the user id.
		req = append(req, 0, 0, 0, 1, 0)
		req = append(req, host...)
		req = append(req, 0)
	}
	if _, err := conn.Write(req); err != nil {
		return err
	}
	resp := make([]byte, 8)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 0 {
		return fmt.Errorf("not a socks4 server, version %d", resp[0])
	}
	if resp[1] != 90 {
		return fmt.Errorf("socks4 connect failed, code %d", resp[1])
	}
	return nil
}
