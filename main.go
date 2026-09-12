// github-user-checker asks GitHub's signup endpoint whether a username can be
// registered, one rotating proxy per request.
//
// A 404 on github.com/<name> is not proof of a free name: "admin" 404s on both
// the web page and the API, yet signup rejects it. Only /signup_check/username
// separates free, taken and reserved.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var debug bool

type verdict int

const (
	vRetry     verdict = iota // proxy junk, anything untrustworthy
	vLimited                  // GitHub 429: real answer, proxy is fine, try another
	vAvailable                // 200, "<name> is available."
	vTaken                    // 422, "Username <name> is not available."
	vReserved                 // 422, "Username 'x' is unavailable."
)

func main() {
	var (
		in         = flag.String("in", "wordlists/dutchlowercase.txt", "wordlist, one name per line")
		out        = flag.String("out", "available.txt", "file that collects available names")
		done       = flag.String("done", "checked.txt", "names already decided, used to resume")
		proxies    = flag.String("proxies", "proxies.txt", "proxy list, scheme://host:port per line")
		perProxy   = flag.Int("per-proxy", 4, "concurrent requests through each proxy")
		timeout    = flag.Duration("timeout", 8*time.Second, "per-request timeout")
		tries      = flag.Int("tries", 8, "attempts per name before giving up")
		backoff    = flag.Duration("backoff", 10*time.Second, "pause after a proxy's first failure, doubles each failure in a row")
		maxBackoff = flag.Duration("max-backoff", 10*time.Minute, "longest pause for a failing proxy")
		limitPause = flag.Duration("limit-pause", 20*time.Second, "pause for a proxy GitHub answered 429 to")
		minLen     = flag.Int("min", 2, "minimum name length")
		maxLen     = flag.Int("max", 8, "maximum name length")
		letters    = flag.Bool("letters", true, "keep only a-z names, no digits or hyphens")
		confirm    = flag.Bool("confirm", true, "re-check every hit through a different proxy")
		dbg        = flag.Bool("debug", false, "print why each proxy attempt was rejected")
		direct     = flag.Bool("direct", true, "also use this machine's own connection")
		directRate = flag.Int("direct-rate", 30, "direct requests per minute, 0 for no limit")
		reload     = flag.Duration("reload", 2*time.Minute, "re-read the proxy list this often, 0 to disable")
	)
	flag.Parse()
	debug = *dbg

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := LoadPool(*proxies, *timeout, *backoff, *maxBackoff, *limitPause)
	switch {
	case err == nil:
		live, _ := pool.Stats()
		fmt.Printf("%d proxies loaded from %s, %d workers each\n", live, *proxies, *perProxy)
	case *direct:
		fmt.Printf("no proxy list (%v), running direct from this machine\n", err)
	default:
		die(err)
	}
	if pool == nil {
		pool = &Pool{seen: map[string]bool{}, baseBackoff: *backoff, maxBackoff: *maxBackoff, limitPause: *limitPause}
	}

	var fallback *limiter
	if *direct {
		fallback = newLimiter(*directRate)
		Direct.cli = Direct.client(*timeout)
		fmt.Printf("direct connection on, limited to %d requests per minute\n", *directRate)
	}

	skip, err := loadSet(*done)
	if err != nil {
		die(err)
	}
	names, err := loadNames(*in, *minLen, *maxLen, *letters, skip)
	if err != nil {
		die(err)
	}
	if len(skip) > 0 {
		fmt.Printf("resuming, %d already checked\n", len(skip))
	}
	fmt.Printf("checking %d names\n", len(names))

	hits, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		die(err)
	}
	defer hits.Close()
	log, err := os.OpenFile(*done, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		die(err)
	}
	defer log.Close()

	r := &run{
		pool:    pool,
		timeout: *timeout,
		tries:   *tries,
		confirm: *confirm,
		queue:   make(chan *job, len(names)),
		start:   time.Now(),
		total:   len(names),
	}
	r.record = func(name string, v verdict) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if v == vAvailable {
			if _, err := fmt.Fprintln(hits, name); err != nil {
				die(fmt.Errorf("writing %s: %w", *out, err))
			}
			_ = hits.Sync()
			fmt.Printf("\r%-100s\r", "")
			fmt.Println("available:", name)
		}
		if _, err := fmt.Fprintln(log, name); err != nil {
			die(fmt.Errorf("writing %s: %w", *done, err))
		}
	}

	// Every name is a job. It sits in the queue or in one worker's hands until
	// decided, then the pending group shrinks by one.
	r.pending.Add(len(names))
	for _, n := range names {
		r.queue <- &job{name: n}
	}

	// Workers stop when every job is decided or the user interrupts.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	spawn := func(px *proxy, n int, lim *limiter) {
		for i := 0; i < n; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				r.worker(wctx, px, lim)
			}()
		}
	}
	pool.mu.Lock()
	all := append([]*proxy(nil), pool.all...)
	pool.mu.Unlock()
	for _, px := range all {
		spawn(px, *perProxy, nil)
	}
	if *direct {
		spawn(Direct, 1, fallback)
	}

	if *reload > 0 {
		go func() {
			t := time.NewTicker(*reload)
			defer t.Stop()
			for {
				select {
				case <-wctx.Done():
					return
				case <-t.C:
					added, err := pool.Reload(*proxies, *timeout)
					if err != nil || len(added) == 0 {
						continue
					}
					for _, px := range added {
						spawn(px, *perProxy, nil)
					}
					fmt.Printf("\r%-100s\rproxy list reloaded: %d new\n", "", len(added))
				}
			}
		}()
	}

	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-t.C:
				r.progress("")
			}
		}
	}()

	finished := make(chan struct{})
	go func() {
		r.pending.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-ctx.Done():
	}
	cancel()
	workers.Wait()

	r.progress("\n")
	fmt.Printf("done in %s, %d available written to %s\n",
		time.Since(r.start).Round(time.Second), r.available.Load(), *out)
}

// job is one name on its way to a verdict.
type job struct {
	name    string
	tries   int    // attempts that produced no trustworthy answer
	yesFrom *proxy // proxy that said available; a second proxy must agree
}

type run struct {
	pool    *Pool
	timeout time.Duration
	tries   int
	confirm bool
	queue   chan *job
	record  func(string, verdict)

	mu        sync.Mutex
	pending   sync.WaitGroup
	start     time.Time
	total     int
	checked   atomic.Int64
	available atomic.Int64
	taken     atomic.Int64
	reserved  atomic.Int64
	gaveUp    atomic.Int64
}

func (r *run) progress(end string) {
	l, p := r.pool.Stats()
	c := r.checked.Load()
	rate := float64(c) / time.Since(r.start).Seconds()
	fmt.Printf("\rchecked %d/%d  available %d  taken %d  reserved %d  gaveup %d  proxies %d live %d paused  %.1f/s   %s",
		c, r.total, r.available.Load(), r.taken.Load(),
		r.reserved.Load(), r.gaveUp.Load(), l, p, rate, end)
}

func (r *run) decide(j *job, v verdict) {
	switch v {
	case vAvailable:
		r.available.Add(1)
	case vTaken:
		r.taken.Add(1)
	case vReserved:
		r.reserved.Add(1)
	default:
		r.gaveUp.Add(1)
		r.checked.Add(1)
		r.pending.Done()
		return
	}
	r.record(j.name, v)
	r.checked.Add(1)
	r.pending.Done()
}

// worker pulls jobs through one proxy until the context ends. It sleeps
// through the proxy's pauses, so a dead proxy costs nothing but its own time.
func (r *run) worker(ctx context.Context, px *proxy, lim *limiter) {
	for {
		if px.wait(ctx) != nil {
			return
		}
		if lim != nil && lim.wait(ctx) != nil {
			return
		}
		var j *job
		select {
		case <-ctx.Done():
			return
		case j = <-r.queue:
		}
		if j.yesFrom == px {
			// This proxy already said yes; hand the job to another one.
			r.queue <- j
			time.Sleep(50 * time.Millisecond)
			continue
		}

		v, err := ask(ctx, px, j.name, r.timeout)
		if debug && (err != nil || v == vRetry || v == vLimited) {
			fmt.Printf("\r%-100s\rdebug %s via %s: verdict=%d err=%v\n", "", j.name, px, v, err)
		}
		if ctx.Err() != nil {
			r.queue <- j
			return
		}

		switch {
		case v == vLimited:
			r.pool.Limited(px)
			j.tries++
		case err != nil || v == vRetry:
			// A proxy that never answered anything is junk, not evidence
			// about this name. Only a proven proxy's failure counts.
			if r.pool.Fail(px) {
				j.tries++
			}
		case v == vAvailable && r.confirm && j.yesFrom == nil:
			// One yes can still be one lying proxy: a free name must answer twice.
			r.pool.Succeed(px)
			j.yesFrom = px
		default:
			r.pool.Succeed(px)
			r.decide(j, v)
			continue
		}
		if j.tries >= r.tries {
			r.decide(j, vRetry)
			continue
		}
		r.queue <- j
	}
}

type limiter struct{ tick <-chan time.Time }

func newLimiter(perMinute int) *limiter {
	if perMinute <= 0 {
		return &limiter{}
	}
	return &limiter{tick: time.NewTicker(time.Minute / time.Duration(perMinute)).C}
}

func (l *limiter) wait(ctx context.Context) error {
	if l.tick == nil {
		return nil
	}
	select {
	case <-l.tick:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func ask(ctx context.Context, px *proxy, name string, timeout time.Duration) (verdict, error) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodGet,
		"https://github.com/signup_check/username?value="+url.QueryEscape(name), nil)
	if err != nil {
		return vRetry, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	// Exactly one media type: the endpoint answers 406 to any Accept list.
	req.Header.Set("Accept", "text/fragment+html")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://github.com")
	req.Header.Set("Referer", "https://github.com/signup")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Dest", "empty")

	resp, err := px.cli.Do(req)
	if err != nil {
		return vRetry, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return vRetry, err
	}

	if debug {
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 120 {
			snippet = snippet[:120]
		}
		fmt.Printf("\r%-90s\rdebug   status=%d ghid=%q ctype=%q body=%q\n", "",
			resp.StatusCode, resp.Header.Get("X-GitHub-Request-Id"),
			resp.Header.Get("Content-Type"), snippet)
	}

	// Only GitHub sets this header. Without it the body is a proxy's own page.
	if resp.Header.Get("X-GitHub-Request-Id") == "" {
		return vRetry, nil
	}

	text := string(body)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return vLimited, nil
	case resp.StatusCode == http.StatusOK && strings.Contains(text, "is available."):
		return vAvailable, nil
	case resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(text, "is unavailable."):
		return vReserved, nil
	case resp.StatusCode == http.StatusUnprocessableEntity &&
		(strings.Contains(text, "is not available.") || strings.Contains(text, "suggested_usernames")):
		return vTaken, nil
	}
	return vRetry, nil
}

// valid mirrors GitHub's rule: alphanumerics and single inner hyphens, up to
// 39 characters.
func valid(s string) bool {
	if len(s) == 0 || len(s) > 39 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-':
			if s[i-1] == '-' {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func onlyLetters(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 'a' || c > 'z' {
			return false
		}
	}
	return true
}

func loadNames(path string, min, max int, letters bool, skip map[string]bool) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var names []string
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		n := strings.ToLower(strings.TrimSpace(sc.Text()))
		if n == "" || len(n) < min || len(n) > max || seen[n] || skip[n] || !valid(n) {
			continue
		}
		if letters && !onlyLetters(n) {
			continue
		}
		seen[n] = true
		names = append(names, n)
	}
	return names, sc.Err()
}

func loadSet(path string) (map[string]bool, error) {
	set := map[string]bool{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return set, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if n := strings.TrimSpace(sc.Text()); n != "" {
			set[n] = true
		}
	}
	return set, sc.Err()
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
