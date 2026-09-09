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
		workers    = flag.Int("workers", 64, "concurrent checks, one proxy per request")
		timeout    = flag.Duration("timeout", 12*time.Second, "per-request timeout")
		tries      = flag.Int("tries", 8, "proxy attempts per name before giving up")
		maxFails   = flag.Int("max-fails", 4, "consecutive failures before a proxy is retired")
		minLen     = flag.Int("min", 2, "minimum name length")
		maxLen     = flag.Int("max", 8, "maximum name length")
		letters    = flag.Bool("letters", true, "keep only a-z names, no digits or hyphens")
		confirm    = flag.Bool("confirm", true, "re-check every hit through a different proxy")
		dbg        = flag.Bool("debug", false, "print why each proxy attempt was rejected")
		direct     = flag.Bool("direct", true, "fall back to this machine's own connection when the rotation cannot answer")
		directRate = flag.Int("direct-rate", 30, "direct requests per minute, 0 for no limit")
		reload     = flag.Duration("reload", 2*time.Minute, "re-read the proxy list this often, 0 to disable")
	)
	flag.Parse()
	debug = *dbg

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := LoadPool(*proxies, *maxFails)
	switch {
	case err == nil:
		live, _ := pool.Stats()
		fmt.Printf("%d proxies loaded from %s\n", live, *proxies)
	case *direct:
		fmt.Printf("no proxy list (%v), running direct from this machine\n", err)
	default:
		die(err)
	}

	var fallback *limiter
	if *direct {
		fallback = newLimiter(*directRate)
		fmt.Printf("direct fallback on, limited to %d requests per minute\n", *directRate)
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
	fmt.Printf("checking %d names with %d workers\n", len(names), *workers)

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

	var (
		mu        sync.Mutex
		checked   atomic.Int64
		available atomic.Int64
		taken     atomic.Int64
		reserved  atomic.Int64
		gaveUp    atomic.Int64
		start     = time.Now()
	)

	record := func(name string, v verdict) {
		mu.Lock()
		defer mu.Unlock()
		if v == vAvailable {
			if _, err := fmt.Fprintln(hits, name); err != nil {
				die(fmt.Errorf("writing %s: %w", *out, err))
			}
			_ = hits.Sync()
			fmt.Printf("\r%-90s\r", "")
			fmt.Println("available:", name)
		}
		if _, err := fmt.Fprintln(log, name); err != nil {
			die(fmt.Errorf("writing %s: %w", *done, err))
		}
	}

	work := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range work {
				v := check(ctx, pool, fallback, name, *timeout, *tries)
				// One yes can still be one lying proxy: a free name must answer twice.
				if v == vAvailable && *confirm {
					if again := check(ctx, pool, fallback, name, *timeout, *tries); again != vAvailable {
						v = again
					}
				}
				switch v {
				case vAvailable:
					available.Add(1)
				case vTaken:
					taken.Add(1)
				case vReserved:
					reserved.Add(1)
				default:
					v = vRetry
					gaveUp.Add(1)
				}
				if v != vRetry {
					record(name, v)
				}
				checked.Add(1)
			}
		}()
	}

	progress := func(end string) {
		var l, d int
		if pool != nil {
			l, d = pool.Stats()
		}
		fmt.Printf("\rchecked %d/%d  available %d  taken %d  reserved %d  gaveup %d  proxies %d live %d dead   %s",
			checked.Load(), len(names), available.Load(), taken.Load(),
			reserved.Load(), gaveUp.Load(), l, d, end)
	}
	if pool != nil && *reload > 0 {
		go func() {
			t := time.NewTicker(*reload)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					added, revived, err := pool.Reload(*proxies)
					if err == nil && added+revived > 0 {
						fmt.Printf("\r%-90s\rproxy list reloaded: %d new, %d revived\n", "", added, revived)
					}
				}
			}
		}()
	}

	finished := make(chan struct{})
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-finished:
				return
			case <-ticker.C:
			}
			progress("")
			if pool == nil || *direct {
				continue
			}
			if l, d := pool.Stats(); l == 0 {
				fmt.Printf("\rall %d proxies are dead, stopping%-40s\n", d, "")
				fmt.Println("refresh the list with proxies that can CONNECT to https targets")
				stop()
				return
			}
		}
	}()

feed:
	for _, n := range names {
		select {
		case work <- n:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()
	close(finished)

	progress("\n")
	fmt.Printf("done in %s, %d available written to %s\n",
		time.Since(start).Round(time.Second), available.Load(), *out)
}

// check walks the rotation until a proxy relays a real GitHub answer, then
// falls back to the local connection.
func check(ctx context.Context, pool *Pool, fallback *limiter, name string, timeout time.Duration, tries int) verdict {
	for i := 0; pool != nil && i < tries; i++ {
		if ctx.Err() != nil {
			return vRetry
		}
		px, ok := pool.Next()
		if !ok {
			break
		}
		v, err := ask(ctx, px, name, timeout)
		if debug && (err != nil || v == vRetry || v == vLimited) {
			fmt.Printf("\r%-90s\rdebug %s via %s: verdict=%d err=%v\n", "", name, px, v, err)
		}
		switch {
		case ctx.Err() != nil:
			// Cancelled mid-request: not the proxy's fault.
			return vRetry
		case v == vLimited:
			// GitHub answered, so the proxy works. Move on without punishing it.
			continue
		case err != nil || v == vRetry:
			pool.Fail(px)
			continue
		}
		pool.Succeed(px)
		return v
	}

	if fallback == nil || ctx.Err() != nil {
		return vRetry
	}
	if err := fallback.wait(ctx); err != nil {
		return vRetry
	}
	v, err := ask(ctx, Direct, name, timeout)
	if err != nil && debug {
		fmt.Printf("\r%-90s\rdebug %s direct: %v\n", "", name, err)
	}
	return v
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

	resp, err := px.client(timeout).Do(req)
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
