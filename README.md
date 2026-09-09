# github-user-checker

Checks if GitHub username can really be registered. Go, zero deps. One proxy
per request, own connection as fallback. Free names go to text file.

## Why not 404

`GET github.com/<name>` → 404 lies:

| name | `github.com/<name>` | `api.github.com/users/<name>` | truth |
|---|---|---|---|
| `admin` | 404 | 404 | **reserved, never registrable** |
| `zzzqqxxwv` | 404 | 404 | free |

Same answer, different truth. This tool asks what signup form asks,
`github.com/signup_check/username?value=<name>`. Three answers:

- `200` + `<name> is available.` → free
- `422` + `Username <name> is not available.` → taken
- `422` + `Username '<name>' is unavailable.` → reserved

Verified live 2026-09-09.

## No false positives

1. Response must carry `X-GitHub-Request-Id`. Proxy error page cannot fake it.
   No header → answer thrown away, name retried elsewhere.
2. Every hit re-checked through second route before written. One yes not enough.
3. GitHub `429` is real answer, not proxy fault. Proxy stays, name moves on.
4. Names breaking GitHub rules (leading/trailing/double hyphen, >39 chars)
   dropped before request spent.

Tested: 13 known names over free proxies, 4 free / 4 taken / 4 reserved / 1
taken-with-hyphen. 13/13 correct. Random sample of 2032 earlier hits, 8/8 still
free.

## Run

```bash
go build -o ghcheck .
./ghcheck -in wordlists/englishlowercase.txt
```

Hits → `available.txt` as found. Every decided name → `checked.txt`. Ctrl+C,
run again, resumes.

## Flags

| flag | default | meaning |
|---|---|---|
| `-in` | `wordlists/dutchlowercase.txt` | wordlist, one name per line |
| `-out` | `available.txt` | free names |
| `-done` | `checked.txt` | resume log |
| `-proxies` | `proxies.txt` | proxy list |
| `-direct` | `true` | own connection when rotation cannot answer |
| `-direct-rate` | `30` | direct requests per minute, `0` unlimited |
| `-workers` | `64` | concurrent checks |
| `-tries` | `8` | proxy attempts per name before fallback |
| `-max-fails` | `4` | consecutive fails before proxy retired |
| `-reload` | `2m` | re-read proxy list this often, `0` off |
| `-min` / `-max` | `2` / `8` | name length filter |
| `-letters` | `true` | keep only `a-z` |
| `-confirm` | `true` | re-check hits via second route |
| `-timeout` | `12s` | per request |
| `-debug` | `false` | print why each attempt rejected |

## Proxies

`proxies.txt`, one per line:

```
http://1.2.3.4:8080
https://user:pass@1.2.3.4:8443
socks4://5.6.7.8:1080
socks5://user:pass@9.10.11.12:1080
```

Bare `host:port` = http. SOCKS handshakes implemented here, no deps. File
gitignored.

Two rules:

- **Proxy must CONNECT to https.** GitHub is TLS. Lists validated against
  plain-http target fail every request.
- **Free proxies die in hours.** Mostly `gaveup` → rebuild list. `-reload`
  picks up new file mid-run.

No list, or all dead → run continues direct at `-direct-rate`. Slow on purpose.
One address, nothing behind it.

## Notes

- Endpoint answers `406` to any `Accept` list. Send exactly one:
  `text/fragment+html`.
- Reserved counted separate from taken. Neither written to output.
