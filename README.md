# Single-Flight with Akka Typed

A hands-on implementation of the **single-flight** pattern in Scala 3 + Akka Typed,
with a fake slow upstream and a Gatling load test to prove it works.

---

## 1. What is single-flight?

> When N callers ask for the same thing at the same time, do the work **once** and
> give everyone the same answer.

Imagine 2000 requests per second all asking for `user:1`, and fetching `user:1`
takes 2 seconds from an upstream service.

**Without single-flight** — every caller triggers its own call. The upstream gets
4000 concurrent calls and dies:

```
caller 1  ──────────► upstream call ──────────► answer
caller 2  ──────────► upstream call ──────────► answer
caller 3  ──────────► upstream call ──────────► answer
 ...                  (4000 calls)
```

**With single-flight** — the first caller triggers the call, everyone else just
waits in line for the same result:

```
caller 1  ──┐
caller 2  ──┤
caller 3  ──┼──► ONE upstream call ──► answer fanned out to all
 ...      ──┤
caller N  ──┘
```

Two important properties:

- **It is coalescing, not caching.** Once the call finishes, the result is *not*
  stored. The next request starts a fresh call. It only merges requests that
  overlap *in time*.
- **Average latency drops by half.** A caller arriving in the middle of an
  in-flight 2s fetch only waits for the *remainder*. Averaged over random arrival
  times, that is ~1s instead of 2s. The measurements below confirm this exactly.

---

## 2. Architecture

The design is **one actor per key**. A router actor owns a map of keys to child
actors; each child owns the single-flight state for its own key alone.

### The files

| File | Role |
|---|---|
| `KeyFlight.scala` | **The pattern.** One actor per key. Holds `fetching: Boolean` and `waiters: List[ActorRef]`. |
| `SingleFlight.scala` | Router. Spawns/looks up the `KeyFlight` child for a key and forwards. Holds no coalescing state. |
| `Backend.scala` | The "expensive operation" — a real async HTTP call to the third-party service. |
| `ThirdPartyService.scala` | Fake slow upstream on `:9090`. JDK `HttpServer` + one virtual thread per request. |
| `SingleFlightHttp.scala` | Akka HTTP server on `:8080`. `GET /value/<key>` and `GET /stats`. |
| `SingleFlightDemo.scala` | In-JVM demo: 8 caller actors, 2 keys, no HTTP. |
| `Metrics.scala` | Counts inbound vs outbound to compute the coalescing ratio. |
| `SingleFlightSimulation.scala` | Gatling load test (in `src/test`). |



---

## 3. Running it

Three terminals:

```bash
# 1. the slow upstream (start this FIRST)
sbt "runMain singleflight.runThirdParty"

# 2. the single-flight server
sbt "runMain singleflight.runSingleFlightHttp"

# 3. try it by hand — 20 concurrent calls, only ONE "[start]" in the logs
seq 20 | xargs -P20 -I{} curl -s localhost:8080/value/user:1
curl localhost:8080/stats
```

Load test:

```bash
sbt "Gatling/test"

# with knobs
sbt -Dsf.rate=6000 -Dsf.keys=20 "Gatling/testOnly singleflight.SingleFlightSimulation"
```

| Knob | Default | Meaning |
|---|---|---|
| `sf.rate` | 2000 | peak requests/sec |
| `sf.keys` | 20 | distinct keys (smaller = more coalescing) |
| `sf.ramp` / `sf.hold` | 4 / 6 | seconds ramping / holding |
| `tp.latency` | 2000 | upstream latency in ms |

---

## 4. Results
at 2000 req/s over 20 keys against a 2000ms upstream.

### Headline

```
16200 requests    16200 OK    0 KO    100% success    10s
```

Both assertions passed: p99 < 5000ms, success > 95%.

### The peak

| Metric | Peak |
|---|---|
| Arrival rate (users started/s) | **2032/s** — matches the 2000/s target exactly |
| Requests sent/s | 2140/s |
| Responses received/s | **3691/s** |
| Concurrent users in flight | 4030 |
| Mean throughput | 1472.73 req/s |

| Run | Requests | KO | % KO | p99 | mean |
|---|---|---|---|---|---|
| 07-05 13:00 | 6152 | 1787 | 29.0% | 19726 ms | 5553 ms |
| 07-06 19:21 | 3474 | 886 | 25.5% | 19391 ms | 5379 ms |
| 07-06 19:28 | 3896 | 1449 | 37.2% | 19394 ms | 6214 ms |
| 07-06 19:40 | 4010 | 0 | 0% | 4340 ms | 1867 ms |
| 07-06 19:54 | 4010 | 0 | 0% | 4248 ms | 1745 ms |
| 07-06 20:04 | 4010 | 0 | 0% | 288 ms | 62 ms |
| 07-08 18:45 | 4010 | 0 | 0% | 353 ms | 63 ms |
| 07-08 18:49 | 16020 | 0 | 0% | 537 ms | 90 ms |
| 07-08 18:51 | 16200 | 0 | 0% | 598 ms | 104 ms |
| 07-08 18:53 | 16200 | 0 | 0% | 2230 ms | 1241 ms |
| **07-08 18:59** | **16200** | **0** | **0%** | **2283 ms** | **1207 ms** |

### Why the early runs had KO

Every single failure in those three runs was the same error — the Gatling error
tables show one entry, at 100%:

```
j.n.ConnectException: Connection refused     1787 / 886 / 1449 errors
```

**Not one HTTP error status. Not one ask timeout.** The KOs never reached the
application at all — they failed at the TCP layer, before a request was even
sent. Every request that *did* get a connection came back 200.

The cause is the load generator, not the pattern. The simulation uses an **open
model** (`constantUsersPerSec`), which means "N users arrive per second no matter
how slow the server is". With a 2000ms upstream, in-flight requests pile up
faster than they drain, and without connection pooling each waiting virtual user
holds its own TCP socket. The server's accept backlog fills, and the OS starts
refusing new connections outright.

The report data shows this collapse clearly. Concurrent users climb and stay
pinned:

```
concurrent users:  512 → 1802 → 4740 → 5116 (peak) → drains slowly over 25s
```

And Gatling's own injection rate breaks down — it could not start the users it
promised to start:

```
users started/s:  532, 449, 1480, 2047, 1670, 29, 192, 316, 828, 725, 57, 0, 0, ...
                                    ↑ target rate      ↑ client falls over
```

The response-time histogram splits cleanly too: the OK responses sit in the fast
buckets, while the KO responses sit entirely in the slow tail — those are
connection attempts that queued for many seconds before being refused. That is
what pushed p99 to ~19 seconds, well past the 5s ask timeout the server never
actually hit.

**What fixed it**, visible in the current simulation:

| Change | Effect |
|---|---|
| `.shareConnections` | Virtual users share a connection pool instead of one socket each — the backlog stops overflowing |
| `.maxDuration(maxSecs)` | Hard stop, so a run can never spend 25s draining a backlog it will never clear |

The healthy run holds a comparable 4030 concurrent users with **0 KO**, because
those users are sharing pooled connections rather than each demanding a socket.

The takeaway: these KOs measured the limits of the test harness and the OS
socket layer, not the single-flight actor. The actor itself never returned an
error in any run.

---

## 5. Known limitations

Deliberate simplifications, worth knowing before copying this into production:
- **Unbounded waiter list.** A large burst holds one `ActorRef` per waiting
  caller. Callers that hit the 5s ask timeout still get replied to later, and
  those replies land in dead letters — harmless, but noisy.
