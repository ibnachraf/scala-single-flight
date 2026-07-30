package singleflight

import io.gatling.core.Predef.*
import io.gatling.http.Predef.*
import scala.concurrent.duration.*

// ============================================================================
//  GATLING LOAD TEST for the single-flight HTTP endpoint
// ============================================================================
//
//  GOAL: throw thousands of requests per second at GET /value/<key>, spread over
//  a SMALL pool of keys, and watch what happens.
//
//  THE POINT: because the callers reuse a handful of keys, single-flight
//  coalesces them. The endpoint should sustain a very high REQUEST rate while
//  the backend `compute` runs only ~(number of distinct keys) times per ~800ms
//  window. Requests scale up; real work stays flat. That's the whole pitch.
//
//  HOW TO RUN (two terminals):
//    1) start the server:
//         sbt "runMain singleflight.runSingleFlightHttp"
//    2) run the load test:
//         sbt "Gatling/test"
//       or tune it on the fly with -D knobs (see below), e.g.:
//         sbt -Dsf.rate=6000 -Dsf.keys=20 "Gatling/testOnly singleflight.SingleFlightSimulation"
//
//  After it finishes, Gatling prints a summary and writes an HTML report
//  (path shown at the end) with throughput + latency percentile charts.
// ============================================================================
class SingleFlightSimulation extends Simulation:

  // ---- Knobs (override from the CLI with -Dsf.xxx=...) --------------------
  val numKeys  = sys.props.getOrElse("sf.keys", "20").toInt   // distinct keys in play
  val peakRate = sys.props.getOrElse("sf.rate", "2000").toInt // requests/sec at peak
  val rampSecs = sys.props.getOrElse("sf.ramp", "4").toInt   // seconds to ramp to peak
  val holdSecs = sys.props.getOrElse("sf.hold", "6").toInt   // seconds held at peak
  // Hard cap on total run time. Guarantees the sim ALWAYS stops, even if the
  // server can't drain the backlog the open-model injection created.
  val maxSecs  = sys.props.getOrElse("sf.max", (rampSecs + holdSecs + 20).toString).toInt

  // ---- Feeder: pick a key at random from the small pool ------------------
  // Small pool => lots of collisions => lots of coalescing. Grow sf.keys to see
  // the backend do proportionally more work.
  val keyFeeder =
    (1 to numKeys).map(i => Map("key" -> s"user:$i")).toArray.random

  // ---- What/where we hit -------------------------------------------------
  val httpProtocol =
    http
      .baseUrl("http://localhost:8080")
      .shareConnections     // pool connections instead of one per virtual user
    // fail a stuck request fast (server ask timeout is 5s),
                                 // so the backlog can't hang the run near the 60s default

  // ---- The virtual user's job: one GET, then leave -----------------------
  val scn = scenario("single-flight fetch")
    .feed(keyFeeder)
    .exec(
      http("GET /value/:key")
        .get("/value/#{key}")
        .check(status.is(200))
    )

  // ---- Injection profile: open model (arrival rate we CONTROL) -----------
  // Ramp the arrival rate from 10/s up to peak, then hold it to observe steady
  // state. Open model = "N requests arrive per second no matter how slow the
  // server is", which is what you want when probing capacity.
  setUp(
    scn.inject(
      rampUsersPerSec(100).to(peakRate).during(rampSecs.seconds),
      constantUsersPerSec(peakRate).during(holdSecs.seconds)
    )
  ).protocols(httpProtocol)
    .maxDuration(maxSecs.seconds) // <-- HARD STOP: the sim can never run longer than this
    .assertions(
      // The first caller per key waits ~800ms for the fetch; nothing should hit
      // the 5s ask timeout. Loosen/tighten these as you experiment.
      global.responseTime.percentile4.lt(5000), // p99 under the ask timeout
      global.successfulRequests.percent.gt(95.0)
    )
