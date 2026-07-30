package singleflight

// ============================================================================
//  AN HTTP FRONT-END FOR THE SINGLE-FLIGHT ACTOR
// ============================================================================
//
//  This is the "scalable" version of the earlier demo. Before, every caller was
//  a full persistent Caller actor — millions of those would OOM. Here, callers
//  are HTTP clients. Each incoming request turns into ONE cheap, TEMPORARY
//  reply-address (created by the `ask` pattern) that vanishes the instant the
//  answer arrives. No persistent actor per caller.
//
//  The flow of a single request:
//    curl GET /value/user:1
//      -> route handler runs
//      -> sf.ask(replyTo => Get("user:1", replyTo))   // ask makes a temp replyTo
//      -> returns a Future[String]
//      -> `complete(future)` writes the string as the HTTP response body
//
//  Fire many concurrent curls for the same key and you'll STILL see only one
//  "[start]" in the logs — the actor coalesces them exactly as before.
//
//  Try it (after `sbt "runMain singleflight.runSingleFlightHttp"`):
//    curl localhost:8080/value/user:1      # first hit: ~800ms (does the work)
//    # fire 20 at once — only ONE compute runs, all 20 get the same answer:
//    seq 20 | xargs -P20 -I{} curl -s localhost:8080/value/user:1
// ============================================================================

import akka.actor.typed.ActorSystem
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern.* // gives ActorRef a `.ask` method
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.* // the route DSL: path, get, complete, ...
import akka.http.scaladsl.server.Route
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Success

@main def runSingleFlightHttp(): Unit =

  // ---- 1. Start the actor system, with SingleFlight as the ROOT actor ----
  // The expensive operation is the SHARED `Backend.compute` — the same one the
  // actor demo uses. `ActorSystem[T]` itself behaves like an ActorRef[T] to its
  // root actor, so here `system` IS the address of our SingleFlight actor.
  //
  // `Backend.compute` needs the ActorSystem (for its scheduler), but the system
  // is what we're creating here — so we build compute INSIDE a setup from the
  // actor's own `ctx.system`. The nested setup still makes SingleFlight the root,
  // so `system ! Get` / `system.ask(...)` work unchanged.
  given system: ActorSystem[SingleFlight.Command] =
    ActorSystem(
      Behaviors.setup[SingleFlight.Command](ctx => SingleFlight(Backend.compute(using ctx.system))),
      "single-flight-http"
    )

  // ---- 2. Implicits the `ask` pattern and HTTP need ----------------------
  // `ask` must know how long to wait for a reply, and which scheduler to use to
  // time out. `given` = Scala 3's way of providing an implicit value.
  given Timeout           = 5.seconds
  given Scheduler         = system.scheduler
  // Execution context for the Futures (mapping results, error handling).
  import system.executionContext

  // ---- 3. The routes: GET /value/<key>  and  GET /stats ------------------
  val route: Route =
    concat(
      // `path("value" / Segment)` matches /value/<something>; the <something>
      // is captured as `key`.
      path("value" / Segment) { key =>
        get { // only handle HTTP GET here
          Metrics.inbound.incrementAndGet() // count every inbound request
          // THE ASK. `system.ask(replyTo => Get(key, replyTo))`:
          //   - Akka creates a tiny, short-lived reply actor (the replyTo).
          //   - We send Get(key, thatReplyTo) to the SingleFlight actor.
          //   - When SingleFlight mails the answer to thatReplyTo, Akka completes
          //     the Future below and throws the temp reply actor away.
          val answer: Future[String] =
            system.ask(replyTo => SingleFlight.Get(key, replyTo))

          // `complete` turns the Future[String] into the HTTP response.
          complete(answer)
        }
      },
      // GET /stats — read the coalescing ratio + upstream latency live.
      path("stats") {
        get {
          complete(Metrics.snapshot)
        }
      }
    )

  // ---- 4. Bind the server to a port --------------------------------------
  val binding = Http().newServerAt("localhost", 8080).bind(route)

  binding.onComplete {
    case Success(b) =>
      val addr = b.localAddress
      println(s"Server online at http://${addr.getHostString}:${addr.getPort}/")
      println("Try:  curl localhost:8080/value/user:1")
    case Failure(ex) =>
      println(s"Failed to bind HTTP server: ${ex.getMessage}")
      system.terminate()
  }
