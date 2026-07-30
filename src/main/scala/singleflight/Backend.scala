package singleflight

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.Unmarshal

import scala.concurrent.Future

/** The shared "expensive operation" that both the actor demo and the HTTP server
  * put behind single-flight. It now makes a REAL network call to the third-party
  * Loom service (see ThirdPartyService.scala) using the Akka HTTP CLIENT.
  *
  * Why this is fast: `Http().singleRequest(...)` is fully async — the upstream
  * latency holds NO thread here; the connection pool and Akka Streams drive it
  * via non-blocking I/O. And single-flight means we make at most ~one
  * outbound call per key per window, so thousands of inbound requests collapse
  * into a trickle of upstream calls (protecting both us and the third party).
  */
object Backend:

  /** How many upstream calls `compute` has actually fired (via Metrics). */
  def calls: Int = Metrics.outboundCalls.toInt

  /** Build the compute function. Needs the ActorSystem because the Akka HTTP
    * client (and its Materializer/dispatcher) live on it. Callers pass
    * `context.system`.
    */
  def compute(using system: ActorSystem[?]): String => Future[String] =
    import system.executionContext // EC for the flatMap/unmarshal
    key =>
      val start   = System.nanoTime() // time JUST the outbound round-trip
      val request = HttpRequest(uri = s"http://localhost:9090/backend/$key")
      // singleRequest returns Future[HttpResponse]; unmarshal the body to String.
      // (Unmarshal consumes the entity, so the pooled connection is released.)
      Http()
        .singleRequest(request)
        .flatMap(resp => Unmarshal(resp.entity).to[String])
        .map { body =>
          Metrics.recordUpstream(System.nanoTime() - start)
          body
        }
