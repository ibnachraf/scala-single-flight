package singleflight

import com.sun.net.httpserver.HttpServer

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

// ============================================================================
//  A fake THIRD-PARTY service — the slow upstream our backend calls.
// ============================================================================
//
//  Built on the JDK's built-in HttpServer with a VIRTUAL-THREAD-per-task
//  executor (Project Loom). Every incoming request is handled on its own
//  virtual thread, so the `Thread.sleep(800)` that simulates slow work PARKS
//  that virtual thread instead of blocking a platform thread. Result: this tiny
//  server can hold thousands of concurrent slow requests on a few carriers.
//
//  This models a real external dependency (a payments API, a DB proxy, ...) that
//  is slow but highly concurrent. Our SingleFlight service calls it over HTTP.
//
//  Run FIRST, before the SingleFlight server:
//    sbt "runMain singleflight.runThirdParty"
//  Test it directly:
//    curl localhost:9090/backend/user:1
// ============================================================================
@main def runThirdParty(): Unit =
  // Simulated upstream latency — a realistic DB / third-party call (~100ms).
  // Override with -Dtp.latency=<ms>.
  val latencyMs = sys.props.getOrElse("tp.latency", "2000").toInt

  // backlog=1024 so a burst of new connections isn't refused at the socket.
  val server = HttpServer.create(new InetSocketAddress(9090), 1024)

  // LOOM: one virtual thread per request.
  server.setExecutor(Executors.newVirtualThreadPerTaskExecutor())

  server.createContext(
    "/backend",
    exchange => {
      // Simulate upstream work. On a virtual thread this parks — it does NOT
      // tie up an OS thread — so concurrency is effectively unbounded.
      Thread.sleep(latencyMs.toLong)

      val key  = exchange.getRequestURI.getPath.stripPrefix("/backend/")
      val body = s"upstream-value-for-$key".getBytes(StandardCharsets.UTF_8)

      exchange.sendResponseHeaders(200, body.length.toLong)
      val os = exchange.getResponseBody
      try os.write(body)
      finally os.close()
    }
  )

  server.start()
  println("Third-party (Loom) service on http://localhost:9090/backend/<key>")
  println(s"Handling each request on its own virtual thread; ~${latencyMs}ms latency.")
