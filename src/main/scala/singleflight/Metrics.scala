package singleflight

import java.util.concurrent.atomic.AtomicLong

/** Tiny in-process metrics so we can read, at runtime via GET /stats:
  *   - the single-flight COALESCING RATIO (inbound requests per outbound call)
  *   - the UPSTREAM latency (time spent in the third-party call), measured
  *     separately from the total response time Gatling sees.
  *
  * Comparing the two answers the "why 1200ms?" question directly: if avg upstream
  * is ~800ms but Gatling's p50 is ~1400ms, the extra ~600ms is coalescing wait +
  * warm-up/GC/queueing — NOT the upstream.
  */
object Metrics:

  /** Every inbound GET /value/<key>. */
  val inbound = new AtomicLong(0)

  // Outbound = work that got PAST coalescing (one real third-party call).
  private val outbound           = new AtomicLong(0)
  private val upstreamTotalNanos = new AtomicLong(0)
  private val upstreamMaxNanos   = new AtomicLong(0)

  def outboundCalls: Long = outbound.get()

  /** Record one completed upstream call and how long it took. */
  def recordUpstream(nanos: Long): Unit =
    outbound.incrementAndGet()
    upstreamTotalNanos.addAndGet(nanos)
    var cur = upstreamMaxNanos.get()
    while nanos > cur && !upstreamMaxNanos.compareAndSet(cur, nanos) do
      cur = upstreamMaxNanos.get()

  /** Human-readable snapshot for the /stats endpoint. */
  def snapshot: String =
    val in    = inbound.get()
    val out   = outbound.get()
    val ratio = if out == 0 then 0.0 else in.toDouble / out.toDouble
    val avgMs = if out == 0 then 0.0 else upstreamTotalNanos.get().toDouble / out / 1e6
    val maxMs = upstreamMaxNanos.get().toDouble / 1e6
    f"""inbound requests : $in
       |outbound calls   : $out
       |coalescing ratio : $ratio%.1f : 1   (inbound per outbound)
       |avg upstream time: $avgMs%.0f ms
       |max upstream time: $maxMs%.0f ms
       |""".stripMargin
