package singleflight

// ============================================================================
//  SingleFlight — the MANAGER (a router over one actor per key)
// ============================================================================
//
//  DESIGN: "one actor doing a single work per key."
//
//    caller ──Get(key,replyTo)──▶  SingleFlight (manager)
//                                     │  keeps Map[key -> per-key actor]
//                                     ▼
//                                  KeyFlight("user:1")   ← owns single-flight for user:1
//                                  KeyFlight("user:2")   ← owns single-flight for user:2
//                                  ...
//
//  The manager holds NO inflight/waiter state itself. Its only job:
//    - first time it sees a key  -> spawn a KeyFlight child for that key.
//    - every time                -> forward the request to that key's child.
//
//  All the real single-flight logic (coalescing, running the work once) lives in
//  KeyFlight, so each key's state is isolated in its own little actor. Compare
//  the earlier version where this one actor held a Map of every key's waiters.
//
//  VOCABULARY (four things)
//  ------------------------
//   1. Behavior[T] : the "code" of an actor; T is the only message type it takes.
//   2. ActorRef[T] : the "address" of an actor.
//   3.  !  (tell)  : send a message and move on. Fire-and-forget.
//   4. replyTo     : the address a request carries so the answer can be mailed back.
// ============================================================================

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

import scala.concurrent.Future

object SingleFlight:

  // ---- Protocol (unchanged, so Demo and Http keep working) ----------------
  sealed trait Command

  /** A caller asking for `key`; the answer is mailed to `replyTo`. */
  final case class Get(key: String, replyTo: ActorRef[String]) extends Command

  // ---- Factory ------------------------------------------------------------
  //
  // `compute` (the expensive op) is injected here and passed down to each
  // per-key child when it's spawned.
  def apply(compute: String => Future[String]): Behavior[Command] =
    Behaviors.setup(context => new SingleFlight(context, compute))

class SingleFlight(
    context: ActorContext[SingleFlight.Command],
    compute: String => Future[String]
) extends AbstractBehavior[SingleFlight.Command](context):

  import SingleFlight.*

  // The manager's ONLY state: which per-key actor handles which key.
  // (No waiters, no inflight flags here — those live inside each KeyFlight.)
  private var workers: Map[String, ActorRef[KeyFlight.Command]] = Map.empty

  override def onMessage(msg: Command): Behavior[Command] =
    msg match
      case Get(key, replyTo) =>
        // Find the per-key actor, or spawn one the first time we see this key.
        val worker = workers.get(key) match
          case Some(w) => w
          case None =>
            val w = context.spawn(KeyFlight(key, compute), childName(key))
            context.log.info(s"[route] spawned worker for $key")
            workers = workers.updated(key, w)
            w

        // Hand the request to the key's own actor — it does the coalescing.
        worker ! KeyFlight.Request(replyTo)
        this

  // Actor names allow only a limited character set, so turn e.g. "user:1" into a
  // safe unique-ish child name. (Uniqueness is guaranteed by the `workers` map:
  // we only spawn once per key.)
  private def childName(key: String): String =
    "key-" + key.replaceAll("[^a-zA-Z0-9]", "_")
