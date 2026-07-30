package singleflight

import akka.actor.typed.Behavior
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

// ============================================================================
//  KeyFlight — ONE actor responsible for ONE key
// ============================================================================
//
//  This is the actor that literally "does a single work per key". There is one
//  live instance of it per distinct key (the SingleFlight manager creates them).
//
//  Its whole world is a single key, so its state is tiny:
//    - `fetching` : is the work for MY key currently running?
//    - `waiters`  : who is waiting for MY key's answer right now?
//
//  Compare with the old design, where ONE actor juggled a Map of every key's
//  waiters. Here that map is gone — each key's coalescing lives in its own actor.
// ============================================================================
object KeyFlight:

  // ---- Protocol -----------------------------------------------------------
  sealed trait Command

  /** A caller wants THIS actor's key; mail the answer to `replyTo`. */
  final case class Request(replyTo: ActorRef[String]) extends Command

  // PRIVATE: pipeToSelf mails these back when the fetch resolves. No key field
  // is needed — this actor only ever handles its own single key.
  private final case class Completed(value: String) extends Command
  private final case class Failed(error: Throwable)  extends Command

  // ---- Factory ------------------------------------------------------------
  def apply(key: String, compute: String => Future[String]): Behavior[Command] =
    Behaviors.setup(context => new KeyFlight(context, key, compute))

class KeyFlight(
    context: ActorContext[KeyFlight.Command],
    key: String,
    compute: String => Future[String]
) extends AbstractBehavior[KeyFlight.Command](context):

  import KeyFlight.*

  // Is a fetch for my key in progress?
  private var fetching: Boolean = false
  // Everyone waiting for the current fetch's result.
  private var waiters: List[ActorRef[String]] = Nil

  override def onMessage(msg: Command): Behavior[Command] =
    msg match

      case Request(replyTo) =>
        // Always record the caller as a waiter.
        waiters = replyTo :: waiters
        if !fetching then
          // Nothing running yet -> I am the trigger. Start the work ONCE.
          fetching = true
          context.log.info(s"[start] $key")
          context.pipeToSelf(compute(key)) {
            case Success(value) => Completed(value)
            case Failure(error) => Failed(error)
          }
        else
          // Work already running for my key -> this caller just joins the wait.
          context.log.info(s"[join ] $key  (waiters -> ${waiters.size})")
        this

      case Completed(value) =>
        // Fan the single result out to everyone who waited, then go idle so the
        // NEXT burst for this key starts a fresh fetch.
        context.log.info(s"[done ] $key -> $value  (replying to ${waiters.size})")
        waiters.foreach(_ ! value)
        waiters = Nil
        fetching = false
        this

      case Failed(error) =>
        waiters.foreach(_ ! s"ERROR($key): ${error.getMessage}")
        waiters = Nil
        fetching = false
        this
