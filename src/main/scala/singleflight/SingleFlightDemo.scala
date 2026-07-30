package singleflight

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Terminated
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

// ============================================================================
//  ACTOR-ONLY DEMO — many Caller actors hammer one SingleFlight actor
// ============================================================================
//
//  This drives the SAME SingleFlight actor as the HTTP server, but with in-JVM
//  actors instead of HTTP clients. Both share `SingleFlight` and `Backend`.
//
//  Run with:  sbt "runMain singleflight.runSingleFlight"
// ============================================================================

/** An independent actor that wants `key`. On birth it fires ONE Get to the
  * SingleFlight actor with its OWN address as replyTo, then waits for its answer.
  */
object Caller:
  // Protocol is just the reply type (String), so `context.self` is
  // ActorRef[String] — exactly what SingleFlight.Get wants for replyTo.
  def apply(name: String, sf: ActorRef[SingleFlight.Command], key: String): Behavior[String] =
    Behaviors.setup(context => new Caller(context, name, sf, key))

class Caller(
    context: ActorContext[String],
    name: String,
    sf: ActorRef[SingleFlight.Command],
    key: String
) extends AbstractBehavior[String](context):

  // The constructor body is the class-style "on start" hook: fire our request.
  context.log.info(s"[$name] asking for $key")
  sf ! SingleFlight.Get(key, context.self)

  override def onMessage(answer: String): Behavior[String] =
    context.log.info(s"[$name] <- $answer")
    Behaviors.stopped // got the answer; done

@main def runSingleFlight(): Unit =

  // Guardian spawns the shared SingleFlight actor + 8 Caller actors, then
  // death-watches them and shuts down once all have answered.
  val guardian: Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
    // The reusable actor, wired to the shared backend. `Backend.compute` needs
    // the system for its scheduler-based (non-blocking) delay.
    val sf = context.spawn(SingleFlight(Backend.compute(using context.system)), "single-flight")

    val callers = Seq(
      "alice" -> "user:1",
      "bob"   -> "user:1",
      "carol" -> "user:1",
      "dan"   -> "user:1",
      "eve"   -> "user:1",
      "frank" -> "user:2",
      "grace" -> "user:2",
      "heidi" -> "user:2"
    )

    callers.foreach { (name, key) =>
      val caller = context.spawn(Caller(name, sf, key), name)
      context.watch(caller) // deliver a Terminated signal when it stops
    }

    var alive = callers.size
    Behaviors.receiveSignal { case (_, Terminated(_)) =>
      alive -= 1
      if alive == 0 then
        println(
          s">>> All ${callers.size} caller actors got an answer, but compute() " +
            s"ran only ${Backend.calls} time(s) (one per distinct key)."
        )
        Behaviors.stopped
      else Behaviors.same
    }
  }

  ActorSystem[Nothing](guardian, "single-flight-system")
