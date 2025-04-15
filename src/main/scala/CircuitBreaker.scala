import java.util.concurrent.TimeUnit

import zio.*
import zio.prelude.fx.ZPure
import zio.stm.STM
import zio.stm.TRef

trait CircuitBreaker[-R, +E] {
  def withCircuitBreaker[R1 <: R, E1 >: E, B](
      zio: ZIO[R1, E1, B],
      onOpen: => E1,
  ): ZIO[R1, E1, B]
}

sealed trait BreakerState
case object Closed extends BreakerState
case object Open extends BreakerState
case object HalfOpen extends BreakerState

case class CircuitBreakerState(
    state: BreakerState,
    failureCount: Int,
    tripTime: Option[Long], // Time (ms) when tripped to Open
    isTesting: Boolean, // Whether a test is in progress in HalfOpen
)

type CBPure[A] =
  ZPure[Nothing, CircuitBreakerState, CircuitBreakerState, Any, Nothing, A]

class CircuitBreakerImpl[R, E](
    threshold: Int,
    timeout: Long,
    stateRef: TRef[CircuitBreakerState],
) extends CircuitBreaker[R, E] {
  override def withCircuitBreaker[R1 <: R, E1 >: E, A](
      zio: ZIO[R1, E1, A],
      onOpen: => E1,
  ): ZIO[R1, E1, A] = for {
    now <- Clock.currentTime(TimeUnit.MILLISECONDS)
    shouldProceed <- STM.atomically {
      for {
        state <- stateRef.get
        proceed = decideProceed(now, threshold, timeout).run(state)._2
        _ <-
          if (proceed && state.state == Open) stateRef
            .set(state.copy(state = HalfOpen, tripTime = None, isTesting = true))
          else STM.unit
      } yield proceed
    }
    result <-
      if (shouldProceed) zio.tapBoth(
        _ =>
          STM.atomically {
            stateRef
              .update(state => updateOnFailure(now, threshold).run(state)._1)
          },
        _ =>
          STM
            .atomically(stateRef.update(state => updateOnSuccess().run(state)._1)),
      )
      else ZIO.fail(onOpen)
  } yield result

  private def decideProceed(
      now: Long,
      threshold: Int,
      timeout: Long,
  ): CBPure[Boolean] = ZPure.get[CircuitBreakerState].map { state =>
    state.state match {
      case Closed => true
      case Open if now > state.tripTime.getOrElse(0L) + timeout => true
      case Open => false
      case HalfOpen if !state.isTesting => true
      case HalfOpen => false
    }
  }

  private def updateOnSuccess(): CBPure[Unit] = ZPure.update { state =>
    state.state match {
      case Closed => state.copy(failureCount = 0)
      case HalfOpen => state.copy(
          state = Closed,
          failureCount = 0,
          tripTime = None,
          isTesting = false,
        )
      case Open => state
    }
  }

  private def updateOnFailure(now: Long, threshold: Int): CBPure[Unit] = ZPure
    .update { state =>
      state.state match {
        case Closed =>
          val newCount = state.failureCount + 1
          if (newCount >= threshold) state.copy(
            state = Open,
            failureCount = 0,
            tripTime = Some(now),
            isTesting = false,
          )
          else state.copy(failureCount = newCount)
        case HalfOpen => state
            .copy(state = Open, tripTime = Some(now), isTesting = false)
        case Open => state
      }
    }
}

object CircuitBreakerImpl {
  def make[R, E](threshold: Int, timeout: Long): UIO[CircuitBreakerImpl[R, E]] =
    for {
      stateRef <- STM
        .atomically(TRef.make(CircuitBreakerState(Closed, 0, None, false)))
    } yield new CircuitBreakerImpl[R, E](threshold, timeout, stateRef)
}
