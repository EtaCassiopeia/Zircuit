package adaptive

import zio.*
import zio.metrics.{Metric, MetricLabel}
import zio.stream.ZStream

import java.time.Instant
import scala.language.postfixOps
import scala.util.Random

sealed trait CircuitBreakerState
object CircuitBreakerState {
  case object Closed   extends CircuitBreakerState
  case object Open     extends CircuitBreakerState
  case object HalfOpen extends CircuitBreakerState
}

sealed trait CircuitBreakerError[+E]
object CircuitBreakerError {
  case object CircuitBreakerOpen       extends CircuitBreakerError[Nothing]
  case class WrappedError[E](error: E) extends CircuitBreakerError[E]
}

sealed trait RecoveryMechanism
object RecoveryMechanism {
  case object AdaptiveResetInterval   extends RecoveryMechanism
  case object ProbabilisticTransition extends RecoveryMechanism
  case object DecayOverTime           extends RecoveryMechanism
}

// Hidden Markov Model (HMM) Belief State
object HMM {
  case class BeliefState(belief: Array[Double], lastUpdate: Instant)

  def getCurrentBelief(
                        state: BeliefState,
                        now: Instant,
                        timeStep: Duration,
                        transitionMatrix: Array[Array[Double]]
                      ): Array[Double] = {
    val elapsedSteps  = ((now.toEpochMilli - state.lastUpdate.toEpochMilli) / timeStep.toMillis).toInt
    var currentBelief = state.belief
    for (_ <- 0 until elapsedSteps) {
      currentBelief = Array(
        currentBelief(0) * transitionMatrix(0)(0) + currentBelief(1) * transitionMatrix(1)(0),
        currentBelief(0) * transitionMatrix(0)(1) + currentBelief(1) * transitionMatrix(1)(1)
      )
    }
    currentBelief
  }

  def updateBeliefOnCall(
                          state: BeliefState,
                          now: Instant,
                          success: Boolean,
                          timeStep: Duration,
                          transitionMatrix: Array[Array[Double]],
                          emissionSuccess: Array[Double],
                          emissionFailure: Array[Double]
                        ): BeliefState = {
    val currentBelief = getCurrentBelief(state, now, timeStep, transitionMatrix)
    val emission      = if (success) emissionSuccess else emissionFailure
    val newBelief = Array(
      currentBelief(0) * emission(0),
      currentBelief(1) * emission(1)
    )
    val sum = newBelief.sum
    BeliefState(newBelief.map(_ / sum), now)
  }
}

class AdaptiveCircuitBreaker[E](
                                 beliefStateRef: Ref[HMM.BeliefState],
                                 val state: Ref[CircuitBreakerState],
                                 halfOpenSwitch: Ref[Boolean],
                                 resetRequests: Queue[Unit],
                                 val stateChangesHub: Hub[StateChange],
                                 isFailure: PartialFunction[E, Boolean],
                                 labels: Option[Set[MetricLabel]],
                                 thresholdTrip: Double,
                                 thresholdReset: Double,
                                 thresholdClose: Double,
                                 checkInterval: Duration,
                                 timeStep: Duration,
                                 transitionMatrix: Array[Array[Double]],
                                 emissionSuccess: Array[Double],
                                 emissionFailure: Array[Double],
                                 recoveryMechanism: RecoveryMechanism
                               ) {
  import CircuitBreakerError.*
  import CircuitBreakerState.*

  private val metrics = labels.map { labels =>
    CircuitBreakerMetrics(
      state = Metric.gauge("circuit_breaker_state").tagged(labels),
      nrStateChanges = Metric.counter("circuit_breaker_state_changes").tagged(labels),
      callsSuccess = Metric.counter("circuit_breaker_calls_success").tagged(labels),
      callsFailure = Metric.counter("circuit_breaker_calls_failure").tagged(labels),
      callsRejected = Metric.counter("circuit_breaker_calls_rejected").tagged(labels)
    )
  }

  val initializeMetrics: UIO[Unit] =
    withMetrics { m =>
      m.state.set(0.0) *>
        m.nrStateChanges.update(0L) *>
        m.callsSuccess.update(0L) *>
        m.callsFailure.update(0L) *>
        m.callsRejected.update(0L)
    }

  private def withMetrics(f: CircuitBreakerMetrics => UIO[Unit]): UIO[Unit] =
    ZIO.fromOption(metrics).flatMap(f).ignore

  private val changeToOpen: ZIO[Any, Nothing, Unit] = for {
    oldState <- state.getAndSet(Open)
    _        <- resetRequests.offer(())
    now      <- ZIO.clockWith(_.instant)
    _        <- stateChangesHub.publish(StateChange(oldState, Open, now))
  } yield ()

  private val changeToClosed: ZIO[Any, Nothing, Unit] = for {
    now      <- ZIO.clockWith(_.instant)
    oldState <- state.getAndSet(Closed)
    _        <- stateChangesHub.publish(StateChange(oldState, Closed, now))
  } yield ()

  private val adaptiveResetIntervalProcess: ZIO[Scope, Nothing, Any] = ZStream
    .fromQueue(resetRequests)
    .mapZIO { _ =>
      for {
        now            <- ZIO.clockWith(_.instant)
        currentBelief  <- beliefStateRef.get.map(s => HMM.getCurrentBelief(s, now, timeStep, transitionMatrix))
        pFailed         = currentBelief(1)
        stepsToRecovery = (math.log(thresholdReset / pFailed) / math.log(transitionMatrix(1)(0))).toInt
        resetInterval   = Duration.fromSeconds(stepsToRecovery * timeStep.toSeconds)
        _              <- ZIO.sleep(resetInterval)
        _              <- halfOpenSwitch.set(true)
        _              <- state.set(HalfOpen)
        now            <- ZIO.clockWith(_.instant)
        _              <- stateChangesHub.publish(StateChange(Open, HalfOpen, now))
      } yield ()
    }
    .runDrain
    .forkScoped

  private val probabilisticTransitionProcess: ZIO[Scope, Nothing, Any] = ZStream
    .fromQueue(resetRequests)
    .mapZIO { _ =>
      def loop: ZIO[Any, Nothing, Unit] = for {
        now              <- ZIO.clockWith(_.instant)
        currentBelief    <- beliefStateRef.get.map(s => HMM.getCurrentBelief(s, now, timeStep, transitionMatrix))
        pFailed           = currentBelief(1)
        transitionProb    = 1 - pFailed
        shouldTransition <- ZIO.succeed(Random.nextDouble() < transitionProb)
        _ <- if (shouldTransition) {
          halfOpenSwitch.set(true) *>
            state.set(HalfOpen) *>
            ZIO.clockWith(_.instant).flatMap(now => stateChangesHub.publish(StateChange(Open, HalfOpen, now)))
        } else {
          loop.delay(checkInterval)
        }
      } yield ()
      loop
    }
    .runDrain
    .forkScoped

  private val decayOverTimeProcess: ZIO[Scope, Nothing, Any] = ZStream
    .fromQueue(resetRequests)
    .mapZIO { _ =>
      def loop: ZIO[Any, Nothing, Unit] = for {
        now           <- ZIO.clockWith(_.instant)
        currentBelief <- beliefStateRef.get.map(s => HMM.getCurrentBelief(s, now, timeStep, transitionMatrix))
        pFailed        = currentBelief(1)
        decayedPFailed = pFailed * 0.99
        _             <- beliefStateRef.update(s => HMM.BeliefState(Array(1 - decayedPFailed, decayedPFailed), now))
        _ <- if (decayedPFailed < thresholdReset) {
          halfOpenSwitch.set(true) *>
            state.set(HalfOpen) *>
            ZIO.clockWith(_.instant).flatMap(now => stateChangesHub.publish(StateChange(Open, HalfOpen, now)))
        } else {
          loop.delay(checkInterval)
        }
      } yield ()
      loop
    }
    .runDrain
    .forkScoped

  val resetProcess: ZIO[Scope, Nothing, Any] = recoveryMechanism match {
    case RecoveryMechanism.AdaptiveResetInterval =>
      adaptiveResetIntervalProcess
    case RecoveryMechanism.ProbabilisticTransition =>
      probabilisticTransitionProcess
    case RecoveryMechanism.DecayOverTime =>
      decayOverTimeProcess
  }

  val trackStateChanges: ZIO[Scope, Nothing, Any] = for {
    stateChanges <- stateChangesHub.subscribe
    _ <- ZStream
      .fromQueue(stateChanges)
      .tap { stateChange =>
        val stateAsInt = stateChange.to match {
          case Closed   => 0
          case HalfOpen => 1
          case Open     => 2
        }
        withMetrics(m => m.nrStateChanges.increment *> m.state.set(stateAsInt.doubleValue))
      }
      .runDrain
      .forkScoped
  } yield ()

  def apply[R, E1 <: E, A](f: ZIO[R, E1, A]): ZIO[R, CircuitBreakerError[E1], A] =
    (for {
      currentState <- state.get
      result <- currentState match {
        case Closed =>
          def onComplete(callSuccessful: Boolean) = for {
            now <- ZIO.clockWith(_.instant)
            _ <- beliefStateRef.update { s =>
              HMM.updateBeliefOnCall(
                s,
                now,
                callSuccessful,
                timeStep,
                transitionMatrix,
                emissionSuccess,
                emissionFailure
              )
            }
            currentBelief <- beliefStateRef.get.map(_.belief)
            pFailed        = currentBelief(1)
            currentState  <- state.get
            _             <- changeToOpen.when(currentState == Closed && pFailed > thresholdTrip)
          } yield ()

          tapZIOOnUserDefinedFailure(f)(
            onFailure = onComplete(false),
            onSuccess = onComplete(true)
          ).mapError(WrappedError(_))

        case Open =>
          ZIO.fail(CircuitBreakerOpen)

        case HalfOpen =>
          for {
            isFirstCall <- halfOpenSwitch.getAndUpdate(_ => false)
            result <- if (isFirstCall) {
              def onComplete(callSuccessful: Boolean) = for {
                now <- ZIO.clockWith(_.instant)
                _ <- beliefStateRef.update { s =>
                  HMM.updateBeliefOnCall(
                    s,
                    now,
                    callSuccessful,
                    timeStep,
                    transitionMatrix,
                    emissionSuccess,
                    emissionFailure
                  )
                }
                currentBelief <- beliefStateRef.get.map(_.belief)
                pFailed        = currentBelief(1)
                _             <- if (pFailed < thresholdClose) changeToClosed else changeToOpen
              } yield ()

              tapZIOOnUserDefinedFailure(f)(
                onFailure = onComplete(false),
                onSuccess = onComplete(true)
              ).mapError(WrappedError(_))
            } else {
              ZIO.fail(CircuitBreakerOpen)
            }
          } yield result
      }
    } yield result)
      .tapBoth(
        {
          case CircuitBreakerOpen => withMetrics(_.callsRejected.increment)
          case WrappedError(_)    => withMetrics(_.callsFailure.increment)
        },
        _ => withMetrics(_.callsSuccess.increment)
      )

  private def tapZIOOnUserDefinedFailure[R, E1 <: E, A](
                                                         f: ZIO[R, E1, A]
                                                       )(onFailure: ZIO[R, E1, Any], onSuccess: ZIO[R, E1, Any]): ZIO[R, E1, A] =
    f.tapBoth(
      e => if (isFailure.applyOrElse(e, (_: E1) => false)) onFailure else onSuccess,
      _ => onSuccess
    )
}

object AdaptiveCircuitBreaker {
  def make[E](
               thresholdTrip: Double = 0.9,
               thresholdReset: Double = 0.1,
               thresholdClose: Double = 0.2,
               timeStep: Duration = 1 second,
               transitionMatrix: Array[Array[Double]] = Array(Array(0.99, 0.01), Array(0.05, 0.95)),
               emissionSuccess: Array[Double] = Array(0.99, 0.01),
               emissionFailure: Array[Double] = Array(0.01, 0.99),
               checkInterval: Duration = 1 second,
               isFailure: PartialFunction[E, Boolean] = { (_: E) => true },
               metricLabels: Option[Set[MetricLabel]] = None,
               recoveryMechanism: RecoveryMechanism = RecoveryMechanism.AdaptiveResetInterval
             ): ZIO[Scope, Nothing, AdaptiveCircuitBreaker[E]] =
    for {
      beliefStateRef  <- Ref.make(HMM.BeliefState(Array(1.0, 0.0), Instant.now()))
      state           <- Ref.make[CircuitBreakerState](CircuitBreakerState.Closed)
      halfOpenSwitch  <- Ref.make[Boolean](true)
      resetRequests   <- Queue.bounded[Unit](1)
      stateChangesHub <- Hub.sliding[StateChange](32).withFinalizer(_.shutdown)
      cb = new AdaptiveCircuitBreaker[E](
        beliefStateRef,
        state,
        halfOpenSwitch,
        resetRequests,
        stateChangesHub,
        isFailure,
        metricLabels,
        thresholdTrip,
        thresholdReset,
        thresholdClose,
        checkInterval,
        timeStep,
        transitionMatrix,
        emissionSuccess,
        emissionFailure,
        recoveryMechanism
      )
      _ <- cb.initializeMetrics
      _ <- cb.resetProcess
      _ <- cb.trackStateChanges
    } yield cb
}

case class StateChange(from: CircuitBreakerState, to: CircuitBreakerState, at: Instant)

case class CircuitBreakerMetrics(
                                  state: Metric.Gauge[Double],
                                  nrStateChanges: Metric.Counter[Long],
                                  callsSuccess: Metric.Counter[Long],
                                  callsFailure: Metric.Counter[Long],
                                  callsRejected: Metric.Counter[Long]
                                )
