package adaptive

import AdaptiveCircuitBreakerSpec.{suite, test}
import zio.*
import zio.test.{ZIOSpecDefault, assertTrue, *}
import zio.metrics.{Metric, MetricLabel}
import java.time.Instant
import scala.util.Random
import nl.vroste.rezilience.CircuitBreaker as RezilienceCircuitBreaker
import nl.vroste.rezilience.CircuitBreaker.{CircuitBreakerCallError, StateChange as RezilienceStateChange}
import zio.stream.ZStream
import java.time.{ZoneId, Duration as JavaDuration}

case object ServiceError extends Exception("Service failed")

sealed trait CommonCircuitBreakerError[+E]
case object CommonCircuitBreakerOpen extends CommonCircuitBreakerError[Nothing]
case class CommonWrappedError[E](error: E) extends CommonCircuitBreakerError[E]

case class BenchmarkMetrics(
                             successfulCalls: Int,
                             failedCalls: Int,
                             rejectedCalls: Int,
                             stateChanges: Int,
                             timeInOpen: Duration
                           )

def runBenchmark(
                  cb: Any,
                  outcomes: List[Boolean],
                  testClock: TestClock,
                  isAdaptive: Boolean
                ): ZIO[Any, Nothing, BenchmarkMetrics] = {
  type E = Exception
  ZIO.scoped {
    for {
      callIndexRef <- Ref.make(0)
      serviceCall = for {
        index <- callIndexRef.getAndUpdate(_ + 1)
        outcome = outcomes(index)
        result <- if (outcome) ZIO.succeed("success") else ZIO.fail(ServiceError)
      } yield result

      // Collect state changes into a Ref
      stateChangesRef <- Ref.make(Chunk.empty[Any])

      _ <- if (isAdaptive) {
        for {
          stateChangesQueue <- cb.asInstanceOf[AdaptiveCircuitBreaker[E]].stateChangesHub.subscribe
          stream = ZStream.fromQueue(stateChangesQueue)
          _ <- stream.runForeach(change => ZIO.debug(s"State change [isAdaptive]: $change") *>  stateChangesRef.update(_ :+ change)).fork
        } yield ()
      } else {
        for {
          stateChangesQueue <- cb.asInstanceOf[RezilienceCircuitBreaker[E]].stateChanges
          stream = ZStream.fromQueue(stateChangesQueue)
          _ <- stream.runForeach(change => ZIO.debug(s"State change [Rezilience]: $change") *> stateChangesRef.update(_ :+ change)).fork
        } yield ()
      }

      // Execute service calls and advance clock
      results <- ZIO.foreach(outcomes.indices.toList) { _ =>
        val effect = if (isAdaptive)
          cb.asInstanceOf[AdaptiveCircuitBreaker[E]].apply(serviceCall)
            .mapError {
              case CircuitBreakerError.CircuitBreakerOpen => CommonCircuitBreakerOpen
              case CircuitBreakerError.WrappedError(e) => CommonWrappedError(e)
            }
        else
          cb.asInstanceOf[RezilienceCircuitBreaker[E]].apply(serviceCall)
            .mapError {
              case RezilienceCircuitBreaker.CircuitBreakerOpen => CommonCircuitBreakerOpen
              case RezilienceCircuitBreaker.WrappedError(e) => CommonWrappedError(e)
            }
        effect.either <* testClock.adjust(100.millis)
      }

      _ <- testClock.adjust(100.millis)

      // Retrieve collected state changes
      stateChanges <- stateChangesRef.get

      // Compute metrics
      successfulCalls <- ZIO.succeed(results.count(_.isRight))
      failedCalls <- ZIO.succeed(results.count {
        case Left(CommonWrappedError(_)) => true
        case _ => false
      })
      rejectedCalls <- ZIO.succeed(results.count {
        case Left(CommonCircuitBreakerOpen) => true
        case _ => false
      })
      timeInOpen <- ZIO.succeed {
        if (isAdaptive) {
          computeTimeInStateAdaptive(stateChanges.asInstanceOf[Chunk[StateChange]], CircuitBreakerState.Open)
        } else {
          computeTimeInStateRezilience(stateChanges.asInstanceOf[Chunk[RezilienceStateChange]], RezilienceCircuitBreaker.State.Open)
        }
      }
    } yield BenchmarkMetrics(
      successfulCalls,
      failedCalls,
      rejectedCalls,
      stateChanges.length,
      timeInOpen
    )
  }
}

// Helper functions to compute time in state
def computeTimeInStateAdaptive(stateChanges: Chunk[StateChange], targetState: CircuitBreakerState): Duration = {
  stateChanges
    .sliding(2)
    .collect {
      case Chunk(StateChange(from, to, at)) if from == targetState =>
        (at, stateChanges.find(_.from == to).map(_.at).getOrElse(Instant.now()))
    }
    .map { case (start, end) => JavaDuration.between(start, end) }
    .foldLeft(JavaDuration.ZERO)(_ plus _)
}

def computeTimeInStateRezilience(stateChanges: Chunk[RezilienceStateChange], targetState: RezilienceCircuitBreaker.State): Duration = {
  stateChanges
    .sliding(2)
    .collect {
      case Chunk(RezilienceStateChange(from, to, at)) if from == targetState =>
        (at, stateChanges.find(_.from == to).map(_.at).getOrElse(Instant.now()))
    }
    .map { case (start, end) => JavaDuration.between(start, end) }
    .foldLeft(JavaDuration.ZERO)(_ plus _)
}

def benchmark(testClock: TestClock): ZIO[Any, Nothing, Unit] = {
  val rng = new Random(42)
  val totalCalls = 1000
  val outcomes = (0 until totalCalls).map { i =>
    val successRate = if ((i / 100) % 2 == 0) 0.9 else 0.1
    rng.nextDouble() < successRate
  }.toList

  ZIO.scoped {
    for {
      rezilienceCB <- RezilienceCircuitBreaker.withMaxFailures[Exception](maxFailures = 5)
      adaptiveCBDecayOverTime <- AdaptiveCircuitBreaker.make[Exception](timeStep = 100.millis, recoveryMechanism= RecoveryMechanism.DecayOverTime)
      adaptiveCBProbabilisticTransition <- AdaptiveCircuitBreaker.make[Exception](timeStep = 100.millis, recoveryMechanism= RecoveryMechanism.ProbabilisticTransition)
      adaptiveCBAdaptiveResetInterval <- AdaptiveCircuitBreaker.make[Exception](timeStep = 100.millis, recoveryMechanism= RecoveryMechanism.AdaptiveResetInterval)
      rezilienceMetrics <- runBenchmark(rezilienceCB, outcomes, testClock, isAdaptive = false)
      _ <- testClock.setTime(Instant.now())
      adaptiveMetricsDecayOverTime <- runBenchmark(adaptiveCBDecayOverTime, outcomes, testClock, isAdaptive = true)
      adaptiveMetricsProbabilisticTransition <- runBenchmark(adaptiveCBProbabilisticTransition, outcomes, testClock, isAdaptive = true)
      adaptiveMetricsAdaptiveResetInterval <- runBenchmark(adaptiveCBAdaptiveResetInterval, outcomes, testClock, isAdaptive = true)
      _ <- ZIO.debug(s"Rezilienze Circuit Breaker Metrics: $rezilienceMetrics")
      _ <- ZIO.debug(s"Adaptive Circuit Breaker Metrics [recoveryMechanism: RecoveryMechanism.DecayOverTime]:\n $adaptiveMetricsDecayOverTime")
      _ <- ZIO.debug(s"Adaptive Circuit Breaker Metrics [recoveryMechanism: RecoveryMechanism.ProbabilisticTransition]:\n $adaptiveMetricsProbabilisticTransition")
      _ <- ZIO.debug(s"Adaptive Circuit Breaker Metrics [recoveryMechanism: RecoveryMechanism.AdaptiveResetInterval]:\n $adaptiveMetricsAdaptiveResetInterval")
    } yield ()
  }
}

object BenchmarkApp extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment & Scope, Any] = suite(s"Benchmark")(
    test("Run benchmark") {
      for {
        testClock <- ZIO.service[TestClock]
        _ <- testClock.setTime(Instant.now())
        _ <- testClock.setTimeZone(ZoneId.of("UTC"))
        _ <- benchmark(testClock)
      } yield assertTrue(true)
    }
  ).provideLayer(TestClock.default)
}
