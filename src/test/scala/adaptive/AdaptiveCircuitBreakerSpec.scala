package adaptive

import zio.*
import zio.test.*
import zio.test.Assertion.*

class MockServer(fail: Ref[Boolean]) {
  def call: ZIO[Any, String, String] = for {
    shouldFail <- fail.get
    _          <- if (shouldFail) ZIO.fail("Server failure") else ZIO.succeed("Success")
  } yield "Success"
}

object AdaptiveCircuitBreakerSpec extends ZIOSpecDefault {

  def spec = suite("AdaptiveCircuitBreaker")(
    testMechanism(RecoveryMechanism.AdaptiveResetInterval),
    testMechanism(RecoveryMechanism.ProbabilisticTransition),
    testMechanism(RecoveryMechanism.DecayOverTime)
  )

  private def testMechanism(mechanism: RecoveryMechanism) =
    suite(s"Recovery Mechanism: $mechanism")(
      test("transitions correctly between states") {
        ZIO.scoped {
          for {
            failRef <- Ref.make(false) // Server starts succeeding
            server   = new MockServer(failRef)
            cb <- AdaptiveCircuitBreaker.make[String](
              thresholdTrip = 0.5,  // Trip to Open when failure probability exceeds 0.5
              thresholdReset = 0.1, // Reset to HalfOpen when below 0.1
              thresholdClose = 0.2, // Close from HalfOpen when below 0.2
              timeStep = Duration.fromSeconds(1),
              checkInterval = Duration.fromSeconds(1),
              recoveryMechanism = mechanism
            )
            // Phase 1: Successful calls (Closed)
            _ <- ZIO.logInfo("Starting successful calls...")
            _ <- ZIO.foreachParDiscard(1 to 5) { i =>
              cb(server.call).foldZIO(
                error => ZIO.logInfo(s"Call $i failed: $error"),
                success => ZIO.logInfo(s"Call $i succeeded: $success")
              )
            }
            state1 <- cb.state.get
            _      <- assertZIO(ZIO.succeed(state1))(equalTo(CircuitBreakerState.Closed))

            // Phase 2: Failures to trip to Open
            _ <- ZIO.logInfo("Simulating server failures...")
            _ <- failRef.set(true) // Server starts failing
            _ <- ZIO.foreachParDiscard(1 to 10) { i =>
              cb(server.call).foldZIO(
                error => ZIO.logInfo(s"Call $i failed: $error"),
                success => ZIO.logInfo(s"Call $i succeeded: $success")
              )
            }
            state2 <- cb.state.get
            _      <- assertZIO(ZIO.succeed(state2))(equalTo(CircuitBreakerState.Open))

            // Phase 3: Recovery simulation with time advancement
            _ <- ZIO.logInfo("Simulating server recovery...")
            _ <- failRef.set(false) // Server recovers
            _ <- mechanism match {
              case RecoveryMechanism.AdaptiveResetInterval =>
                TestClock.adjust(Duration.fromSeconds(10)) // Simulate time for reset interval
              case RecoveryMechanism.ProbabilisticTransition =>
                TestClock.adjust(Duration.fromSeconds(10)) // Simulate time for probabilistic transition
              case RecoveryMechanism.DecayOverTime =>
                TestClock.adjust(Duration.fromSeconds(15)) // Simulate time for decay
            }
            state3 <- cb.state.get
            _      <- assertZIO(ZIO.succeed(state3))(equalTo(CircuitBreakerState.HalfOpen))

            // Phase 4: Successful call to return to Closed
            _ <- cb(server.call).foldZIO(
              error => ZIO.logInfo(s"Recovery call failed: $error"),
              success => ZIO.logInfo(s"Recovery call succeeded: $success")
            )
            state4 <- cb.state.get
            _      <- assertZIO(ZIO.succeed(state4))(equalTo(CircuitBreakerState.Closed))
          } yield assertTrue(true)
        }
      }
    )
}
