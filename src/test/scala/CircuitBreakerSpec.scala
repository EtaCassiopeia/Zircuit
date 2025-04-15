import zio.*
import zio.test.*
import zio.test.Assertion.*

object CircuitBreakerSpec extends ZIOSpecDefault {

  def failingFileStore: UIO[FileStore[String, String]] = MockFileStore
    .make(fail = true)

  def succeedingFileStore: UIO[FileStore[String, String]] = MockFileStore
    .make(fail = false)

  def makeCircuitBreaker(
      threshold: Int,
      timeout: Long,
  ): UIO[CircuitBreaker[Any, FileStoreError]] = CircuitBreakerImpl
    .make[Any, FileStoreError](threshold, timeout)

  def spec: Spec[Any, TestFailure[FileStoreError]] = suite("CircuitBreakerSpec")(
    test("should allow operations when closed") {
      for {
        fs <- succeedingFileStore
        cb <- makeCircuitBreaker(threshold = 3, timeout = 1000L)
        protectedFs = new ProtectedFileStore(fs, cb)
        result <- protectedFs.read("bucket", "key").flatMap(stream =>
          stream.runCollect.map(chunk => new String(chunk.toArray)),
        ).mapError(TestFailure.fail)
      } yield assertTrue(result == "data from bucket/key")
    },
    test("should trip to open after threshold failures") {
      for {
        fs <- failingFileStore
        cb <- makeCircuitBreaker(threshold = 2, timeout = 1000L)
        protectedFs = new ProtectedFileStore(fs, cb)
        _ <- protectedFs.read("bucket", "key").ignore // Perform the first failing read, incrementing the failure count to 1 (threshold not yet reached)
        _ <- protectedFs.read("bucket", "key").ignore // Perform the second failing read, hitting the threshold (2) and tripping the breaker to Open
        result <- protectedFs.read("bucket", "key").either // Perform a third read, which should fail immediately because the breaker is Open
      } yield assert(result)(isLeft(
        equalTo(FileStoreError("Circuit breaker is open")),
      ))
    },
    test("should transition to half-open after timeout and allow retry") {
      for {
        fs <- failingFileStore
        cb <- makeCircuitBreaker(threshold = 1, timeout = 100L)
        protectedFs = new ProtectedFileStore(fs, cb)
        _ <- protectedFs.read("bucket", "key").ignore // Trips the breaker to Open
        _ <- TestClock.adjust(150.millis) // Advances the clock by 150ms
        result <- protectedFs.read("bucket", "key").either // Should retry in HalfOpen
      } yield assert(result)(isLeft(equalTo(FileStoreError("Mock read failure"))))
    }, // Provides TestClock for the test
    test("should reset to closed after successful half-open test") {
      for {
        fs <- ZIO.succeed(new ToggleFileStore()) // Custom FileStore: fails once, then succeeds
        cb <- makeCircuitBreaker(threshold = 1, timeout = 100L) // Threshold 1, timeout 100ms
        protectedFs = new ProtectedFileStore(fs, cb) // Wrap FileStore with breaker
        _ <- protectedFs.read("bucket", "key").ignore // First read fails, trips to Open
        _ <- TestClock.adjust(150.millis) // Advance clock past timeout, moves to HalfOpen
        result1 <- protectedFs.read("bucket", "key").either // Succeeds in HalfOpen, resets to Closed
        result2 <- protectedFs.read("bucket", "key").either // Succeeds again, confirms Closed
      } yield assert(result1)(isRight) && assert(result2)(isRight) // Verify both reads succeed
    },
  )
}
