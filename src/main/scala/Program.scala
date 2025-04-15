import zio.*
import zio.stream.ZStream

object Program extends ZIOAppDefault {
  val program: ZIO[FileStore[String, String], FileStoreError, Unit] = for {
    fs <- ZIO.service[FileStore[String, String]]
    _ <- fs.createLocation("bucket")
    _ <- fs.write("bucket", "key", ZStream.fromIterable("test data".getBytes))
    data <- fs.read("bucket", "key")
      .flatMap(_.runCollect.map(_.toArray).map(new String(_)))
    _ <- Console.printLine(s"Read data: $data")
      .mapError(e => FileStoreError(e.getMessage))
    keys <- fs.list("bucket", None).runCollect
    _ <- Console.printLine(s"Listed keys: ${keys.mkString(", ")}")
      .mapError(e => FileStoreError(e.getMessage))
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] = program.provide(
    (ZLayer.fromZIO(MockFileStore.make(fail = false)) ++ ZLayer.fromZIO(
      CircuitBreakerImpl
        .make[Any, FileStoreError](threshold = 2, timeout = 1000L),
    )) >>> ProtectedFileStore.layer[String, String],
  ).catchAll(e => Console.printLine(s"Caught error: ${e.getMessage}"))
}
