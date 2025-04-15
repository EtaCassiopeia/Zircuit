import zio._
import zio.stream._

class MockFileStore(fail: Boolean) extends FileStore[String, String] {
  override def read(
      location: String,
      key: String,
  ): ZIO[Any, FileStoreError, ZStream[Any, FileStoreError, Byte]] =
    if (fail) ZIO.fail(FileStoreError("Mock read failure"))
    else ZIO.succeed(ZStream.fromIterable(s"data from $location/$key".getBytes))

  override def list(
      location: String,
      prefix: Option[String],
  ): ZStream[Any, FileStoreError, String] =
    if (fail) ZStream.fail(FileStoreError("Mock list failure"))
    else ZStream("file1", "file2").map(k => prefix.getOrElse("") + k)

  override def write(
      location: String,
      key: String,
      content: ZStream[Any, FileStoreError, Byte],
  ): ZIO[Any, FileStoreError, Unit] =
    if (fail) ZIO.fail(FileStoreError("Mock write failure"))
    else content.runDrain.unit

  override def delete(
      location: String,
      key: String,
  ): ZIO[Any, FileStoreError, Unit] =
    if (fail) ZIO.fail(FileStoreError("Mock delete failure")) else ZIO.unit

  override def locationExists(
      location: String,
  ): ZIO[Any, FileStoreError, Boolean] =
    if (fail) ZIO.fail(FileStoreError("Mock exists failure"))
    else ZIO.succeed(true)

  override def createLocation(
      location: String,
  ): ZIO[Any, FileStoreError, Unit] =
    if (fail) ZIO.fail(FileStoreError("Mock create failure")) else ZIO.unit
}

object MockFileStore {
  def make(fail: Boolean): UIO[FileStore[String, String]] = ZIO
    .succeed(new MockFileStore(fail))
}

class ToggleFileStore extends FileStore[String, String] {
  private var failNext = true

  override def read(
      location: String,
      key: String,
  ): ZIO[Any, FileStoreError, ZStream[Any, FileStoreError, Byte]] =
    if (failNext) {
      failNext = false
      ZIO.fail(FileStoreError("Mock read failure"))
    } else ZIO.succeed(ZStream.fromIterable(s"data from $location/$key".getBytes))

  override def list(
      location: String,
      prefix: Option[String],
  ): ZStream[Any, FileStoreError, String] = ???

  override def write(
      location: String,
      key: String,
      content: ZStream[Any, FileStoreError, Byte],
  ): ZIO[Any, FileStoreError, Unit] = ???

  override def delete(
      location: String,
      key: String,
  ): ZIO[Any, FileStoreError, Unit] = ???

  override def locationExists(
      location: String,
  ): ZIO[Any, FileStoreError, Boolean] = ???

  override def createLocation(
      location: String,
  ): ZIO[Any, FileStoreError, Unit] = ???
}
