import zio._
import zio.stream.ZStream

trait FileStore[Location, Key] {
  def read(
      location: Location,
      key: Key,
  ): ZIO[Any, FileStoreError, ZStream[Any, FileStoreError, Byte]]
  def list(
      location: Location,
      prefix: Option[Key],
  ): ZStream[Any, FileStoreError, Key]
  def write(
      location: Location,
      key: Key,
      content: ZStream[Any, FileStoreError, Byte],
  ): ZIO[Any, FileStoreError, Unit]
  def delete(location: Location, key: Key): ZIO[Any, FileStoreError, Unit]
  def locationExists(location: Location): ZIO[Any, FileStoreError, Boolean]
  def createLocation(location: Location): ZIO[Any, FileStoreError, Unit]
}

case class FileStoreError(message: String, cause: Option[Throwable] = None)
    extends Exception(message, cause.orNull)

class ProtectedFileStore[Location, Key](
    underlying: FileStore[Location, Key],
    cb: CircuitBreaker[Any, FileStoreError],
) extends FileStore[Location, Key] {
  private val onOpenError = FileStoreError("Circuit breaker is open")

  override def read(
      location: Location,
      key: Key,
  ): ZIO[Any, FileStoreError, ZStream[Any, FileStoreError, Byte]] = cb
    .withCircuitBreaker(underlying.read(location, key), onOpenError)

  override def list(
      location: Location,
      prefix: Option[Key],
  ): ZStream[Any, FileStoreError, Key] = ZStream.fromZIO(cb.withCircuitBreaker(
    underlying.list(location, prefix).runCollect,
    onOpenError,
  )).flatMap(ZStream.fromChunk(_))

  override def write(
      location: Location,
      key: Key,
      content: ZStream[Any, FileStoreError, Byte],
  ): ZIO[Any, FileStoreError, Unit] = cb
    .withCircuitBreaker(underlying.write(location, key, content), onOpenError)

  override def delete(
      location: Location,
      key: Key,
  ): ZIO[Any, FileStoreError, Unit] = cb
    .withCircuitBreaker(underlying.delete(location, key), onOpenError)

  override def locationExists(
      location: Location,
  ): ZIO[Any, FileStoreError, Boolean] = cb
    .withCircuitBreaker(underlying.locationExists(location), onOpenError)

  override def createLocation(
      location: Location,
  ): ZIO[Any, FileStoreError, Unit] = cb
    .withCircuitBreaker(underlying.createLocation(location), onOpenError)
}

object ProtectedFileStore {
  def layer[Location: Tag, Key: Tag]: ZLayer[
    FileStore[Location, Key] with CircuitBreaker[Any, FileStoreError],
    Nothing,
    FileStore[Location, Key],
  ] = ZLayer.fromFunction(new ProtectedFileStore[Location, Key](_, _))
}
