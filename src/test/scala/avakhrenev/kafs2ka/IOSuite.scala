package avakhrenev.kafs2ka

import cats.effect.{ContextShift, IO, Timer}
import utest.TestSuite

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

// taken from oleg-py/stm4cats, see NondetIOSuite there
abstract class IOSuite extends TestSuite {
  def ec: ExecutionContext = ExecutionContext.global
  implicit def cs: ContextShift[IO] = IO.contextShift(ec)
  implicit def timer: Timer[IO] = IO.timer(ec)


  def ioTimeout: FiniteDuration = 5.second

  override def utestWrap(path: Seq[String], runBody: => Future[Any])(implicit ec: ExecutionContext): Future[Any] = {
    super.utestWrap(path, runBody.flatMap {
      case io: IO[_] => io.timeout(ioTimeout).unsafeToFuture()
      case other => Future.successful(other)
    })(ec)
  }
}
