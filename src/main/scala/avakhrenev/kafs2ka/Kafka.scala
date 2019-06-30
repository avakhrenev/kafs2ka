package avakhrenev.kafs2ka

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import avakhrenev.kafs2ka.binary.{DecRun, Decoder}
import avakhrenev.kafs2ka.binary.KafkaProtocol.{CorrId, ResponseFrame}
import cats.effect.{ContextShift, IO, Resource}
import fs2.io.tcp.Socket

import scala.concurrent.duration.FiniteDuration

object Kafka {

  def asynchronousChannelGroup: Resource[IO, AsynchronousChannelGroup] =
    Resource.make(
      IO(
          AsynchronousChannelGroup
          .withThreadPool(Executors.newCachedThreadPool())
      ))(acg => IO(acg.shutdown()))

  def socket(host: String, port: Int)(implicit C: ContextShift[IO]) =
    asynchronousChannelGroup.flatMap(
      implicit ACG =>
        Socket.client[IO](
            new InetSocketAddress(host, port)
      ))

  def readMessage[M](dec: Decoder[M],
                     socket: Socket[IO],
                     readTimeout: Option[FiniteDuration]): IO[Either[String, (CorrId, M)]] = {
    def go(dec: Decoder[ResponseFrame]): IO[Either[String, ResponseFrame]] =
      socket.read(dec.expected, readTimeout).flatMap {
        case None => IO.pure(Left("Socket closed too early"))
        case Some(ch) =>
          dec.decode(ch) match {
            case s @ DecRun.Success(frame, rem) =>
              if (rem.nonEmpty)
                IO.raiseError(new RuntimeException(
                  s"ResponseFrame decoder is wrong: too much bytes consumed: excess: ${rem.size}, corrId: ${frame.correlationId}"))
              else IO.pure(Right(frame))
            case s @ DecRun.Failure(msg)     => IO(Left(msg))
            case dec: Decoder[ResponseFrame] => go(dec)
          }
      }
    go(ResponseFrame.codec.dec).map(_.flatMap(fr =>
      dec.decode(fr.payload.toChunk) match {
        case DecRun.Success(a, remainder) =>
          if (remainder.isEmpty) Right(fr.correlationId -> a)
          else Left(s"Reminder size is ${remainder.size}: not all bytes consumed")
        case DecRun.Failure(msg)  => Left(msg)
        case Decoder(expected, _) => Left(s"Not enough bytes: expected at last $expected more")
    }))
  }

}
