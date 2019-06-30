package avakhrenev.kafs2ka

import avakhrenev.kafs2ka.binary.{Codec, Encoder, ~}
import avakhrenev.kafs2ka.binary.KafkaProtocol.{ApiKey, ApiVersionsResponse, CorrId, RequestFrame}
import utest._
import cats.effect._
import fs2.Chunk
import fs2.io.tcp._

object BrokerStartTest extends IOSuite {
  lazy val kafka                     = new RunningKafka(true)
  override def utestAfterAll(): Unit = kafka.close()

  def connect =
    Kafka.asynchronousChannelGroup.flatMap(implicit ACG =>
      Kafka.socket("localhost", kafka.kafkaPort))

  def write[M](key: ApiKey, msg: M, corrId: CorrId = CorrId(42))(implicit C: Encoder[M],
                                                                 S: Socket[IO]): IO[Unit] =
    S.write(RequestFrame.codec.enc.encode(
              RequestFrame(
                  key
                , corrId
                , clientId = None
                , payload = C.encode(msg)
              )).toChunk,
            Some(ioTimeout))

  def read[M](implicit C: Codec[M], S: Socket[IO]) = Kafka.readMessage(C.dec, S, Some(ioTimeout))

  override val tests = this {
    test("ApiVersions0") {
      connect.use { implicit socket =>
        for {
          _    <- write(ApiKey.ApiVersions0, Chunk.empty[Byte], CorrId(123))

          resp <- read[ApiVersionsResponse]
          res  <- IO{
            assert(resp.isRight)
            val (corrId, r) = resp.right.get
            corrId ==> CorrId(123)
            val apiVer = r.versions.find{case key ~ _ ~ _ => key == ApiKey.ApiVersions0.key}
            assert(apiVer.exists{case ((_,  minVer), _) => minVer == ApiKey.ApiVersions0.version})

          }
        } yield res
      }
    }
  }
}
