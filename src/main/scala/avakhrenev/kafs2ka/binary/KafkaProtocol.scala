package avakhrenev.kafs2ka.binary

import fs2.Chunk
import io.estatico.newtype.macros.newsubtype

object KafkaProtocol {
  import io.estatico.newtype.ops._

  @newsubtype final case class CorrId(id: Int)
  object CorrId {
    val codec: Codec[CorrId] = Codec.int32.coerce[Codec[CorrId]]
  }
  sealed abstract class ApiKey(val key: Int)
  object ApiKey {
    object ApiVersions extends ApiKey(key = 18)

    val codec: Codec[ApiKey] = Codec.int16.emap[ApiKey] {
      case ApiVersions.key => Right(ApiVersions)
    }(_.key)

  }

  /*

  RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32

  Request Header => api_key api_version correlation_id client_id
  api_key => INT16
  api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING

  Response Header => correlation_id
  correlation_id => INT32

   */
  // ToDo relies on lengthDelimited codec to assembly full Chunk
  private[KafkaProtocol] val payload: Codec[Enc] =
    Codec[Enc](Decoder(Int.MaxValue, c => DecRun.pure(Enc.chunk(c))), Encoder(identity))

  final case class RequestFrame(key: ApiKey,
                                version: Int,
                                correlationId: CorrId,
                                clientId: Option[String],
                                payload: Enc)
  object RequestFrame {

    import Codec._
    /*
    Request => Size RequestMessage
      Size => int32
      RequestMessage => RequestHeader Payload
        RequestHeader => api_key api_version correlation_id client_id
          api_key => INT16
          api_version => INT16
          correlation_id => INT32
          client_id => NULLABLE_STRING
        Payload => actual request
     */
    val codec: Codec[RequestFrame] = lengthDelimited[RequestFrame](
        int32
      , (ApiKey.codec ~ int16 ~ CorrId.codec ~ stringOpt ~ payload).xmap {
        case key ~ version ~ corrId ~ clientId ~ payload =>
          RequestFrame(key, version, corrId, clientId, payload)
      }(k => k.key -> k.version -> k.correlationId -> k.clientId -> k.payload)
    )
  }
  final case class ResponseFrame(correlationId: CorrId, payload: Enc)
  object ResponseFrame {
    import Codec._
    val codec: Codec[ResponseFrame] = lengthDelimited[ResponseFrame](
        int32
      , (CorrId.codec ~ payload).xmap {
        case corrId ~ payload =>
          ResponseFrame(corrId, payload)
      }(k => k.correlationId -> k.payload)
    )
  }

  final case class ApiVersionsResponse(errorCode: Int, versions: Chunk[((Int, Int), Int)])
  object ApiVersionsResponse {
    import Codec._
    /*
    ApiVersions Response (Version: 0) => error_code [api_versions]
  error_code => INT16
  api_versions => api_key min_version max_version
    api_key => INT16
    min_version => INT16
    max_version => INT16

     */
    val codec: Codec[ApiVersionsResponse] = (int16 ~ arrayOf(int16 ~ int16 ~ int16)).xmap {
      case error ~ versions => ApiVersionsResponse(error, versions)
    }(r => r.errorCode -> r.versions)
  }

}
