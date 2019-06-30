package avakhrenev.kafs2ka.binary

import fs2.Chunk
import io.estatico.newtype.macros.newsubtype

object KafkaProtocol {
  import io.estatico.newtype.ops._

  @newsubtype final case class CorrId(id: Int)
  object CorrId {
    val codec: Codec[CorrId] = Codec.int32.coerce[Codec[CorrId]]
  }
  sealed abstract class ApiKey(val key: Int, val version: Int)
  object ApiKey {
    import Codec._
    object ApiVersions0 extends ApiKey(key = 18, 0)
    object Fetch6       extends ApiKey(key = 1, 6)

    val codec: Codec[ApiKey] = (int16 ~ int16).emap[ApiKey] {
      case ApiVersions0.key ~ ApiVersions0.version => Right(ApiVersions0)
      case key ~ version                           => Left(s"Unknown Api key: $key, ver: $version")
    }(a => a.key -> a.version)

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
      , (ApiKey.codec ~ CorrId.codec ~ stringOpt ~ payload).xmap {
        case key ~ corrId ~ clientId ~ payload =>
          RequestFrame(key, corrId, clientId, payload)
      }(k => k.key -> k.correlationId -> k.clientId -> k.payload)
    )
  }
  final case class ResponseFrame(correlationId: CorrId, payload: Enc)
  object ResponseFrame {
    import Codec._
    implicit val codec: Codec[ResponseFrame] = lengthDelimited[ResponseFrame](
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
    implicit val codec: Codec[ApiVersionsResponse] = (int16 ~ arrayOf(int16 ~ int16 ~ int16)).xmap {
      case error ~ versions => ApiVersionsResponse(error, versions)
    }(r => r.errorCode -> r.versions)
  }
  /*
  Fetch Request (Version: 6) => replica_id max_wait_time min_bytes max_bytes isolation_level [topics]
  replica_id => INT32
  max_wait_time => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  topics => topic [partitions]
    topic => STRING
    partitions => partition fetch_offset log_start_offset partition_max_bytes
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
   */
  final case class FetchReq6(replicaId: Int,
                             maxWaitTime: DurationMs,
                             minBytes: Int,
                             maxBytes: Int,
                             isolationLevel: FetchReq6.IsolationLevel,
                             topics: Chunk[FetchReq6.Topic])
  object FetchReq6 {
    import Codec._
    implicit val codec: Codec[FetchReq6] =
      (int32 ~ of[DurationMs] ~ int32 ~ int32 ~ of[FetchReq6.IsolationLevel] ~ arrayOf[
        FetchReq6.Topic]).xmap {
        case replicaId ~ maxWaitTime ~ minBytes ~ maxBytes ~ isolationLevel ~ topics =>
          FetchReq6(replicaId, maxWaitTime: DurationMs, minBytes, maxBytes, isolationLevel, topics)
      }(r =>
        r.replicaId -> r.maxWaitTime -> r.minBytes -> r.maxBytes -> r.isolationLevel -> r.topics)

    sealed trait IsolationLevel extends Product with Serializable
    object IsolationLevel {
      final case object ReadCommitted   extends IsolationLevel
      final case object ReadUncommitted extends IsolationLevel

      implicit val codec: Codec[IsolationLevel] = int8.emap {
        case 0 => Right(ReadUncommitted)
        case 1 => Right(ReadCommitted)
        case x => Left(s"Unknown isolation level $x")
      } {
        case ReadUncommitted => 0
        case ReadCommitted   => 1
      }
    }

    final case class Topic(topic: String, partitions: Chunk[Partition])
    object Topic {
      implicit val codec: Codec[Topic] = (string ~ arrayOf[Partition]).xmap {
        case (topic, ps) => Topic(topic, ps)
      }(t => t.topic -> t.partitions)

    }

    final case class Partition(partition: Int,
                               fetchOffset: Long,
                               logStartOffset: Long,
                               partitionMaxBytes: Int)
    object Partition {
      implicit val codec: Codec[Partition] = (int32 ~ int64 ~ int64 ~ int32).xmap {
        case partition ~ fetchOffset ~ logStartOffset ~ partitionMaxBytes =>
          Partition(partition, fetchOffset, logStartOffset, partitionMaxBytes)
      }(p => p.partition -> p.fetchOffset -> p.logStartOffset -> p.partitionMaxBytes)

    }

  }
  /*
  Fetch Response (Version: 6) => throttle_time_ms [responses]
  throttle_time_ms => INT32
  responses => topic [partition_responses]
    topic => STRING
    partition_responses => partition_header record_set
      partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions]
        partition => INT32
        error_code => INT16
        high_watermark => INT64
        last_stable_offset => INT64
        log_start_offset => INT64
        aborted_transactions => producer_id first_offset
          producer_id => INT64
          first_offset => INT64
      record_set => RECORDS
   */
  final case class FetchResp6()
  object FetchResp6 {}

}
