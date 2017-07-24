package avakhrenev.kafs2ka.binary

import java.io.{ByteArrayOutputStream, DataOutput, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import fs2.{Chunk, Segment}
import kafka.message.{ByteBufferMessageSet, Message => KMessage}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.utils.ByteUtils
import utest._

object SerializeTest extends TestSuite {
  type Record = ProducerRecord[Array[Byte], Array[Byte]]

  implicit def byteArrayToByteBuffer(arr: Array[Byte]): ByteBuffer = ByteBuffer.wrap(arr)
  def utf8(str: String): Array[Byte]                               = str.getBytes(StandardCharsets.UTF_8)

  def record(key: String, value: String, timestamp: Option[Long]): Record =
    new Record("blah", 0, timestamp.map(java.lang.Long.valueOf).orNull, utf8(key), utf8(value))

  def toMessage(record: Record): KMessage = new KMessage(
      record.value()
    , record.key()
    , Option(record.timestamp()).map(_.toLong).getOrElse(KMessage.NoTimestamp)
    , 1.toByte
  )
  def dataOutputSegment(fn: DataOutput => Unit): Segment[Byte, Unit] = {
    val bao = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bao)
    fn(dos)
    dos.flush()
    Chunk.bytes(bao.toByteArray)
  }


  override val tests = this {
    'encode {
      "message batch" - {
        val now = System.currentTimeMillis()
        val records = Seq(
            record("123", "456", Some(now))
          , record("789", "ABC", None)
          , record(null, "DEF", None)
          , record("GHJ", null, None)
        )

        val expected = {
          val buf = new ByteBufferMessageSet(records.map(toMessage): _*).getBuffer
          val arr = new Array[Byte](buf.remaining())
          buf.get(arr)
          Chunk.bytes(arr)
        }
        'uncompressed {
          val expected = {
            val buf = new ByteBufferMessageSet(records.map(toMessage): _*).getBuffer
            val arr = new Array[Byte](buf.remaining())
            buf.get(arr)
            Chunk.bytes(arr)
          }

//          val actual = KafkaLeaderConnection.encodeMessageBatch(records)
//
//          actual ==> expected
        }
      }
    }
    'decode {
      'zigzag {
        val seg = dataOutputSegment{dao => ByteUtils.writeVarlong(123, dao); ByteUtils.writeVarlong(567, dao)}
        def readBytes(segment: Segment[Byte, Unit]): Vector[Long] = {
          def go(seg: Segment[Byte, Unit], acc: Vector[Long]): Segment[Unit, (Vector[Long], (Long, Long))] =
          Primitives.readVarlong(segment).mapResult{
            case Left((_, r)) => acc -> r
            case Right((l, seg)) => go(seg, acc :+ l).run
          }
          go(segment, Vector.empty).run._1
        }
        def splitted(n: Int, seg: Segment[Byte, Unit]): Segment[Byte, Unit] = {
          val s = seg.splitAt(n).right.get
          Segment.catenated(s._1) ++ s._2
        }
        println(seg)
        'o {readBytes(seg) ==> Vector(123L, 567L)}
        'a {readBytes(splitted(1, seg)) ==> Vector(123L, 567L)}
        'b {readBytes(splitted(2, seg)) ==> Vector(123L, 567L)}
        'c {readBytes(splitted(3, seg)) ==> Vector(123L, 567L)}
        'd {readBytes(splitted(4, seg)) ==> Vector(123L, 567L)}
        'e {readBytes(splitted(5, seg)) ==> Vector(123L, 567L)}
      }
    }

  }

}
