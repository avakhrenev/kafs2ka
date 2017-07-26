package avakhrenev.kafs2ka.binary

import java.io.{ByteArrayOutputStream, DataOutput, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import fs2._
import kafka.message.{ByteBufferMessageSet, Message => KMessage}
import org.apache.kafka.clients.producer.ProducerRecord
import utest._

object SerializeTest extends TestSuite {
  type Record = ProducerRecord[Array[Byte], Array[Byte]]

  implicit def byteArrayToByteBuffer(arr: Array[Byte]): ByteBuffer = ByteBuffer.wrap(arr)
  def utf8(str: String): Array[Byte] =
    if (str ne null) str.getBytes(StandardCharsets.UTF_8) else null

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
      "length delimited" - {
        def write(corrId: Int, str: String): Chunk[Byte] = {
          val bb      = new ByteArrayOutputStream()
          val out     = new DataOutputStream(bb)
          val content = str.getBytes(StandardCharsets.UTF_8)
          out.writeInt(4 + content.length)
          out.writeInt(corrId)
          out.write(content)
          out.flush()
          Chunk.bytes(bb.toByteArray)
        }

        def readByteByffer(bb: ByteBuffer): String = {
          val bytes = new Array[Byte](bb.remaining())
          bb.get(bytes)
          new String(bytes, StandardCharsets.UTF_8)
        }

        def stream(chSize: Int) =
          Stream(write(1, "abc"), write(15, "def"), write(30, "ABCDEF"))
            .flatMap(Stream.chunk)
            .segmentN(chSize, allowFewer = true)
            .flatMap(s => Stream.chunk(s.toChunk))
        def result(s: Stream[Pure, Byte]): Vector[(Int, String)] =
          s.through(Primitives.readKafkaResponses[Pure])
            .map { case (a, b) => a -> readByteByffer(b) }
            .toVector

        def check(x: Int) = {
          val expected = Vector(1 -> "abc", 15 -> "def", 30 -> "ABCDEF")
          val actual   = result(stream(x))
          actual ==> expected
        }
        'chunk100 (check(100))
        'chunk10 (check(10))
        'chunk5 (check(5))
        'chunk3 (check(3))
        'chunk2 (check(2))
        'chunk1 (check(1))
      }
    }

  }

}
