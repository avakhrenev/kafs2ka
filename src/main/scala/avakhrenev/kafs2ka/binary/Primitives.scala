package avakhrenev.kafs2ka.binary

import java.nio.ByteBuffer

import fs2._

object Primitives {
  //quick link:  https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
  /** Eagerly converts segment to a byte buffer */
  def toByteBuffer(seg: Segment[Byte, Unit]): ByteBuffer =
    seg.toChunks.uncons match {
      case None => ByteBuffer.allocate(0)
      case Some((hd, tl)) =>
        tl.uncons match {
          case Some(_) => ByteBuffer.wrap(seg.toArray)
          case None =>
            val b = hd.toBytes
            ByteBuffer.wrap(b.values, b.offset, b.length)
        }
    }

  /** Reads length-delimited kafka responses from the stream and outputs them as pairs of correlationId with body*/
  //response = length correlationId response
  def readKafkaResponses[F[_]]: Pipe[F, Byte, (Int, ByteBuffer)] =
    _.repeatPull[(Int, ByteBuffer)](_.unconsN(4, allowFewer = false).flatMap {
      case None => Pull.pure(None)
      case Some((h, tl)) =>
        val length = toByteBuffer(h).getInt()
        tl.pull.unconsN(length, allowFewer = false).flatMap {
          case None => Pull.fail(new RuntimeException("Can't read response fully"))
          case Some((h, tl)) =>
            val buf    = toByteBuffer(h)
            val corrId = buf.getInt()
            Pull.output1(corrId -> buf).as(Some(tl))
        }
    })
}
