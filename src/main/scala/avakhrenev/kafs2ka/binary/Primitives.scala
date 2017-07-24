package avakhrenev.kafs2ka.binary

import fs2.Segment

object Primitives {

  /**
    * Read a long stored in variable-length format using zig-zag decoding from
    * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
    *
    * @param data The data segment to read from
    * @param result Intermediate state.
    * @return Result
    * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
    */
  def readVarlong[R](data: Segment[Byte, R], result: (Long, Long) = (0L, 0L)): Segment[Unit, Either[(R, (Long, Long)), (Long, Segment[Byte, R])]] = {
    var (value, i) = result
    data.takeWhile(b => (b & 0x80) != 0, takeFailure = true).map{b =>
      val bb = if((b & 0x80) != 0) (b & 0x7f) << i else b << i //signum
      value = value | bb
      i += 7
      if (i > 63) throw new IllegalArgumentException("Illegal varlog")
    }.mapResult{
      case Left(r) => Left(r -> (value -> i))
      case Right(s) =>
        (value >>> 1) ^ -(value & 1)
        Right(value -> s)
    }
  }

  def readVarint[R](data: Segment[Byte, R], result: (Int, Long)): Segment[Unit, Either[(R, (Int, Long)), (Int, Segment[Byte, R])]] = {
    var (value, i) = result
    data.takeWhile(b => (b & 0x80) != 0, takeFailure = true).map{b =>
      val bb = if((b & 0x80) != 0) (b & 0x7f) << i else b << i //signum
      value = value | bb
      i += 7
      if (i > 28) throw new IllegalArgumentException("Illegal varint")
    }.mapResult{
      case Left(r) => Left(r -> (value -> i))
      case Right(s) =>
        (value >>> 1) ^ -(value & 1)
        Right(value -> s)
    }
  }
/*
  MessageSet =>
FirstOffset => int64
Length => int32
CRC => int32
Magic => int8  /* bump up to “2” */
Attributes => int16
LastOffsetDelta => int32
FirstTimestamp => int64
MaxTimestamp => int64
PID => int64
Epoch => int16
FirstSequence => int32
Messages => Message1, Message2, … , MessageN

Message =>
Length => varint
Attributes => int8
TimestampDelta => varint
OffsetDelta => varint
KeyLen => varint
Key => data
ValueLen => varint
Value => data

   */

}
