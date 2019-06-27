package avakhrenev.kafs2ka

import java.nio.charset.StandardCharsets

import avakhrenev.kafs2ka.binary.Decoder.{int32, takeElements}
import cats.data.Chain

import scala.{specialized => sp}
import fs2.Chunk
import io.estatico.newtype.macros.newtype

import scala.annotation.tailrec
import scala.collection.mutable

package object binary {
  // Decoding run: current state of the parser
  sealed trait DecRun[@sp(Int, Short) +A] extends Product with Serializable {
    def map[B](f: A => B): DecRun[B]
    def emap[B](f: A => Either[String, B]): DecRun[B]
    def flatMap[B](f: A => DecRun[B]): DecRun[B]
  }

  object DecRun {
    final case class Success[@sp(Int, Short) +A](a: A, remainder: Chunk[Byte]) extends DecRun[A] {
      override def map[@sp(Int, Short) B](f: A => B): DecRun[B] = Success(f(a), remainder)
      override def emap[@sp(Int, Short) B](f: A => Either[String, B]): DecRun[B] =
        f(a).fold(failure, success(_, remainder))
      override def flatMap[@sp(Int, Short) B](f: A => DecRun[B]): DecRun[B] =
        f(a) match { //ToDo isn't stack-safe
          case s: Success[B] =>
            s.copy(remainder = Chunk.concatBytes(remainder :: s.remainder :: Nil))
          case i: Decoder[B] => i.decode(remainder)
          case e: Failure    => e
        }
    }
    final case class Failure(msg: String) extends DecRun[Nothing] {
      override def map[B](f: Nothing => B): DecRun[B]                  = this
      override def emap[B](f: Nothing => Either[String, B]): DecRun[B] = this
      override def flatMap[B](f: Nothing => DecRun[B]): DecRun[B]      = this
    }
    def success[@sp(Int, Short) A](a: A, remainder: Chunk[Byte]): DecRun[A] = Success(a, remainder)
    def pure[@sp(Int, Short) A](a: A): DecRun[A]                            = Success(a, Chunk.empty)
    def inProgress[@sp(Int, Short) A](expected: Int)(next: Chunk[Byte] => DecRun[A]): Decoder[A] =
      Decoder(expected, next)
    def failure(msg: String): DecRun[Nothing] = Failure(msg)

  }

  // Decoder is a binary decoder: given a byte chunk, returns current parsing state.
  // The `expected` parameters tells how much bytes left to the next state transition.
  // Implementation relies on  constant time slice on Chunk.
  final case class Decoder[@sp(Int, Short) +A](expected: Int, decode: Chunk[Byte] => DecRun[A])
      extends DecRun[A] {
    override def map[@sp(Int, Short) B](f: A => B): Decoder[B] =
      copy(decode = decode.andThen(_.map(f)))
    override def emap[@sp(Int, Short) B](f: A => Either[String, B]): Decoder[B] =
      copy(decode = decode.andThen(_.emap(f)))
    override def flatMap[@sp(Int, Short) B](f: A => DecRun[B]): Decoder[B] =
      copy(decode = decode.andThen(_.flatMap(f)))
    def ~[@sp(Int, Short) B](b: Decoder[B]): Decoder[(A, B)] = flatMap(a => b.map(b => a -> b))
  }
  object Decoder {
    import DecRun._
    def takeBytes(length: Int): Decoder[Chunk[Byte]] = {

      def go(x: Int, acc: List[Chunk[Byte]]): Decoder[Chunk[Byte]] = inProgress(x) {
        case c if c.size >= x =>
          val (value, remainder) = c.splitAt(x)
          success(Chunk.concatBytes((value :: acc).reverse), remainder)
        case c if c.isEmpty  => go(x, acc)
        case c if c.size < x => go(x - c.size, c :: acc)
      }
      go(length, Nil)
    }

    def lengthDelimited(length: Decoder[Int]): Decoder[Chunk[Byte]] =
      length.flatMap(length => takeBytes(length))
    def lengthDelimitedOpt(length: Decoder[Int]): Decoder[Option[Chunk[Byte]]] = length.flatMap {
      case -1     => pure(None)
      case length => takeBytes(length).map(Some.apply)
    }
    /*
    Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure.
    First, the length N is given as an INT32. Then N instances of type T follow. A null array is represented with a length of -1.
    In protocol documentation an array of T instances is referred to as [T].
     */

    def takeElements[A](length: Int, decoder: Decoder[A]): Decoder[Chunk[A]] =
      inProgress(decoder.expected) { ch =>
        val acc = mutable.Buffer.newBuilder[A]

        @tailrec def goTr(x: Int, rem: Chunk[Byte]): DecRun[Chunk[A]] =
          if (x == 0) {
            DecRun.success(Chunk.buffer(acc.result()), rem)
          } else {
            decoder.decode(rem) match {
              case f: DecRun.Failure => f
              case dec: Decoder[A]   => inProgress[Chunk[A]](dec.expected)(ch => go(x, dec, ch))
              case DecRun.Success(a, rem) =>
                acc += a
                goTr(x - 1, rem)
            }
          }

        def go(x: Int, dec: Decoder[A], rem: Chunk[Byte]): DecRun[Chunk[A]] =
          if (x == 0) {
            DecRun.success(Chunk.buffer(acc.result()), rem)
          } else {
            dec.decode(rem) match {
              case f: DecRun.Failure => f
              case dec: Decoder[A]   => inProgress[Chunk[A]](dec.expected)(ch => go(x, dec, ch))
              case DecRun.Success(a, rem) =>
                acc += a
                goTr(x - 1, rem)
            }
          }

        inProgress(decoder.expected) { ch =>
          go(length, decoder, ch)
        }
      }

    val int16: Decoder[Int] = takeBytes(16).map(c => c(0) << 8 | (c(1) & 0xFF))
    val int32: Decoder[Int] =
      takeBytes(32).map(c => c(0) << 24 | (c(1) & 0xFF) << 16 | (c(2) & 0xFF) << 8 | (c(3) & 0xFF))
    val string: Decoder[String] =
      lengthDelimited(int16).map(c => new String(c.toArray, StandardCharsets.UTF_8))
    val stringOpt: Decoder[Option[String]] =
      lengthDelimitedOpt(int16).map(_.map(c => new String(c.toArray, StandardCharsets.UTF_8)))
    // Kafka array are nullable. We represent them as empty chunks
    def arrayOf[A](decoder: Decoder[A]): Decoder[Chunk[A]] = int32.flatMap {
      case -1     => pure(Chunk.empty[A])
      case length => takeElements(length, decoder)
    }
  }

  // Sequence of bytes with constant time append, prepend and size.
  final case class Enc(data: Chain[Chunk[Byte]], size: Int) {
    def prepend(c: Chunk[Byte]): Enc = Enc(data.prepend(c), size + c.size)
    def append(c: Chunk[Byte]): Enc  = Enc(data.append(c), size + c.size)
    def ~(c: Enc): Enc               = Enc(data ++ c.data, size + c.size)
    def isEmpty: Boolean             = size == 0

    def toChunk: Chunk[Byte] =
      if (isEmpty) Chunk.empty
      else {
        val chunks = data.iterator.filter(_.nonEmpty).to[mutable.Buffer]
        if (chunks.size == 1) chunks.head
        else {
          val arr    = new Array[Byte](size)
          var offset = 0
          chunks.foldLeft(()) { (_, c) =>
            if (!c.isEmpty) {
              c.copyToArray(arr, offset)
              offset += c.size
            }
          }
          Chunk.bytes(arr)
        }
      }
  }
  object Enc {
    val empty                      = Enc(Chain.empty, 0)
    def chunk(c: Chunk[Byte]): Enc = Enc(Chain.one(c), c.size)
  }

  // Encoder is a binary encoder.
  @newtype final case class Encoder[-A](encode: A => Enc) {
    def contramap[@sp(Int, Short) B](f: B => A): Encoder[B] = Encoder(encode compose f)
    def ~[@sp(Int, Short) B](b: Encoder[B]): Encoder[(A, B)] = Encoder {
      case (x, y) => encode(x) ~ b.encode(y)
    }
  }
  object Encoder {
    val int16: Encoder[Int] = Encoder[Int](
      value =>
        Enc.chunk(
          Chunk.bytes(
            Array(
                (value >> 8).toByte
              , value.toByte
            ))))

    val int32: Encoder[Int] = Encoder[Int](
      value =>
        Enc.chunk(
          Chunk.bytes(
            Array(
                (value >> 24).toByte
              , (value >> 16).toByte
              , (value >> 8).toByte
              , value.toByte
            ))))

    def lengthDelimited(length: Encoder[Int]): Encoder[Enc] = Encoder[Enc] { ch =>
      length.encode(ch.size) ~ ch
    }
    def lengthDelimitedOpt(length: Encoder[Int]): Encoder[Option[Enc]] =
      Encoder[Option[Enc]] {
        case Some(ch) => length.encode(ch.size) ~ ch
        case None     => length.encode(-1)
      }

    val string: Encoder[String] = lengthDelimited(int16).contramap[String] { s =>
      Enc.chunk(Chunk.bytes(s.getBytes(StandardCharsets.UTF_8)))
    }
    val stringOpt: Encoder[Option[String]] =
      lengthDelimitedOpt(int16).contramap[Option[String]](_.map(s =>
        Enc.chunk(Chunk.bytes(s.getBytes(StandardCharsets.UTF_8)))))
    def arrayOf[A](encoder: Encoder[A]): Encoder[Chunk[A]] =
      Encoder[Chunk[A]](ch =>
        int32.encode(ch.size) ~ ch.foldLeft(Enc.empty)((a, b) => a ~ encoder.encode(b)))
  }

  trait Codec[@sp(Int, Short) A] {
    def enc: Encoder[A]
    def dec: Decoder[A]
    def ~[@sp(Int, Short) B](b: Codec[B]): Codec[(A, B)] = Codec(dec ~ b.dec, enc ~ b.enc)
    def xmap[@sp(Int, Short) B](map: A => B)(contramap: B => A): Codec[B] = Codec(
        dec.map(map)
      , enc.contramap(contramap)
    )
    def emap[@sp(Int, Short) B](emap: A => Either[String, B])(contramap: B => A): Codec[B] = Codec(
        dec.emap(emap)
      , enc.contramap(contramap)
    )

  }

  object Codec {
    def apply[@sp(Int, Short) A](decode: Decoder[A], encode: Encoder[A]): Codec[A] = new Codec[A] {
      override def enc: Encoder[A] = encode
      override def dec: Decoder[A] = decode
    }

    // ToDo make sure we consume all the input
    def lengthDelimited[A](length: Codec[Int], codec: Codec[A]): Codec[A] = {
      val encA = codec.enc
      val decA = codec.dec
      Codec[A](Decoder.lengthDelimited(length.dec).flatMap(ch => decA.decode(ch)),
               Encoder.lengthDelimited(length.enc).contramap[A](encA.encode))
    }

    val int16: Codec[Int]                = Codec(Decoder.int16, Encoder.int16)
    val int32: Codec[Int]                = Codec(Decoder.int32, Encoder.int32)
    val string: Codec[String]            = Codec(Decoder.string, Encoder.string)
    val stringOpt: Codec[Option[String]] = Codec(Decoder.stringOpt, Encoder.stringOpt)
    def arrayOf[A](codec: Codec[A]): Codec[Chunk[A]] =
      Codec(Decoder.arrayOf(codec.dec), Encoder.arrayOf(codec.enc))
  }

  object ~ {
    @inline def unapply[A, B](arg: (A, B)): Some[(A, B)] = Some(arg)
  }

}
