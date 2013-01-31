//--------------------------------------
//
// ColumnBlock.scala
// Since: 2013/01/31 3:59 PM
//
//--------------------------------------

package xerial.silk.index

import xerial.lens._
import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe._
import reflect.ClassTag
import org.xerial.snappy.Snappy
import java.nio.charset.Charset
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import xerial.silk.index.VarLenSeq
import xerial.lens.StandardType
import xerial.core.log.Logger


/**
 * Page block of a columnar storage. All elements in a ColumnBlock must have the same type.
 *
 *
 *
 * @author Taro L. Saito
 */
case class ColumnBlock(tpe:ObjectType) {




}


object ColumnBlock extends Logger {

  import ColumnCompressor._

  def compressorOf(tpe:ObjectType) : ColumnCompressor = {
    tpe match {
      case Primitive.Boolean => new BooleanCompressor
      case Primitive.Byte => new ByteCompressor
      case Primitive.Char => new CharCompressor
      case Primitive.Short => new ShortCompressor
      case Primitive.Int => new IntCompressor
      case Primitive.Float => new FloatCompressor
      case Primitive.Double => new DoubleCompressor
      case Primitive.Long => new LongCompressor
      case TextType.String => new StringCompressor
      case TextType.Date => new DateCompressor
      case StandardType(c) if c.getClass == classOf[ObjectType] => new ObjectTypeCompressor
      case _ =>
        throw new IllegalArgumentException(f"no compressor is found for object type:$tpe")
    }
  }

}

case class VarLenSeq(index:Array[Int], compressedData:Array[Byte])



trait ColumnCompressor[@specialized(Byte, Int, Long, Float, Double) A] {
  def add(value:A) : Unit

}

trait VarLenColumnCompressor[A] extends ColumnCompressor[A] {
  def compress : VarLenSeq

}

trait FixedWidthColumnCompressor[@specialized(Byte, Int, Long, Float, Double) A] extends ColumnCompressor[A] {
  def compress : Array[Byte]
}


object ColumnCompressor {

  class ObjectTypeCompressor extends ColumnCompressor[ObjectType] {
    def add(value: ObjectType) {

    }

    def compress = {

    }
  }


  class StringCompressor extends VarLenColumnCompressor[String] {

    private val indexBuilder = Array.newBuilder[Int]
    private val builder = new ByteArrayOutputStream()
    private val utf8 = Charset.forName("UTF-8")

    def add(value: String) {
      val b = value.getBytes(utf8)
      indexBuilder += b.length
      builder.write(b)
    }

    def compress : VarLenSeq = {
      VarLenSeq(indexBuilder.result, Snappy.compress(builder.toByteArray))
    }
  }


  class DateCompressor extends FixedWidthColumnCompressor[java.util.Date] {
    private val builder = Array.newBuilder[Long]

    def add(value:java.util.Date) {
      builder += value.getTime
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }

  class ByteCompressor extends FixedWidthColumnCompressor[Byte] {
    private val builder = Array.newBuilder[Byte]

    def add(value:Byte) {
      builder += value
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }

  class ShortCompressor extends FixedWidthColumnCompressor[Short] {
    private val builder = Array.newBuilder[Short]

    def add(value:Short) {
      builder += value
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }

  class CharCompressor extends FixedWidthColumnCompressor[Char] {
    private val builder = Array.newBuilder[Char]

    def add(value:Char) {
      builder += value
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }


  class BooleanCompressor extends FixedWidthColumnCompressor[Boolean] {
    private val builder = Array.newBuilder[Byte]

    def add(value:Boolean) {
      builder += (if(value) 1 else 0)
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }


  class IntCompressor extends FixedWidthColumnCompressor[Int] {
    private val builder = Array.newBuilder[Int]

    def add(value:Int) {
      builder += value
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }

  class FloatCompressor extends FixedWidthColumnCompressor[Float] {
    private val builder = Array.newBuilder[Float]

    def add(value:Float) {
      builder += value
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }

  class LongCompressor extends FixedWidthColumnCompressor[Long] {
    private val builder = Array.newBuilder[Long]

    def add(value:Long) {
      builder += value
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }

  class DoubleCompressor extends FixedWidthColumnCompressor[Double] {
    private val builder = Array.newBuilder[Double]

    def add(value:Double) {
      builder += value
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
  }



}



