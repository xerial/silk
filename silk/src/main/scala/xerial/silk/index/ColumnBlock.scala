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
import java.io.{DataOutputStream, File, ByteArrayOutputStream, ByteArrayInputStream}
import xerial.lens.StandardType
import xerial.core.log.Logger
import java.util.Date


class CompressedFieldWriter(name:String, tpe: ObjectType) extends FieldWriter with Logger {

  private val indexCompressor = new ColumnCompressor.IntCompressor
  private val compressor = ColumnBlock.compressorOf(tpe)

  private var first: Option[OrdPath] = None
  private var prev: OrdPath = null

  def columnBlock = {
    val index = indexCompressor.compress
    val data = compressor.compress
    ColumnBlock(name, tpe, first.get, indexCompressor.byteSize + compressor.byteSize, index, data)
  }


  private def writeIndex[U](index: OrdPath){
    if (first == None) {
      first = Some(index)
      prev = index
    }
    require(prev != null)

    val stepDiffs = index.stepDiffs(prev)
    for(s <- stepDiffs) {
      indexCompressor.add(s.level)
      indexCompressor.add(s.step)
    }
    prev = index
  }


  def write(index: OrdPath, value: Any) {
    writeIndex(index)
    compressor.addAny(value)
  }

  def writeBoolean(index: OrdPath, value: Boolean) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeByte(index: OrdPath, value: Byte) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeChar(index: OrdPath, value: Char) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeShort(index: OrdPath, value: Short) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeInt(index: OrdPath, value: Int) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeFloat(index: OrdPath, value: Float) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeLong(index: OrdPath, value: Long) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeDouble(index: OrdPath, value: Double) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeString(index: OrdPath, value: String) {
    writeIndex(index)
    compressor.add(value)
  }
  def writeFile(index: OrdPath, value: File) {
    writeIndex(index)
    compressor.addAny(value)
  }
  def writeDate(index: OrdPath, value: Date) {
    writeIndex(index)
    compressor.add(value)
  }

}


/**
 * Page block of a columnar storage. All elements in a ColumnBlock must have the same type.
 *
 *
 *
 * @author Taro L. Saito
 */
case class ColumnBlock(path: String, tpe: ObjectType, offset: OrdPath, uncompressedSize:Long, compressedIndex:Array[Byte], compressedData:Array[Byte]) {

  def byteLength = {
    var size = compressedIndex.length.toLong
    size += compressedData.length
    size
  }
}


object ColumnBlock extends Logger {

  import ColumnCompressor._

  def compressorOf(tpe: ObjectType): ColumnCompressor = {
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
      case StandardType(c) if c == classOf[ObjectType] => new ObjectTypeCompressor
      case _ =>
        throw new IllegalArgumentException(f"no compressor is found for object type:$tpe")
    }
  }

}



trait ColumnCompressor {

  private def invalidInput: Unit = throw new IllegalArgumentException("invalid input")

  def add(value: Boolean): Unit = invalidInput
  def add(value: Byte): Unit = invalidInput
  def add(value: Char): Unit = invalidInput
  def add(value: Short): Unit = invalidInput
  def add(value: Int): Unit = invalidInput
  def add(value: Float): Unit = invalidInput
  def add(value: Double): Unit = invalidInput
  def add(value: Long): Unit = invalidInput
  def add(value: String): Unit = invalidInput
  def add(value: java.util.Date): Unit = invalidInput
  def add(value: File): Unit = invalidInput
  def addAny(value: Any): Unit = invalidInput

  def compress: Array[Byte]

  def byteSize : Long
}

trait VarLenColumnCompressor extends ColumnCompressor {


}

trait FixedWidthColumnCompressor extends ColumnCompressor {

}


object ColumnCompressor {

  class ObjectTypeCompressor extends ColumnCompressor {

    private val builder = Array.newBuilder[Int]

    private val index = collection.mutable.Map[ObjectType, Int]()
    private var count = 0
    private var size = 0L

    override def addAny(value: Any) {
      val tpe = value.asInstanceOf[ObjectType]
      val id = index.getOrElseUpdate(tpe, { count += 1; count })
      builder += id
      size += 4
    }



    def compress = {
      val b = new ByteArrayOutputStream()
      // TODO compress object table


      // compress type ids
      b.write(Snappy.compress(builder.result))

      b.close
      b.toByteArray
    }
    def byteSize = size
  }


  class StringCompressor extends VarLenColumnCompressor {

    private val indexBuilder = Array.newBuilder[Int]
    private val builder = new ByteArrayOutputStream()
    private val utf8 = Charset.forName("UTF-8")
    private var size = 0L
    def byteSize = size

    override def add(value: String) {
      val b = value.getBytes(utf8)
      indexBuilder += b.length
      builder.write(b)

      size += 4
      size += b.length
    }


    def compress: Array[Byte] = {
      val b = new ByteArrayOutputStream
      val d = new DataOutputStream(b)
      val index = indexBuilder.result
      d.writeInt(index.length)
      d.write(Snappy.compress(index))
      d.write(Snappy.compress(builder.toByteArray))
      d.close
      b.toByteArray
    }

    def cast(value: Any) = value.asInstanceOf[String]
  }


  class DateCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Long]
    private var size = 0L
    def byteSize = size

    override def add(value: java.util.Date) {
      builder += value.getTime
      size += 8
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }

    def cast(value: Any) = value.asInstanceOf[java.util.Date]

  }

  class ByteCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Byte]
    private var size = 0L
    def byteSize = size

    override def add(value: Byte) {
      builder += value
      size += 1
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }

    def cast(value: Any) = value.asInstanceOf[Byte]

  }

  class ShortCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Short]
    private var size = 0L
    def byteSize = size

    override def add(value: Short) {
      builder += value
      size += 2
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }

    def cast(value: Any) = value.asInstanceOf[Short]

  }

  class CharCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Char]
    private var size = 0L
    def byteSize = size

    override def add(value: Char) {
      builder += value
      size += 2
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }

    def cast(value: Any) = value.asInstanceOf[Char]

  }


  class BooleanCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Byte]
    private var size = 0L
    def byteSize = size

    override def add(value: Boolean) {
      builder += (if (value) 1 else 0)
      size += 1
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
    def cast(value: Any) = value.asInstanceOf[Boolean]

  }


  class IntCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Int]
    private var size = 0L
    def byteSize = size

    override def add(value: Int) {
      builder += value
      size += 4
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
    def cast(value: Any) = value.asInstanceOf[Int]

  }

  class FloatCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Float]
    private var size = 0L
    def byteSize = size

    override def add(value: Float) {
      builder += value
      size += 4
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }

    def cast(value: Any) = value.asInstanceOf[Float]

  }

  class LongCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Long]
    private var size = 0L
    def byteSize = size

    override def add(value: Long) {
      builder += value
      size += 8
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }

    def cast(value: Any) = value.asInstanceOf[Long]

  }

  class DoubleCompressor extends FixedWidthColumnCompressor {
    private val builder = Array.newBuilder[Double]
    private var size = 0L
    def byteSize = size

    override def add(value: Double) {
      builder += value
      size += 8
    }
    def compress = {
      val arr = builder.result
      Snappy.compress(arr)
    }
    def cast(value: Any) = value.asInstanceOf[Double]

  }


}



