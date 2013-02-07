//--------------------------------------
//
// ColumnarEncoder.scala
// Since: 2013/02/05 4:32 PM
//
//--------------------------------------

package xerial.silk.index

import xerial.lens.ObjectType
import xerial.core.log.Logger
import scala.reflect.runtime.universe._
import collection.GenSeq


sealed trait EncoderType
case object ReflectionEncoder extends EncoderType
case object JavassistEncoder extends EncoderType


/**
 * @author Taro L. Saito
 */
class ColumnarEncoder(encoderType:EncoderType = ReflectionEncoder) extends Logger {

  private var writer = List.empty[CompressedFieldWriter]

  private val encoder = new StructureEncoder(new FieldWriterFactory {
    def newWriter(name: String, tpe: ObjectType) = {
      val w = new CompressedFieldWriter(name, tpe)
      writer = w :: writer
      w
    }},
    encoder = encoderType match {
      case ReflectionEncoder => new FieldEncoderWithReflection
      case JavassistEncoder => new FieldEncoderWithJavassist
    }
  )

  def encode[A:TypeTag](obj:A) {
    encoder.encode(obj)
  }


  def compress : GenSeq[ColumnBlock] = {
    trace("apply compression")
    for(w <- writer) yield {
      w.columnBlock
    }
  }





}