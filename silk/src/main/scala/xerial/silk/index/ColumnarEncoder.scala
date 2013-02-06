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



sealed trait EncoderType
case object ReflectionEncoder extends EncoderType
case object JavassistEncoder extends EncoderType


/**
 * @author Taro L. Saito
 */
class ColumnarEncoder(encoderType:EncoderType = ReflectionEncoder) extends Logger {

  private var writer = Seq.newBuilder[CompressedFieldWriter]

  private val encoder = new StructureEncoder(new FieldWriterFactory {
    def newWriter(name: String, tpe: ObjectType) = {
      val w = new CompressedFieldWriter(name, tpe)
      writer += w
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


  def compress : Seq[ColumnBlock] = {
    trace("apply compression")
    for(w <- writer.result) yield {
      w.columnBlock
    }
  }



}