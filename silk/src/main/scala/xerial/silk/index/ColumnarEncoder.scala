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

/**
 * @author Taro L. Saito
 */
class ColumnarEncoder extends Logger {

  private var writer = Seq.newBuilder[CompressedFieldWriter]

  private val encoder = new StructureEncoder(new FieldWriterFactory {
    def newWriter(name: String, tpe: ObjectType) = {
      val w = new CompressedFieldWriter(name, tpe)
      writer += w
      w
    }
  })

  def encode[A:TypeTag](obj:A) {
    encoder.encode(obj)
  }


  def compress = {
    debug("apply compression")
    for(w <- writer.result) yield {
      w.columnBlock
    }
  }



}