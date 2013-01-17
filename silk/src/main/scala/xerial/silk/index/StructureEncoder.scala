//--------------------------------------
//
// StructureEncoder.scala
// Since: 2013/01/17 10:36 AM
//
//--------------------------------------

package xerial.silk.index

import xerial.lens._
import xerial.core.log.Logger


trait FieldWriter {
  def write(index: OrdPath, value: Any): Unit
}

class SimpleFieldWriter(name: String) extends FieldWriter with Logger {
  private var prev : Option[OrdPath] = None
  def write(index: OrdPath, value: Any) {
    val diff = prev.map(index - _) getOrElse (OrdPath.zero)
    debug("write %-10s (%10s) %-10s : %s", name, index, diff, value)
    prev = Some(index)
  }
}

case class ParamKey(name: String, valueType: ObjectType)

/**
 *
 *
 * @author Taro L. Saito
 */
class StructureEncoder extends Logger {

  import TypeUtil._

  private val objTypeWriterTable = collection.mutable.Map[Int, FieldWriter]()
  private val writerTable = collection.mutable.Map[ParamKey, FieldWriter]()

  def objTypeWriter(level:Int) : FieldWriter = {
    objTypeWriterTable.getOrElseUpdate(level, new SimpleFieldWriter("<type:L%d>".format(level)))
  }

  def fieldWriterOf(paramName: String, valueType: ObjectType): FieldWriter = {
    val k = ParamKey(paramName, valueType)
    writerTable.getOrElseUpdate(k, new SimpleFieldWriter(paramName))
  }

  private var prev = OrdPath.zero

  def encode(obj: Any) {
    val next = prev.sibling
    encode(next, obj)
    prev = next
  }


  private def encode(path: OrdPath, obj: Any) {
    val cl = obj.getClass
    if (TypeUtil.isSeq(cl)) {
      objTypeWriter(path.length).write(path, "Seq")
      val seq = obj.asInstanceOf[Seq[_]]
      var next = path.nextChild
      seq.foreach { e =>
        encode(next, e)
        next = next.sibling
      }
    }
    else {
      val schema = ObjectSchema(cl)

      // write object type
      objTypeWriter(path.length).write(path, schema.name)

      for (param <- schema.constructor.params) {
        val next = path.nextChild
        encode(next, param.name, param.valueType, param.get(obj))
      }
    }
  }


  private def encode(path: OrdPath, paramName: String, valueType: ObjectType, obj: Any) {

    def writeField {
      // TODO improve the value retrieval by using code generation
      val w = fieldWriterOf(paramName, valueType)
      w.write(path, obj)
    }

    valueType match {
      case p: Primitive => writeField
      case t: TextType => writeField
      case s: StandardType =>
        encode(path, obj)
      case s: SeqType =>
        val seq = obj.asInstanceOf[Seq[_]]
        var next = path.nextChild
        seq.foreach { e =>
          encode(next, paramName, s.elementType, e)
          next = next.sibling
        }
      case o: OptionType =>
        val opt = obj.asInstanceOf[Option[_]]
        opt.foreach {
          encode(path, paramName, o.elementType, _)
        }
      case g: GenericType =>
        warn("TODO impl: %s", g)
    }

  }

}