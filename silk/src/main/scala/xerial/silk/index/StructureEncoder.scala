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
    val diff = prev.map(index.incrementalDiff(_)) getOrElse (OrdPath.zero)
    val lmnz = diff.leftMostNonZeroPos
    val dl = if(lmnz == 0) 0 else diff(lmnz-1)
    debug("write %-10s (%-10s) %-10s [r:%d, d:%d] : %s".format(name, index, diff, diff.leftMostNonZeroPos, dl, value))
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

  private val objectWriterTable = collection.mutable.Map[Int, FieldWriter]()
  private val writerTable = collection.mutable.Map[ParamKey, FieldWriter]()

  def objectWriter(level:Int) : FieldWriter = {
    objectWriterTable.getOrElseUpdate(level, new SimpleFieldWriter("<obj:L%d>".format(level)))
  }

  def fieldWriterOf(paramName: String, valueType: ObjectType): FieldWriter = {
    val k = ParamKey(paramName, valueType)
    writerTable.getOrElseUpdate(k, new SimpleFieldWriter(paramName))
  }

  private var current = OrdPath.one

  def encode(obj: Any) {
    current = encode(current, obj)
  }


  private def encode(path: OrdPath, obj: Any) : OrdPath = {
    val cl = obj.getClass
    if (TypeUtil.isSeq(cl)) {
      //objectWriter(path.length).write(path, "Seq")
      val seq = obj.asInstanceOf[Seq[_]]
      var next = path
      seq.foreach { e =>
        encode(next, e)
        next = next.sibling
      }
      next
    }
    else {
      val schema = ObjectSchema(cl)

      // write object type
      objectWriter(path.length).write(path, schema.name)

      var next = path
      for (param <- schema.constructor.params) {
        next = path.child
        encode(next, param.name, param.valueType, param.get(obj))
      }
      next
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
        var next = path.child
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