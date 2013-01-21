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

trait FieldWriterFactory {
  def newWriter(level:Int, name:String) : FieldWriter

}
class SimpleFieldWriterFactory extends FieldWriterFactory {
  def newWriter(level: Int, name: String) = new SimpleFieldWriter(level, name)
}


class SimpleFieldWriter(level:Int, name: String) extends FieldWriter with Logger {
  private var prev : Option[OrdPath] = None
  def write(index: OrdPath, value: Any) {
    val diff = prev.map(index.incrementalDiff(_)) getOrElse (OrdPath.zero)
    val lmnz = diff.leftMostNonZeroPos
    val dl = if(lmnz == 0) 0 else diff(lmnz-1)
    debug("write %10s:L%d (%-15s) %-15s [level:%d, offset:%d] : %s".format(name, level, index, diff, diff.leftMostNonZeroPos, dl, value))
    prev = Some(index)
  }
}




case class ParamKey(level:Int, name: String, valueType: ObjectType)

object StructureEncoder {
  def simpleEncoder = new StructureEncoder(new SimpleFieldWriterFactory)

}

/**
 *
 *
 * @author Taro L. Saito
 */
class StructureEncoder(writerFactory:FieldWriterFactory) extends Logger {

  import TypeUtil._

  private val objectWriterTable = collection.mutable.Map[Int, FieldWriter]()
  private val writerTable = collection.mutable.Map[ParamKey, FieldWriter]()

  def objectWriter(level:Int) : FieldWriter = {
    objectWriterTable.getOrElseUpdate(level, writerFactory.newWriter(level, "<obj>"))
  }

  def fieldWriterOf(level:Int, paramName: String, valueType: ObjectType): FieldWriter = {
    val k = ParamKey(level, paramName, valueType)
    writerTable.getOrElseUpdate(k, writerFactory.newWriter(level, paramName))
  }

  private var current = OrdPath.one

  def encode(obj: Any) {
    current = encode(current, obj)
  }


  private def encode(path: OrdPath, obj: Any) : OrdPath = {
    val cl = obj.getClass
    val ot = ObjectType(cl)

    val next = ot match {
      case SeqType(cl, t) =>
        objectWriter(path.length).write(path, "Seq")
        val seq = obj.asInstanceOf[Seq[_]]
        var next = path.child
        seq.foreach { e =>
          encode(next, e)
          next = next.sibling
        }
        path.sibling
      case _ =>
        val schema = ObjectSchema(cl)
        // write object type
        var next = path.child
        objectWriter(path.length).write(path, "[%s]".format(schema.name))
        for (c <- schema.findConstructor; param <- c.params) {
          encode(next, param.name, param.valueType, param.get(obj))
          next = next.sibling
        }
        path.sibling
    }

    next
  }


  /**
   * Encode an object when its explicit parameter name and type are known
   * @param path
   * @param paramName
   * @param valueType
   * @param obj
   */
  private def encode(path: OrdPath, paramName: String, valueType: ObjectType, obj: Any) {

    def fieldWriter = fieldWriterOf(path.length, paramName, valueType)

    def writeField {
      // TODO improve the value retrieval by using code generation
      fieldWriter.write(path, obj)
    }

    valueType match {
      case p: Primitive => writeField
      case t: TextType => writeField
      case s: StandardType =>
        encode(path, obj)
      case s: SeqType =>
        val seq = obj.asInstanceOf[Seq[_]]
        objectWriter(path.length).write(path, "Seq[%s]".format(s.elementType))
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
      case a: ArrayType =>
        val arr = obj.asInstanceOf[Array[_]]
        objectWriter(path.length).write(path, "Array[%s]".format(a.elementType))
        var next = path.child
        arr.foreach { e =>
          encode(next, paramName, a.elementType, e)
          next = next.sibling
        }
      case g: GenericType =>
        warn("TODO impl: %s", g)
    }

  }

}