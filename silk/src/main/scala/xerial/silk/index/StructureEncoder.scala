//--------------------------------------
//
// StructureEncoder.scala
// Since: 2013/01/17 10:36 AM
//
//--------------------------------------

package xerial.silk.index

import xerial.lens._
import xerial.core.log.Logger
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

trait FieldWriter {
  def write(index: OrdPath, value: Any): Unit
}

trait FieldWriterFactory {
  def newWriter(level:Int, name:String) : FieldWriter

}
class SimpleFieldWriterFactory extends FieldWriterFactory {

  private val writer = Seq.newBuilder[SimpleFieldWriter]

  def newWriter(level: Int, name: String) = {
    val w = new SimpleFieldWriter(level, name)
    writer += w
    w
  }

  def writers = writer.result


  def contentString : String = {
    val s = Seq.newBuilder[String]
    for(w <- writers) {
      s += w.toString
      for(e <- w.entries)
        s += e
    }
    s.result.mkString("\n")
  }

}


class SimpleFieldWriter(level:Int, name: String) extends FieldWriter with Logger {

  val entry = Seq.newBuilder[String]

  override def toString = "%s:L%d offset:%s".format(name, level, first.getOrElse(""))

  private var first : Option[OrdPath] = None
  private var prev : Option[OrdPath] = None
  def write(index: OrdPath, value: Any) {
    if(first.isEmpty) {
      first = Some(index)
    }
    val diff = prev.map(index.incrementalDiff(_)) getOrElse (OrdPath.zero)
    val lmnz = diff.leftMostNonZeroPos
    val offset = if(lmnz == 0) 0 else diff(lmnz-1)


    val s = "write %25s:L%d (%-15s) %-15s [level:%d, offset:%d] : %s".format(name, level, index, diff, diff.leftMostNonZeroPos, offset, value)
    entry += s
    debug(s)
    prev = Some(index)
  }

  def entries = entry.result



}




case class ParamKey(level:Int, tagPath: Path, valueType: ObjectType)

object StructureEncoder {
  def simpleEncoder = {
    val s = new StructureEncoder(new SimpleFieldWriterFactory)
    s
  }

}

/**
 *
 *
 * @author Taro L. Saito
 */
class StructureEncoder(val writerFactory:FieldWriterFactory) extends Logger {

  import TypeUtil._

  private val objectWriterTable = collection.mutable.Map[Int, FieldWriter]()
  private val writerTable = collection.mutable.Map[ParamKey, FieldWriter]()


  def objectWriter(level:Int) : FieldWriter = {
    objectWriterTable.getOrElseUpdate(level, writerFactory.newWriter(level, "<obj>"))
  }

  def fieldWriterOf(level:Int, tagPath:Path, valueType: ObjectType): FieldWriter = {
    val k = ParamKey(level, tagPath, valueType)
    writerTable.getOrElseUpdate(k, writerFactory.newWriter(level, tagPath.fullPath))
  }

  private var current = OrdPath.one





  def encode[A:TypeTag](obj: A) {
    current = encodeObj_i(current, Path.root, obj)
  }

  private def encodeObj_i[A : TypeTag](path: OrdPath, tagPath:Path, obj: A) : OrdPath = {
    val ot = ObjectType(obj)
    encodeObj(path, tagPath, obj, ot)
  }


  private def encodeObj[A : TypeTag](path: OrdPath, tagPath:Path, obj: A, ot:ObjectType) : OrdPath = {


    trace(f"encoding cl:${obj.getClass.getSimpleName}, type:$ot")

    def fieldWriter = fieldWriterOf(path.length, tagPath, ot)

    def writeField {
      // TODO improve the value retrieval by using code generation
      fieldWriter.write(path, obj)
    }

    val next = ot match {
      case p: Primitive =>
        writeField
        path
      case t: TextType =>
        writeField
        path
      case SeqType(cl, et) =>
        objectWriter(path.length).write(path, f"Seq[$et]")
        val seq = obj.asInstanceOf[Seq[_]]
        var next = path.child
        seq.foreach { e =>
          encodeObj(next, tagPath, e, et)
          next = next.sibling
        }
        path.sibling
      case OptionType(cl, et) =>
        val opt = obj.asInstanceOf[Option[_]]
        opt.foreach { e =>
          encodeObj(path, tagPath, e, et)
        }
        path.sibling
      case ArrayType(cl, et) =>
        val arr = obj.asInstanceOf[Array[_]]
        if(!arr.isEmpty) {
          fieldWriter.write(path, "Array[%s]".format(et))
          var next = path.child
          arr.foreach { e =>
            encodeObj(next, tagPath, e, et)
            next = next.sibling
          }
        }
        path.sibling
      case g: GenericType =>
        warn("TODO impl: %s", g)
        path.sibling
      case StandardType(cl) => encodeClass(path, tagPath, obj, cl)
      case AnyRefType => encodeClass(path, tagPath, obj, obj.getClass)
      case _ => encodeClass(path, tagPath, obj, obj.getClass)
    }

    next
  }

  private def encodeClass(path:OrdPath, tagPath:Path, obj:Any, cls:Class[_]) = {
    val schema = ObjectSchema(cls)
    // write object type
    objectWriter(path.length).write(path, "[%s]".format(schema.name))
    var child = path.child
    for (c <- schema.findConstructor; param <- c.params) {
      // TODO improve the value retrieval by using code generation
      encodeObj(child, tagPath / param.name, param.get(obj), param.valueType)
      child = child.sibling
    }
    path.sibling
  }

}