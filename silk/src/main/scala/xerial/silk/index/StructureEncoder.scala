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
  def newWriter(name:String) : FieldWriter

}
class SimpleFieldWriterFactory extends FieldWriterFactory {

  private val writer = Seq.newBuilder[SimpleFieldWriter]

  def newWriter(name: String) = {
    val w = new SimpleFieldWriter(name)
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


class SimpleFieldWriter(name: String) extends FieldWriter with Logger {

  val entry = Seq.newBuilder[String]

  override def toString = f"$name offset:${first.getOrElse("")}"

  private var first : Option[OrdPath] = None
  private var prev : Option[OrdPath] = None
  def write(index: OrdPath, value: Any) {
    if(first.isEmpty) {
      first = Some(index)
    }
    val diff = prev.map(index.incrementalDiff(_)) getOrElse (OrdPath.zero)
    val lmnz = diff.leftMostNonZeroPos
    val offset = if(lmnz == 0) 0 else diff(lmnz-1)


    val s = "write %25s (%-15s) %-15s [level:%d, offset:%d] : %s".format(name, index, diff, diff.leftMostNonZeroPos, offset, value)
    entry += s
    debug(s)
    prev = Some(index)
  }

  def entries = entry.result



}




case class ParamKey(tagPath: Path, valueType: ObjectType)

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
    objectWriterTable.getOrElseUpdate(level, writerFactory.newWriter(f"<obj:L$level>"))
  }

  def fieldWriterOf(level:Int, tagPath:Path, valueType: ObjectType): FieldWriter = {
    val k = ParamKey(tagPath, valueType)
    writerTable.getOrElseUpdate(k, writerFactory.newWriter(tagPath.fullPath))
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

    def iterate(obj:AnyRef, elementType:ObjectType) : OrdPath = {
      obj match {
        case lst:Traversable[Any] =>
          objectWriter(path.length).write(path, ot)
          var next = path.child
          lst.foreach { e =>
            encodeObj(next, tagPath, e, elementType)
            next = next.sibling
          }
          path.sibling
        case _ =>
          error(f"unknown type ${obj.getClass}")
          path
      }
    }

    val next = ot match {
      case p: Primitive =>
        writeField
        path
      case t: TextType =>
        writeField
        path
      case SeqType(cl, et) =>
        iterate(obj.asInstanceOf[Traversable[_]], et)
      case SetType(cl, et) =>
        iterate(obj.asInstanceOf[Traversable[_]], et)
      case MapType(cl, kt, vt) =>
        objectWriter(path.length).write(path, ot)
        val m = obj.asInstanceOf[Traversable[_]]
        var next = path.child
        for((k, v) <- m) {
          val kv = (k, v)
          encodeObj(next, tagPath, kv, TupleType(kv.getClass, Seq(kt, vt)))
          next = next.sibling
        }
        path.sibling
      case TupleType(cl, elemTypes) =>
        val p = obj.asInstanceOf[Product]
        val next = path.child
        val len = p.productArity
        objectWriter(path.length).write(path, ot)
        for(i <- 0 until len) {
          encodeObj(next, tagPath / (i+1).toString, p.productElement(i), elemTypes(i))
        }
        path.sibling
      case OptionType(cl, et) =>
        val opt = obj.asInstanceOf[Option[_]]
        opt.map { e =>
          encodeObj(path, tagPath, e, et)
          path.sibling
        } getOrElse(path)
      case EitherType(cl, lt, rt) =>
        obj match {
          case Left(l) => encodeObj(path, tagPath, l, lt)
          case Right(r) => encodeObj(path, tagPath, r, rt)
        }
        path.sibling
      case ArrayType(cl, et) =>
        iterate(obj.asInstanceOf[Traversable[_]], et)
      case g: GenericType =>
        warn("TODO impl: %s", g)
        path.sibling
      case StandardType(cl) => encodeClass(path, tagPath, obj, cl)
      case _ => encodeClass(path, tagPath, obj, obj.getClass)
    }

    next
  }

  private def encodeClass(path:OrdPath, tagPath:Path, obj:Any, cls:Class[_]) = {
    val schema = ObjectSchema(cls)
    // write object type
    objectWriter(path.length).write(path, schema)
    val child = path.child
    for (c <- schema.findConstructor; param <- c.params) {
      // TODO improve the value retrieval by using code generation
      encodeObj(child, tagPath / param.name, param.get(obj), param.valueType)
    }
    path.sibling
  }

}