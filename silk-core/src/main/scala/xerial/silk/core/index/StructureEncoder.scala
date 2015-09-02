/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// StructureEncoder.scala
// Since: 2013/01/17 10:36 AM
//
//--------------------------------------

package xerial.silk.core.index

import java.io.File
import java.util.Date

import xerial.core.log.Logger
import xerial.lens.{ArrayType, EitherType, MapType, OptionType, ParSeqType, SeqType, SetType, StandardType, TupleType, _}

import scala.collection.GenTraversable
import scala.collection.parallel.ParIterable
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

/**
 * Field writer has redundant write methods for all of the primitive types
 * in order to avoid boxing/unboxing of the primitive types.
 * Actual FieldWriter implementations only need to support one of the methods.
 *
 *
 */
trait FieldWriter {

  def write(index: OrdPath, value: Any): Unit
  def writeBoolean(index: OrdPath, value: Boolean): Unit
  def writeByte(index: OrdPath, value: Byte): Unit
  def writeChar(index: OrdPath, value: Char): Unit
  def writeShort(index: OrdPath, value: Short): Unit
  def writeInt(index: OrdPath, value: Int): Unit
  def writeFloat(index: OrdPath, value: Float): Unit
  def writeLong(index: OrdPath, value: Long): Unit
  def writeDouble(index: OrdPath, value: Double): Unit
  def writeString(index: OrdPath, value: String): Unit
  def writeFile(index: OrdPath, value: File): Unit
  def writeDate(index: OrdPath, value: Date): Unit
}

trait FieldWriterFactory {
  def newWriter(name: String, tpe: ObjectType): FieldWriter
}


class SimpleFieldWriterFactory extends FieldWriterFactory {

  private val writer = Seq.newBuilder[SimpleFieldWriter]

  def newWriter(name: String, tpe: ObjectType) = {
    val w = new SimpleFieldWriter(name)
    writer += w
    w
  }

  def writers = writer.result


  def contentString: String = {
    val s = Seq.newBuilder[String]
    for (w <- writers) {
      s += w.toString
      for (e <- w.entries)
        s += e
    }
    s.result.mkString("\n")
  }

}


class SimpleFieldWriter(name: String) extends FieldWriter with Logger {

  val entry = Seq.newBuilder[String]

  override def toString = f"$name offset:${first.getOrElse("")}"

  private var first: Option[OrdPath] = None
  private var prev: OrdPath = null


  def wrap[U](index: OrdPath)(body: => U) {
    if (first.isEmpty) {
      first = Some(index)
      prev = index
    }
    require(prev != null)
    body
    prev = index
  }

  def writeBoolean(index: OrdPath, value: Boolean) = write(index, value)
  def writeByte(index: OrdPath, value: Byte) = write(index, value)
  def writeChar(index: OrdPath, value: Char) = write(index, value)
  def writeShort(index: OrdPath, value: Short) = write(index, value)
  def writeInt(index: OrdPath, value: Int) = write(index, value)
  def writeFloat(index: OrdPath, value: Float) = write(index, value)
  def writeLong(index: OrdPath, value: Long) = write(index, value)
  def writeDouble(index: OrdPath, value: Double) = write(index, value)
  def writeString(index: OrdPath, value: String) = write(index, value)
  def writeFile(index: OrdPath, value: File) = write(index, value)
  def writeDate(index: OrdPath, value: Date) = write(index, value)


  def write(index: OrdPath, value: Any) {
    wrap(index) {
      val incrSteps = index.stepDiffs(prev)
      val steps = if (incrSteps.isEmpty) Seq(IncrStep(0, 0)) else incrSteps
      val s = f"$name%25s $index%-10s\t[${steps.mkString(", ")}] : $value"
      entry += s
      debug(s)
    }
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
class StructureEncoder(val writerFactory: FieldWriterFactory, private val encoder : FieldEncoder = new FieldEncoderWithJavassist) extends Logger {

  private val objectWriterTable = collection.mutable.Map[Int, FieldWriter]()
  private val writerTable = collection.mutable.Map[ParamKey, FieldWriter]()


  def objectWriter(level: Int): FieldWriter = {
    objectWriterTable.getOrElseUpdate(level, writerFactory.newWriter(f"<obj:L$level>", ObjectType(classOf[ObjectType])))
  }

  def fieldWriterOf(level: Int, tagPath: Path, valueType: ObjectType): FieldWriter = {
    val k = ParamKey(tagPath, valueType)
    writerTable.getOrElseUpdate(k, writerFactory.newWriter(tagPath.fullPath, valueType))
  }

  private var current = OrdPath.zero


  def encode[A: TypeTag](obj: A) {
    current = encodeObj_i(current, Path.root, obj)
  }

  private def encodeObj_i[A: TypeTag](path: OrdPath, tagPath: Path, obj: A): OrdPath = {
    val ot = ObjectType(obj)
    encodeObj(path, tagPath, obj, ot)
  }


  def encodeObj(path: OrdPath, tagPath: Path, obj: Any, ot: ObjectType): OrdPath = {

    //trace(f"encoding cl:${obj.getClass.getSimpleName}, type:$ot")

    def fieldWriter = fieldWriterOf(path.length, tagPath, ot)

    def iterate(obj: AnyRef, elementType: ObjectType): OrdPath = {
      obj match {
        case lst: GenTraversable[Any] =>
          objectWriter(path.length).write(path, ot)
          var next = path.child
          lst.foreach {
            e =>
              encodeObj(next, tagPath, e, elementType)
              next = next.sibling
          }
          path.sibling
        case arr: Array[_] =>
          objectWriter(path.length).write(path, ot)
          var next = path.child
          arr.foreach {
            e =>
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
      case s@StandardType(cl) =>
        encodeClass(path, tagPath, obj, s)
      case Primitive.Boolean =>
        fieldWriter.writeBoolean(path, obj.asInstanceOf[Boolean])
        path
      case Primitive.Byte =>
        fieldWriter.writeByte(path, obj.asInstanceOf[Byte])
        path
      case Primitive.Char =>
        fieldWriter.writeChar(path, obj.asInstanceOf[Char])
        path
      case Primitive.Short =>
        fieldWriter.writeShort(path, obj.asInstanceOf[Short])
        path
      case Primitive.Int =>
        fieldWriter.writeInt(path, obj.asInstanceOf[Int])
        path
      case Primitive.Float =>
        fieldWriter.writeFloat(path, obj.asInstanceOf[Float])
        path
      case Primitive.Long =>
        fieldWriter.writeLong(path, obj.asInstanceOf[Long])
        path
      case Primitive.Double =>
        fieldWriter.writeDouble(path, obj.asInstanceOf[Double])
        path
      case TextType.String =>
        fieldWriter.writeString(path, obj.asInstanceOf[String])
        path
      case TextType.Date =>
        fieldWriter.writeDate(path, obj.asInstanceOf[Date])
        path
      case TextType.File =>
        fieldWriter.writeFile(path, obj.asInstanceOf[File])
        path
      case SeqType(cl, et) =>
        iterate(obj.asInstanceOf[Traversable[_]], et)
      case ParSeqType(cl, et) =>
        iterate(obj.asInstanceOf[ParIterable[_]], et)
      case SetType(cl, et) =>
        iterate(obj.asInstanceOf[Traversable[_]], et)
      case MapType(cl, kt, vt) =>
        objectWriter(path.length).write(path, ot)
        val m = obj.asInstanceOf[Traversable[_]]
        var next = path.child
        for ((k, v) <- m) {
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
        for (i <- 0 until len) {
          encodeObj(next, tagPath / (i + 1).toString, p.productElement(i), elemTypes(i))
        }
        path.sibling
      case OptionType(cl, et) =>
        val opt = obj.asInstanceOf[Option[_]]
        opt.map {
          e =>
            encodeObj(path, tagPath, e, et)
            path.sibling
        } getOrElse (path)
      case EitherType(cl, lt, rt) =>
        obj match {
          case Left(l) => encodeObj(path, tagPath, l, lt)
          case Right(r) => encodeObj(path, tagPath, r, rt)
        }
        path.sibling
      case ArrayType(cl, et) =>
        iterate(obj.asInstanceOf[Array[_]], et)
      case g: GenericType =>
        warn(s"TODO impl: $g")
        path.sibling
      case _ =>
        encodeClass(path, tagPath, obj, StandardType(obj.getClass))
    }

    next
  }


  private def encodeClass(path: OrdPath, tagPath: Path, obj: Any, cls: StandardType[_]) = {
    //trace(f"encode class: $cls")
    // write object type
    objectWriter(path.length).write(path, cls)
    val child = path.child
    encoder.encode(this, child, tagPath, obj, cls)
    path.sibling
  }


}

trait FieldEncoder {
  def encode(encoder:StructureEncoder, path:OrdPath, tagPath:Path, obj:Any, tpe:StandardType[_])
}

trait RawFieldEncoder {
  def encode(encoder:StructureEncoder, path:OrdPath, tagPath:Path, obj:AnyRef, valueType:Array[ObjectType]) : Unit
}


class FieldEncoderWithReflection extends FieldEncoder {
  def encode(encoder:StructureEncoder, path:OrdPath, tagPath:Path, obj:Any, tpe:StandardType[_]) {
    for(param <- tpe.constructorParams) {
      encoder.encodeObj(path, tagPath / param.name, param.get(obj), param.valueType)
    }
  }
}

object FieldEncoderWithJavassist {

  private[FieldEncoderWithJavassist] val codeFactory = JavassistUtil.newFactory[Class[_], RawFieldEncoder]

}


class FieldEncoderWithJavassist extends FieldEncoder with Logger {

  import FieldEncoderWithJavassist._

  def encode(encoder:StructureEncoder, path:OrdPath, tagPath:Path, obj:Any, tpe:StandardType[_]) {



    def writeCode = {
      val prepPart = Seq.newBuilder[String]
      val writePart = Seq.newBuilder[String]
      for((param, i) <- tpe.constructorParams.zipWithIndex) {
        val vt = param.valueType
        val objType = param.owner.getCanonicalName
        val pathCode = s"""tagPath.$$div("${param.name}")"""
        if(vt.isPrimitive || vt.isTextType) {
          prepPart += s"  FieldWriter w$i = encoder.fieldWriterOf(path.length(), $pathCode, valueType[$i]);"
          writePart += s"  w$i.write${vt.name}(path, (($objType) obj).${param.name}());"
        }
        else {
          writePart += s"  encoder.encodeObj(path, $pathCode, (($objType) obj).${param.name}(), valueType[$i]);"
        }
      }

      (prepPart.result ++ writePart.result).mkString("\n")
    }

    def code =
      s"""
          |public void encode(
          |    StructureEncoder encoder,
          |    OrdPath path,
          |    xerial.lens.Path tagPath,
          |    Object obj,
          |    xerial.lens.ObjectType[] valueType)
          |{
          |$writeCode
          |}
        """.stripMargin


    val fieldEncoder = codeFactory.getOrElseUpdate(tpe.rawType, s"${tpe.rawType.getName}$$Encoder", Seq(code))
    fieldEncoder.encode(encoder, path, tagPath, obj.asInstanceOf[AnyRef], tpe.constructorParamTypes) // invoke writeInt, writeFloat, etc.
  }


}


