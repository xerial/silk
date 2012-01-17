/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xerial.silk
package writer


import collection.mutable.Stack
import xerial.silk.lens.ObjectSchema
import java.io.{ByteArrayOutputStream, PrintStream, PrintWriter, OutputStream}
import util.{Cache, TypeUtil, Logging}

//--------------------------------------
//
// SilkWriter.scala
// Since: 2012/01/17 9:44
//
//--------------------------------------

/**
 * Interface for generating silk data from objects
 *
 * @author leo
 */
trait SilkWriter {
  type self = SilkWriter

  def write[A](obj: A): self

  def write[A, B](parent: A, child: B): self

  def pushContext[A](obj: A): Unit

  def popContext: Unit
}

trait SilkContextStack extends Logging {
  private val contextStack = new Stack[Any]

  def pushContext[A](obj: A): Unit = {
    contextStack.push(obj)
  }

  def popContext: Unit = {
    if (contextStack.isEmpty)
      warn {
        "Context stack is empty"
      }
    else
      contextStack.pop
  }
}

object SilkObjectWriter {
  sealed abstract class SilkValueType
  case class PrimitiveValue(valueType:Class[_]) extends SilkValueType
  case class SequenceValue(valueType:Class[_]) extends SilkValueType
  case class MapValue(valueType:Class[_]) extends SilkValueType
  case class SetValue(valueType:Class[_]) extends SilkValueType
  case class ObjectValue(valueType:Class[_]) extends SilkValueType


  private val silkValueTable = Cache[Class[_], SilkValueType](createSilkValueType)
  private def createSilkValueType(cl:Class[_]):SilkValueType = {
    import TypeUtil._
    if(TypeUtil.isPrimitive(cl))
      PrimitiveValue(cl)
    else if(TypeUtil.isSeq(cl))
      SequenceValue(cl)
    else if(TypeUtil.isMap(cl))
      MapValue(cl)
    else if(TypeUtil.isSet(cl))
      SetValue(cl)
    else
      ObjectValue(cl)
  }

  def getSilkValueType(cl:Class[_]): SilkValueType = silkValueTable(cl)

}

trait SilkObjectWriter {
  import SilkObjectWriter._
  type self = SilkObjectWriter

  def writeVal(name: String, v: AnyVal): self

  def writeSchema(schema: ObjectSchema)

  def writeString(s: String): self

  def writeVal[A <: AnyVal](i: A): self

  def writeSeq[A](seq: Seq[A]): self

  def writeMap[A, B](map: Map[A, B]): self

  def writeSet[A](set: Set[A]): self
}

object SilkTextWriter {

  def toSilk(v: Any): String = {
    val buf = new ByteArrayOutputStream
    val writer = new SilkTextWriter(buf)
    writer.write(v)
    writer.flush
    buf.toString
  }


}





class SilkTextWriter(out: OutputStream) extends SilkWriter with SilkContextStack {

  private var registeredSchema = Set[ObjectSchema]()

  class ObjectWriter extends SilkObjectWriter {
    val o = new PrintStream(out)
    var indentLevel = 0

    private def writeIndent = {
      val indentLen = indentLevel // *  2
      val indent = Array.fill(indentLen)(' ')
      o.print(indent)
    }

    private def writeType(typeName: String) = {
      o.print("[")
      o.print(typeName)
      o.print("]")
      this
    }

    private def writeNodePrefix = {
      writeIndent
      o.print("-")
      this
    }

    def writeVal(name: String, v: AnyVal) = {
      writeNodePrefix
      o.print(name)
      o.print(":")
      o.print(v.toString)
      o.print("\n")
      this
    }

    def writeVal[A <: AnyVal](v: A) = {
      writeNodePrefix
      writeType(v.getClass.getSimpleName)
      o.print(":")
      o.print(v.toString)
      o.print("\n")
      this
    }

    def writeString(s: String) = null

    def writeSeq[A](seq: Seq[A]) = null

    def writeMap[A, B](map: Map[A, B]) = null

    def writeSet[A](set: Set[A]) = null

    def writeSchema(schema: ObjectSchema) = {
      o.print("%class ")
      o.print(schema.name)
      if (!schema.attributes.isEmpty) {
        o.print(" - ")
        val attr =
          for (a <- schema.attributes) yield {
            "%s:%s".format(a.name, a.valueType.getSimpleName)
          }
        o.print(attr.mkString(", "))
      }
      o.print("\n")
    }

    def objectScope(schema: ObjectSchema)(body: => Unit): Unit = {
      writeNodePrefix
      writeType(schema.name)
      o.print("\n")
      indentBlock(body)
    }

    def objectScope(name: String, schema: ObjectSchema)(body: => Unit): Unit = {
      writeNodePrefix
      o.print(name)
      writeType(schema.name)
      o.print("\n")
      indentBlock(body)
    }

    def indentBlock(body: => Unit): Unit = {
      val prevIndentLevel = indentLevel
      try {
        indentLevel += 1
        body
      }
      finally {
        indentLevel = prevIndentLevel
      }
    }

    def flush = o.flush
  }

  private val w = new ObjectWriter

  def flush: Unit = w.flush

  import TypeUtil._
  def write[A](obj: A) = {

    val cl: Class[_] = obj.getClass
    if (TypeUtil.isPrimitive(cl)) {
      // primitive values
      w.writeVal(obj.asInstanceOf[AnyVal])
    }
    else if (TypeUtil.isSeq(cl)) {
      // array
      val elementType = cl.getComponentType



    }
    else {
      // general object
      val schema = ObjectSchema.getSchemaOf(obj)
      if (!registeredSchema.contains(schema)) {
        w.writeSchema(schema)
        registeredSchema += schema
      }

      w.objectScope(schema) {
        for (a <- schema.attributes) {
          val v = schema.read(obj, a)
          writeAttribute(a, v)
        }
      }
    }

    this
  }

  def writeSchema(cl:Class[_]) = {



  }


  def writeAttribute[A](attr: ObjectSchema.Attribute, value: A) = {
    if (TypeUtil.isPrimitive(attr.valueType)) {
      w.writeVal(attr.name, value.asInstanceOf[AnyVal])
    }
    else {
      w.objectScope(attr.name, ObjectSchema.getSchemaOf(attr.valueType)) {
        write(value)
      }
    }

    this
  }


  def write[A, B](parent: A, child: B) = {

    this
  }


}