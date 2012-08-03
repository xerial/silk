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

package xerial.silk.io

import java.io.{ByteArrayOutputStream, PrintStream, OutputStream}
import xerial.silk.model.{SilkModel, SilkPackage}
import xerial.core.log.Logging
import xerial.core.lens.{StandardType, ObjectType, GenericType, ObjectSchema, TypeUtil}
import xerial.core.util.CName

//--------------------------------------
//
// SilkTextWriter.scala
// Since: 2012/03/23 13:41
//
//--------------------------------------

/**
 * Configuration of silk text format
 */
class SilkTextFormatConfig
(
  // preamble
  val restrainPreambleHeader: Boolean = false,

  // indentation
  val indentWidth: Int = 1,
  val indentCommentLine: Boolean = false,

  // line wrap
  val lineWidth: Int = 120,

  // comment
  val insertSpaceAfterCommentSymbol: Boolean = true,
  // comma
  val insertSpaceAfterComma: Boolean = true,

  // colon
  val insertSpaceBeforeColon: Boolean = false,
  val insertSpaceAfterColon: Boolean = true,

  /**
   * insert tab after colon symbol (intermediate node only)
   */
  val insertTabAfterColon: Boolean = false,

  // colon (inline-node)
  val insertSpaceBeforeAttributeColon: Boolean = false,
  val insertSpaceAfterAttributeColon: Boolean = false,
  val insertTabAfterAttributeColon: Boolean = false,

  // parenthesis
  val insertSpaceOutsideOfParenthesis: Boolean = false,
  val insertSpaceInsideOfParenthesis: Boolean = false,

  // preference to inline style
  val preferInlineStyle: Boolean = false,

  val EOL: String = "\n"
  )

/**
 * Hold contexts
 * @param contextLevelOffset
 */
class SilkTextWriterContext
(
  val contextLevelOffset: Int = 0
  )

object SilkTextWriter {
  def toSilk[A](obj: A): String = {
    val buf = new ByteArrayOutputStream
    val w = new SilkTextWriter(buf)
    w.write(obj)
    w.flush
    new String(buf.toByteArray)
  }

  def toSilk[A](name: String, obj: A): String = {
    val buf = new ByteArrayOutputStream
    val w = new SilkTextWriter(buf)
    w.write(name, obj)
    w.flush
    new String(buf.toByteArray)
  }

}

/**
 * Silk io in text format
 *
 * @author leo
 */
class SilkTextWriter(out: OutputStream, context: SilkTextWriterContext = new SilkTextWriterContext, config: SilkTextFormatConfig = new SilkTextFormatConfig) extends SilkObjectWriter with Logging {

  private val o = new PrintStream(out)
  private var indentLevel = 0
  private var contextLevel = 0
  private val observedClasses = collection.mutable.Set[Class[_]]()
  private var currentPackage = SilkPackage.root

  if (!config.restrainPreambleHeader) {
    o.print("%silk version:" + SilkModel.VERSION)
    newline
  }

  private def indent {
    val indentLen = (context.contextLevelOffset + indentLevel) * config.indentWidth
    indent(indentLen)
  }

  private def indent(len: Int) {
    for (i <- 0 until len)
      o.print(' ')
  }

  private def newline = o.print(config.EOL)

  private def writeType(typeName: String) = {
    o.print("[")
    o.print(typeName)
    o.print("]")
    this
  }

  private def nodePrefix = {
    indent
    o.print("-")
    this
  }

  private def node(name: String) = {
    nodePrefix
    o.print(name)
  }

  private def leaf(name: String, value: String): self = {
    node(name)
    colon
    o.print(value)
    newline
    this
  }

  def preamble(text: String): self = {
    indent
    o.print("%")
    o.print(text)
    newline
    this
  }

  private def colon = {
    // colon
    if (config.insertSpaceBeforeColon)
      o.print(" ")
    o.print(":")
    if (config.insertSpaceAfterColon)
      o.print(" ")
  }

  def flush = o.flush

  def objectScope(schema: ObjectSchema)(body: => Unit): Unit = {
    nodePrefix
    writeType(schema.name)
    newline
    indent(body)
  }

  def objectScope(name: String, schema: ObjectSchema)(body: => Unit): Unit = {
    nodePrefix
    o.print(name)
    writeType(schema.name)
    newline
    indent(body)
  }

  def indent(body: => Unit): Unit = {
    val prevIndentLevel = indentLevel
    try {
      indentLevel += 1
      body
    }
    finally {
      indentLevel = prevIndentLevel
    }
  }

  private def hasToSilk[A](obj: A) = {
    obj.isInstanceOf[SilkWritable]
  }

  def write[A](obj: A) = {
    if (obj != null) {
      val cl = obj.getClass
      schema(cl)

      if (TypeUtil.isPrimitive(cl)) {
        writeValue("", obj)
      }
      else {
        if (hasToSilk(obj)) {
          val w = obj.asInstanceOf[SilkWritable]
          w.toSilk(this)
        }
        else {
          val schema = ObjectSchema.getSchemaOf(obj)
          schema.parameters foreach {
            p =>
              val value = p.get(obj)
              write(p.name, value)
          }
        }
      }
    }
    this
  }

  private def hasParameters(cl: Class[_]) = {
    val s = ObjectSchema(cl)
    s.parameters.length > 0
  }

  import TypeUtil._


  def write[A](name: String, obj: A) = {
    if (obj != null) {
      val cl = cls(obj)
      schema(cl)
      if (TypeUtil.isPrimitive(cl)) {
        writeValue(name, obj)
      }
      else {
        if (hasToSilk(obj)) {
          node(name)
          newline
          indent {
            val w = obj.asInstanceOf[SilkWritable]
            w.toSilk(this)
          }
        }
        else if (isArray(cl)) {
          writeArray(name, obj.asInstanceOf[Array[_]])
        }
        else if (isMap(cl)) {
          writeMap(name, obj.asInstanceOf[Map[_, _]])
        }
        else if (isSet(cl)) {
          writeSet(name, obj.asInstanceOf[Set[_]])
        }
        else if (isSeq(cl)) {
          writeSeq(name, obj.asInstanceOf[Seq[_]])
        }
        else if (hasParameters(cl)) {
          val s = ObjectSchema(cl)
          node(name)
          if (CName(name) != CName(cl.getSimpleName))
            writeType(cl.getSimpleName)

          newline
          indent {
            s.parameters foreach {
              p =>
                write(p.name, p.get(obj))
            }
          }
        }
        else {
          writeValue(name, obj)
        }
      }
    }
    this
  }

  private def schema(cl: Class[_]) {
    if (!observedClasses.contains(cl)) {
      observedClasses += cl

      if (isPrimitive(cl) || cl.getSimpleName.endsWith("$")) {
        // do nothing for primitives
      }
      else if (cl.isInstanceOf[Class[xerial.silk.model.Enum[_]]]) {
        //TODO
        //val enum = TypeUtil.newInstance(cl).asInstanceOf[xerial.silk.model.Enum[_]]
        //preamble("enum %s(%s)".format(cl.getSimpleName, enum.symbols.mkString(",")))
      }
      else if (isArray(cl)) {
        val elemType = cl.getComponentType
        schema(elemType)
      }
      else if (isTraversableOnce(cl)) {
        // TODO
      }
      else {
        val s = ObjectSchema(cl)
        def iter(t: ObjectType) {
          t match {
            case StandardType(rawType) => schema(rawType)
            case g:GenericType => {
              g.genericTypes.foreach(gt => iter(gt))
            }
            case _ => this.warn("unknown type " + t)
          }
        }
        writeSchema(s)
        s.parameters.foreach(p => iter(p.valueType))
      }
    }
  }

  def writeSchema(schema: ObjectSchema) = {
    val p = SilkPackage(schema.cl, schema.name)
    if (p != currentPackage) {
      preamble("package " + p.name)
      currentPackage = p
    }

    val b = new StringBuilder
    b.append("record ")
    b.append(schema.name)
    if (!schema.parameters.isEmpty) {
      b.append(" - ")
      val attr =
        for (a <- schema.parameters) yield {
          "%s:%s".format(a.name, a.valueType)
        }
      b.append(attr.mkString(", "))
    }
    preamble(b.toString)
  }

  protected def pushContext[A](obj: A) {
    indent(contextLevel * config.indentWidth)
    o.print("=")
    // TODO
    o.print(obj.toString)
    newline
    contextLevel += 1
  }

  protected def popContext {
    contextLevel -= 1
  }

  def writeInt(name: String, v: Int) = leaf(name, v.toString)

  def writeShort(name: String, v: Short) = leaf(name, v.toString)

  def writeLong(name: String, v: Long) = leaf(name, v.toString)

  def writeFloat(name: String, v: Float) = leaf(name, v.toString)

  def writeDouble(name: String, v: Double) = leaf(name, v.toString)

  def writeBoolean(name: String, v: Boolean) = leaf(name, v.toString)

  def writeByte(name: String, v: Byte) = leaf(name, v.toString)

  def writeChar(name: String, v: Char) = leaf(name, v.toString)

  def writeString(name: String, s: String) = leaf(name, s)

  def writeValue[A](name: String, v: A) = leaf(name, v.toString)

  def writeSeq[A](name: String, seq: Seq[A]) = {
    writeTraversable(name, seq)
  }

  def writeArray[A](name: String, array: Array[A]) = {



    writeTraversable(name, array)
  }

  def writeMap[A, B](name: String, map: Map[A, B]) = {
    // TODO map representation -> (key, value) tuples
    // Need an inline silk writer for packing data into a column
    this
  }

  def writeSet[A](name: String, set: Set[A]) = {
    writeTraversable(name, set)
  }

  def writeTraversable[A](name: String, t: Traversable[A]): self = {
    val size = t.size
    indent
    if (size > 0) {
      o.print("@size:")
      o.print(size)
      newline
      t.foreach {
        e =>
          write(name, e)
      }
    }
    this
  }

}

class InlineSilkTextWriter(out: OutputStream) extends SilkObjectWriter {
  def write[A](obj: A) = null

  def write[A](name: String, obj: A) = null

  def writeSchema(schema: ObjectSchema) = null

  protected def pushContext[A](obj: A) {}

  protected def popContext {}

  def writeInt(name: String, v: Int) = null

  def writeShort(name: String, v: Short) = null

  def writeLong(name: String, v: Long) = null

  def writeFloat(name: String, v: Float) = null

  def writeDouble(name: String, v: Double) = null

  def writeBoolean(name: String, v: Boolean) = null

  def writeByte(name: String, v: Byte) = null

  def writeChar(name: String, v: Char) = null

  def writeString(name: String, s: String) = null

  def writeValue[A](name: String, v: A) = null

  def writeSeq[A](name: String, seq: Seq[A]) = null

  def writeArray[A](name: String, array: Array[A]) = null

  def writeMap[A, B](name: String, map: Map[A, B]) = null

  def writeSet[A](name: String, set: Set[A]) = null
}