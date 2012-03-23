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

import xerial.silk.lens.ObjectSchema
import java.io.{ByteArrayOutputStream, PrintStream, OutputStream}
import xerial.silk.lens.ObjectSchema.{Type, ValueType, GenericType, StandardType}
import xerial.silk.util.{CName, Logger, TypeUtil}
import xerial.silk.model.{SilkModel, SilkPackage}

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
  val restrainPreambleHeader : Boolean = false,

  // indentation
  val indentWidth: Int = 2,
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

object SilkTextWriter {
  def toSilk[A](obj: A): String = {
    val buf = new ByteArrayOutputStream
    val w = new SilkTextWriter(buf)
    w.write(obj)
    w.flush
    new String(buf.toByteArray)
  }
}

/**
 * Silk io in text format
 *
 * @author leo
 */
class SilkTextWriter(out: OutputStream, config: SilkTextFormatConfig = new SilkTextFormatConfig) extends SilkObjectWriter with Logger {

  private val o = new PrintStream(out)
  private var indentLevel = 0
  private var contextLevel = 0
  private val observedClasses = collection.mutable.Set[Class[_]]()
  private var currentPackage = SilkPackage.root

  if(!config.restrainPreambleHeader) {
    o.print("%silk version:" + SilkModel.VERSION)
    newline
  }
  
  
  private def writeIndent {
    val indentLen = indentLevel * config.indentWidth
    indent(indentLen)
  }
  
  def indent(len:Int) {
    for(i <- 0 until len)
      o.print(' ')
  }
  

  private def newline = o.print(config.EOL)

  private def writeType(typeName: String) = {
    o.print("=")
    o.print(typeName)
    this
  }

  private def nodePrefix = {
    writeIndent
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
    val cl = obj.getClass
    schema(cl)
    context(cl.getSimpleName) { writer =>
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

  def write[A](name: String, obj: A) = {
    val cl = obj.getClass
    schema(cl)
    if (TypeUtil.isPrimitive(cl)) {
      writeValue(name, obj)
    }
    else {
      import TypeUtil._
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
        newline
        indent {
          s.parameters foreach { p =>
            write(p.name,  p.get(obj))
          }
        }
      }
      else {
        writeValue(name, obj)
      }
    }
    this
  }

  private def schema(cl: Class[_]) {
    if (!observedClasses.contains(cl)) {
      observedClasses += cl

      import TypeUtil._
      if (isPrimitive(cl)) {
        // do nothing for primitives
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
        def iter(t: Type) {
          t match {
            case StandardType(rawType) => schema(rawType)
            case GenericType(rawType, genericTypes) => {
              genericTypes.foreach(gt => iter(gt))
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
    if(p != currentPackage) {
      o.print("%package ")
      o.print(p.name)
      currentPackage = p
      newline
    }
    o.print("%record ")
    o.print(schema.name)
    if (!schema.parameters.isEmpty) {
      o.print(" - ")
      val attr =
        for (a <- schema.parameters) yield {
          "%s:%s".format(a.name, a.valueType)
        }
      o.print(attr.mkString(", "))
    }
    newline
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

    this
  }
  def writeSet[A](name: String, set: Set[A]) = {
    writeTraversable(name, set)
  }

  def writeTraversable[A](name:String, t:Traversable[A]) : self = {
    val size = t.size
    t.foreach {
      e =>
        write(name, e)
    }
    this
  }

}