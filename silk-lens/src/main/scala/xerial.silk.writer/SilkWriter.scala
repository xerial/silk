package writer


import collection.mutable.Stack
import xerial.silk.util.Logging
import xerial.silk.lens.ObjectSchema
import java.io.{PrintStream, PrintWriter, OutputStream}

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


trait SilkObjectWriter {
  type self = SilkObjectWriter

  def writeInt(name:String, v:Int) : self

  def writeSchema(schema: ObjectSchema)

  def writeString(s: String): self

  def writeVal[A <: AnyVal](i: A): self

  def writeSeq[A](seq: Seq[A]): self

  def writeMap[A, B](map: Map[A, B]): self

  def writeSet[A](set: Set[A]): self
}


class SilkTextWriter(out: OutputStream) extends SilkWriter with SilkContextStack {

  private var registeredSchema = Set[ObjectSchema]()

  class ObjectWriter extends SilkObjectWriter {
    val o = new PrintStream(out)
    var indentLevel = 0

    def writeInt(name:String, v:Int)

    def writeString(s: String) = null

    def writeVal[A <: AnyVal](i: A) = null

    def writeSeq[A](seq: Seq[A]) = null

    def writeMap[A, B](map: Map[A, B]) = null

    def writeSet[A](set: Set[A]) = null

    def writeSchema(schema: ObjectSchema) = {
      o.print("%class ")
      o.print(schema.name)
      if (!schema.attributes.isEmpty) {
        o.print(" - ")
        o.print {
          for (a <- schema.attributes) yield {
            "%s:%s".format(a.name, a.valueType.getName)
          }.mkString(", ")
        }
      }
    }

    def scope[A](obj: A)(body: => Unit) = {
      val prevIndentLevel = indentLevel
      try {
        indentLevel += 1
        body
      }
      finally {
        indentLevel = prevIndentLevel
      }
    }

  }

  private val w = new ObjectWriter

  def write[A](obj: A) = {
    val schema = ObjectSchema.getSchemaOf(obj)
    if (registeredSchema.contains(schema)) {
      w.writeSchema(schema)
      registeredSchema += schema
    }

    w.scope(obj) {
      for (a <- schema.attributes) {
        


      }
    }


    this
  }

  def write[A, B](parent: A, child: B) = {

    this
  }


}