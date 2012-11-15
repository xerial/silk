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

import xerial.lens.ObjectSchema

//--------------------------------------
//
// SilkWriter.scala
// Since: 2012/03/23 13:38
//
//--------------------------------------


/**
 * Trait for classes that have custom silk generators
 * @author leo
 */
trait SilkWritable {
  def toSilk(out:SilkObjectWriter)
}


/**
 * Interface for generating silk data from objects
 *
 * @author leo
 */
trait SilkWriter {
  type self = this.type

  def write[A](obj: A): self

  def write[A](name: String, obj: A): self
  def writeSchema(schema: ObjectSchema)

  def context[A](context: A)(body: self => Unit): self = {
    pushContext(context)
    try {
      body(this)
    }
    finally popContext
    this
  }

  protected def pushContext[A](obj: A): Unit

  protected def popContext: Unit
}

/**
 * SilkWriter
 */
trait SilkObjectWriter extends SilkWriter {

  // Primitive type io
  def writeInt(name: String, v: Int): self
  def writeShort(name: String, v: Short): self
  def writeLong(name: String, v: Long): self
  def writeFloat(name: String, v: Float): self
  def writeDouble(name: String, v: Double): self
  def writeBoolean(name: String, v: Boolean): self
  def writeByte(name: String, v: Byte): self
  def writeChar(name: String, v: Char): self
  def writeString(name: String, s: String): self
  def writeValue[A](name: String, v: A): self

  // Collection type io
  def writeSeq[A](name: String, seq: Seq[A]): self
  def writeArray[A](name: String, array: Array[A]): self
  def writeMap[A, B](name: String, map: Map[A, B]): self
  def writeSet[A](name: String, set: Set[A]): self

}

