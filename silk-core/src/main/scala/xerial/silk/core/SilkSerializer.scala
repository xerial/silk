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
// SilkSerializer.scala
// Since: 2012/12/05 3:44 PM
//
//--------------------------------------

package xerial.silk.core

import java.io._
import xerial.core.log.Logger
import xerial.silk.Silk


/**
 * @author Taro L. Saito
 */
object SilkSerializer extends Logger {

  implicit class SerializeHelper(n:Any) {
    def serialize : Array[Byte] = serializeObj(n)
  }

  implicit class DeserializeHelper(b:Array[Byte]) {
    def deserialize : AnyRef = deserializeObj[AnyRef](b)
    def deserializeAs[A] : A = deserializeObj[A](b)
  }


  def serializeObj(v:Any) = {
    val buf = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(buf)
    oos.writeObject(v)
    oos.close()
    val ba = buf.toByteArray
    ba
  }

  class ObjectDeserializer(in:InputStream, classLoader:ClassLoader=Thread.currentThread().getContextClassLoader) extends ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) = {
      Class.forName(desc.getName, false, classLoader)
    }
  }

  def deserializeObj[A](b:Array[Byte]): A = deserializeObj(new ByteArrayInputStream(b))

  def deserializeObj[A](in:InputStream): A = {
    val ois = new ObjectDeserializer(in)
    val op = ois.readObject().asInstanceOf[A]
    ois.close()
    op
  }

  def serializeFunc[A, B, C](f: (A, B) => C) = serializeObj(f)
  def deserializeFunc[A, B, C](b:Array[Byte]) : (A, B) => C = deserializeObj(b)
  def serializeOp[A](op: Silk[A]) = serializeObj(op)
  def deserializeOp(b: Array[Byte]): Silk[_] = deserializeObj(b)



}