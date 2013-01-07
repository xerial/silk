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
import org.objectweb.asm.{Opcodes, ClassVisitor, MethodVisitor, ClassReader}
import collection.mutable.{Set, Map}
import xerial.core.log.Logger
import java.lang.reflect.Constructor
import xerial.lens.TypeUtil




/**
 * @author Taro L. Saito
 */
object SilkSerializer extends Logger {

  def serialize(silk:AnyRef) : Array[Byte] = {
    val cl = silk.getClass
    debug("serializing %s", cl)
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(silk)
    o.flush()
    o.close
    b.close
    b.toByteArray
  }

  def deserializeAny(b:Array[Byte]) : AnyRef = {
    val in = new ObjectDeserializer(new ByteArrayInputStream(b))
    val ret = in.readObject()
    in.close()
    ret
  }

  class ObjectDeserializer(in:InputStream) extends ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) = {
      Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
    }
  }


}