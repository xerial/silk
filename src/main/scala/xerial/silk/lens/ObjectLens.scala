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

package xerial.silk.lens

import xerial.silk.util.{Logger, TypeUtil}
import java.io.OutputStream


//--------------------------------------
//
// ScalaObjectLenss.scala
// Since: 2012/01/17 10:54
//
//--------------------------------------

/**
 * Object -> Silk
 *
 * @author leo
 */
object ObjectLens extends Logger {

  def toSilk(in:Any) : String = {
    val schema = ObjectSchema(in.getClass)


    ""
  }

  def updateWithSilk[T: ClassManifest](obj:T, silk:String) : T = {
    val cl = classManifest[T].erasure
    debug { "class type: " + cl  }

    TypeUtil.newInstance(cl).asInstanceOf[T]
  }

  def createFromSilk[T](cl:Class[T], silk:String) : T = {
    TypeUtil.newInstance(cl).asInstanceOf[T]
  }


}

trait SilkConverter[A] {

  def toSilk(obj:A) : Array[Byte]

  def fromSilk(bytes:Array[Byte]) : A

}


trait ObjectLens {

  def toSilk[A](obj:A)(implicit conv:SilkConverter[A]) : Array[Byte]

  def fromSilk[A <: ClassManifest[A]](in:Array[Byte])(implicit conv:SilkConverter[A]) : A

}




