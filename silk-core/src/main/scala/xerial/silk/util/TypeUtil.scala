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

package xerial.silk.util

import java.io.File
import java.util.Date
import java.text.DateFormat
import java.lang.Byte
import java.lang.reflect.Field
import collection.mutable.ArrayBuffer

//--------------------------------------
//
// TypeUtil.scala
// Since: 2012/01/11 12:46
//
//--------------------------------------

/**
 * @author leo
 */
object TypeUtil extends Logging {

  implicit def toClassManifest[T](targetType: Class[T]): ClassManifest[T] = ClassManifest.fromClass(targetType)

  def convertType[A](s:Any, targetType:Class[A]) : A = {
    if(targetType.isAssignableFrom(s.getClass))
      s.asInstanceOf[A]
    else {
      convert(s.toString, targetType)
    }
  }
  
  def convert[A](s: String, targetType: ClassManifest[A]): A = {
    val v: Any = targetType match {
      case c if c == classManifest[String] => s
      case c if (c == ClassManifest.Boolean || c == classManifest[java.lang.Boolean]) => s.toBoolean
      case c if (c == ClassManifest.Int || c == classManifest[java.lang.Integer]) => s.toInt
      case c if (c == ClassManifest.Float || c == classManifest[java.lang.Float]) => s.toFloat
      case c if (c == ClassManifest.Double || c == classManifest[java.lang.Double]) => s.toDouble
      case c if (c == ClassManifest.Long || c == classManifest[java.lang.Long]) => s.toLong
      case c if (c == ClassManifest.Short || c == classManifest[java.lang.Short]) => s.toShort
      case c if (c == ClassManifest.Byte || c == classManifest[java.lang.Byte]) => s.toByte
      case c if ((c == ClassManifest.Char || c == classManifest[java.lang.Character]) && s.length() == 1) => s(0)
      case c if c == classManifest[File] => new File(s)
      case c if c == classManifest[Date] => DateFormat.getDateInstance.parse(s)
      case _ => {
        throw new IllegalArgumentException("""Failed to convert "%s" to %s""".format(s, targetType.toString))
      }
    }
    v.asInstanceOf[A]
  }

  def updateAsIntArray(array:Any, i:Int, v:Any) {
    val converted : Int = convertType(v, classOf[Int])
    val a = array.asInstanceOf[Array[Int]]
    a(i) = converted
  }

  def updateArray(array:Any, elementType:ClassManifest[_], i:Int, v:Any) {
    elementType match {
      case c if c == classManifest[String] => array.asInstanceOf[Array[String]].update(i, convertType(v, classOf[String]))
      case _ => {}
        throw new IllegalArgumentException("failed to update array")
    }
  }

  
  def updateField[A <: Any](obj:A, f:Field, value:String) : Unit = {
    def getOrElse[T](default: =>T) = {
      val e = f.get(obj)
      if(e == null)
        default
      else
        e.asInstanceOf[T]
    }

    val accessible = f.isAccessible
    try {
      if(!accessible)
        f.setAccessible(true)

      val ftype = f.getType
      if(ftype.isArray) {
        val elementType = ClassManifest.fromClass(ftype.getComponentType)
        val prevArray : Array[_] = f.get(obj).asInstanceOf[Array[_]]
        val newArray = elementType.newArray(prevArray.length+1)
        for(i <- 0 until prevArray.length) {
          updateArray(newArray, elementType, i, prevArray(i))
        }
        updateArray(newArray, elementType, prevArray.length, value)
        f.set(obj, newArray)
      }
      else if (f.getType.isAssignableFrom(classOf[Seq[_]])){
        val prevSeq = getOrElse[Seq[_]](Seq.empty)

        
      }
      else {
        f.set(obj, convert(value, ClassManifest.fromClass(f.getType)))
      }
    }
      
    finally {
      if(!accessible)
        f.setAccessible(false)
    }

  }


}