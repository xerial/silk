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

  object BasicType extends Enumeration {
    val Boolean, Int, String, Float, Double, Long, Short, Byte, Char, File, Date, Other = Value
  }

  /**
   * Helper method to translate primitive types into BasicType enumerations
   */
  def basicType[T](cl: ClassManifest[T]): BasicType.Value = {
    cl match {
      case c if c == classManifest[String] => BasicType.String
      case c if (c == ClassManifest.Boolean || c == classManifest[java.lang.Boolean]) => BasicType.Boolean
      case c if (c == ClassManifest.Int || c == classManifest[java.lang.Integer]) => BasicType.Int
      case c if (c == ClassManifest.Float || c == classManifest[java.lang.Float]) => BasicType.Float
      case c if (c == ClassManifest.Double || c == classManifest[java.lang.Double]) => BasicType.Double
      case c if (c == ClassManifest.Long || c == classManifest[java.lang.Long]) => BasicType.Long
      case c if (c == ClassManifest.Short || c == classManifest[java.lang.Short]) => BasicType.Short
      case c if (c == ClassManifest.Byte || c == classManifest[java.lang.Byte]) => BasicType.Byte
      case c if (c == ClassManifest.Char || c == classManifest[java.lang.Character]) => BasicType.Char
      case c if c == classManifest[File] => BasicType.File
      case c if c == classManifest[Date] => BasicType.Date
      case _ => BasicType.Other
    }
  }


  def convertType[A](s: Any, targetType: Class[A]): A = {
    if (targetType.isAssignableFrom(s.getClass))
      s.asInstanceOf[A]
    else {
      convert(s.toString, targetType)
    }
  }

  def convert[A](s: String, targetType: ClassManifest[A]): A = {
    val v: Any = basicType(targetType) match {
      case BasicType.String => s
      case BasicType.Boolean => s.toBoolean
      case BasicType.Int => s.toInt
      case BasicType.Float => s.toFloat
      case BasicType.Double => s.toDouble
      case BasicType.Long => s.toLong
      case BasicType.Short => s.toShort
      case BasicType.Byte => s.toByte
      case BasicType.Char if (s.length == 1) => s(0)
      case BasicType.File => new File(s)
      case BasicType.Date => DateFormat.getDateInstance.parse(s)
      case _ => {
        throw new IllegalArgumentException("""Failed to convert "%s" to %s""".format(s, targetType.toString))
      }
    }
    v.asInstanceOf[A]
  }


  /**
   * update an element of the array. This method is useful when only the element type information of the array is available
   */
  def updateArray(array: Any, elementType: ClassManifest[_], i: Int, v: Any) {
    basicType(elementType) match {
      case BasicType.String => array.asInstanceOf[Array[String]].update(i, convertType(v, classOf[String]))
      case BasicType.Boolean => array.asInstanceOf[Array[Boolean]].update(i, convertType(v, classOf[Boolean]))
      case BasicType.Int => array.asInstanceOf[Array[Int]].update(i, convertType(v, classOf[Int]))
      case BasicType.Float => array.asInstanceOf[Array[Float]].update(i, convertType(v, classOf[Float]))
      case BasicType.Double => array.asInstanceOf[Array[Double]].update(i, convertType(v, classOf[Double]))
      case BasicType.Long => array.asInstanceOf[Array[Long]].update(i, convertType(v, classOf[Long]))
      case BasicType.Short => array.asInstanceOf[Array[Short]].update(i, convertType(v, classOf[Short]))
      case BasicType.Byte => array.asInstanceOf[Array[Byte]].update(i, convertType(v, classOf[Byte]))
      case BasicType.Char => array.asInstanceOf[Array[Char]].update(i, convertType(v, classOf[Char]))
      case BasicType.File => array.asInstanceOf[Array[File]].update(i, convertType(v, classOf[File]))
      case BasicType.Date => array.asInstanceOf[Array[Date]].update(i, convertType(v, classOf[Date]))
      case _ => {}
      throw new IllegalArgumentException("failed to update array")
    }
  }


  def updateField[A](obj: Any, f: Field, value: Any): Unit = {
    def getOrElse[T](default: => T) = {
      val e = f.get(obj)
      if (e == null)
        default
      else
        e.asInstanceOf[T]
    }

    val accessible = f.isAccessible
    try {
      if (!accessible)
        f.setAccessible(true)

      val ftype = f.getType
      if (ftype.isArray) {
        // array type
        val elementType = ClassManifest.fromClass(ftype.getComponentType)
        val prevArray: Array[_] = getOrElse(Array()).asInstanceOf[Array[_]]
        val newArray = elementType.newArray(prevArray.length + 1)
        for (i <- 0 until prevArray.length) {
          updateArray(newArray, elementType, i, prevArray(i))
        }
        updateArray(newArray, elementType, prevArray.length, value)
        f.set(obj, newArray)
      }
      else if (f.getType.isAssignableFrom(classOf[Seq[_]])) {
        val prevSeq = getOrElse[Seq[_]](Seq.empty)


      }
      else {
        f.set(obj, convertType(value, ftype))
      }
    }

    finally {
      if (!accessible)
        f.setAccessible(false)
    }

  }


}