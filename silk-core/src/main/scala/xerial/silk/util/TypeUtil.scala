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
import collection.mutable.ArrayBuffer
import java.lang.reflect.{AccessibleObject, Method, Field}

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
    val Boolean, Int, String, Float, Double, Long, Short, Byte, Char, File, Date, Enum, Other = Value
  }

  def isEnumeration[T](cl: ClassManifest[T]): Boolean = {
    cl <:< classManifest[Enumeration$Value]
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
      case c if c <:< classManifest[Enumeration$Value] => BasicType.Enum
      case _ => BasicType.Other
    }
  }


  /**
   * Convert the input value into the target type
   */
  def convertType[A](value: Any, targetType: Class[A]): A = {
    if (targetType.isAssignableFrom(value.getClass))
      value.asInstanceOf[A]
    else {
      convert(value, targetType)
    }
  }

  /**
   * Convert the input value into the target type
   */
  def convert[A](value: Any, targetType: ClassManifest[A]): A = {
    convertToBasicType[A](value, basicType(targetType))
  }
  /**
   * Convert the input value into the target type
   */
  def convertToBasicType[A](value: Any, targetType: BasicType.Value): A = {
    val s = value.toString
    val v: Any = targetType match {
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
      case BasicType.Enum => throw new IllegalArgumentException("""Scala Enumeration (%s) cannot be set with convert(). Use updateField instead: value:%s""" format(targetType.toString, s))
      case _ => throw new IllegalArgumentException("""Failed to convert "%s" to %s""".format(s, targetType.toString))
    }
    v.asInstanceOf[A]
  }
  
  

  /**
   * update an element of the array. This method is useful when only the element type information of the array is available
   */
  def updateArray(array: Any, elementType: ClassManifest[_], i: Int, v: Any) {
    val bt = basicType(elementType)
    bt match {
      case BasicType.String => array.asInstanceOf[Array[String]].update(i, convertToBasicType[String](v, bt))
      case BasicType.Boolean => array.asInstanceOf[Array[Boolean]].update(i, convertToBasicType[Boolean](v, bt))
      case BasicType.Int => array.asInstanceOf[Array[Int]].update(i, convertToBasicType[Int](v, bt))
      case BasicType.Float => array.asInstanceOf[Array[Float]].update(i, convertToBasicType[Float](v, bt))
      case BasicType.Double => array.asInstanceOf[Array[Double]].update(i, convertToBasicType[Double](v, bt))
      case BasicType.Long => array.asInstanceOf[Array[Long]].update(i, convertToBasicType[Long](v, bt))
      case BasicType.Short => array.asInstanceOf[Array[Short]].update(i, convertToBasicType[Short](v, bt))
      case BasicType.Byte => array.asInstanceOf[Array[Byte]].update(i, convertToBasicType[Byte](v, bt))
      case BasicType.Char => array.asInstanceOf[Array[Char]].update(i, convertToBasicType[Char](v, bt))
      case BasicType.File => array.asInstanceOf[Array[File]].update(i, convertToBasicType[File](v, bt))
      case BasicType.Date => array.asInstanceOf[Array[Date]].update(i, convertToBasicType[Date](v, bt))
      case _ => {}
      throw new IllegalArgumentException("failed to update array")
    }
  }

  def updateEnumField(obj: Any, f: Field, enumValue: Any): Unit = {
    val cl = ClassManifest.fromClass(f.getType)
    val outer = cl.erasure.getDeclaredField("scala$Enumeration$$outerEnum")
    access(outer) {
      val prevEnumObj = readField[Enumeration$Value](obj, f)
      val enclosingEnumType: Enumeration = outer.get(prevEnumObj).asInstanceOf[Enumeration]
      val name = enumValue.toString.toLowerCase
      enclosingEnumType.values.find(_.toString.toLowerCase == name) match {
        case None => warn { "unknown enum value %s".format(enumValue)}
        case Some(ev) => access(f) { f.set(obj, ev) }
      }
    }
  }

  private def access[A <: AccessibleObject, B](f: A)(body: => B): B = {
    val accessible = f.isAccessible
    try {
      if (!accessible)
        f.setAccessible(true)
      body
    }
    finally {
      if (!accessible)
        f.setAccessible(false)
    }
  }

  def readField[A](obj: Any, f: Field): A = {
    access(f) {
      f.get(obj).asInstanceOf[A]
    }
  }


  def updateField(obj: Any, f: Field, value: Any): Unit = {
    def getOrElse[T](default: => T) = {
      val e = f.get(obj)
      if (e == null) default else e.asInstanceOf[T]
    }

    access(f) {
      val fieldType = f.getType
      if (fieldType.isArray) {
        // array type
        val elementType = ClassManifest.fromClass(fieldType.getComponentType)
        val prevArray: Array[_] = getOrElse(Array()).asInstanceOf[Array[_]]
        val newArray = elementType.newArray(prevArray.length + 1)
        // Copy the array contents to the new array
        for (i <- 0 until prevArray.length) {
          updateArray(newArray, elementType, i, prevArray(i))
        }
        // Add a new element to the tail
        updateArray(newArray, elementType, prevArray.length, value)
        // Update the field
        f.set(obj, newArray)
      }
      else if (isEnumeration(fieldType)) {
        updateEnumField(obj, f, value)
      }
      else if (fieldType.isAssignableFrom(classOf[Seq[_]])) {
        val prevSeq = getOrElse[Seq[_]](Seq.empty)
      }
      else {
        f.set(obj, convertType(value, fieldType))
      }
    }

  }


}