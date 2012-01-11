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

  private val t_boolean = classOf[Boolean]
  private val t_int = classOf[Int]
  private val t_float = classOf[Float]
  private val t_long = classOf[Long]
  private val t_short = classOf[Short]
  private val t_double = classOf[Double]
  private val t_byte = classOf[Byte]
  private val t_char = classOf[Char]
  private val t_file = classOf[File]
  private val t_date = classOf[Date]
  private val t_str = ClassManifest.fromClass(classOf[String])

  //  def convert[A](s:String, targetType:Class[A]): A  = {
  //    convert (s, ClassManifest.fromClass(targetType))
  //  }

  implicit def toClassManifest[T](targetType: Class[T]): ClassManifest[T] = ClassManifest.fromClass(targetType)

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

}