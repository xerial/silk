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

//--------------------------------------
//
// TypeUtilTest.scala
// Since: 2012/01/11 12:47
//
//--------------------------------------

/**
 * @author leo
 */
class TypeUtilTest extends SilkSpec {

  import TypeUtil._

  def conv[A <: Any](v: A): Unit = {
    val cl = v.getClass
    trace { """convert "%s" to %s""" format (v.toString, cl)}
    convert(v.toString, cl) must be (v)
  }
  def convAll[T <: Any](cl:Class[T], v: List[T]): Unit = {
    v.foreach (conv(_))
  }

  "TypeUtil" should "convert strings into AnyVals" in {

    conv(true)
    conv(34)
    conv("hello world")

    convAll(classOf[Int], List(1, 100, 2000, 342, -134, 43, 43))
    convAll(classOf[Boolean], List(true, false))
    convAll(classOf[Double], List(1.0, 1.43, 1.0e-10, 4, -43.34e-14))
    convAll(classOf[Float], List(1.0f, 1.43f, 1.0e-10f, 4f, 3.134f, -1234f, -34.34e-3f))
    convAll(classOf[Long], List(1L, 100L, 2000L, 342L, -134L, 43L, 43L))

  }

  "TypeUtil" should "convert Java primitive types" in {
    val m = ClassManifest.fromClass(java.lang.Boolean.TYPE)

    conv(java.lang.Boolean.TRUE)
    conv(java.lang.Boolean.FALSE)
    conv(new java.lang.Integer(1))
    
    conv(new java.lang.Long(3L))
  }
  
  "update field" should "increase the array size" in {

    class Sample {
      var input:Array[String] = Array.empty
    }
    val a = new Sample
    val f = a.getClass.getDeclaredField("input")

    updateField(a, f, "hello")
    a.input.size must be (1)
    a.input(0) must be ("hello")

    updateField(a, f, "world")
    a.input.size must be (2)
    a.input(0) must be ("hello")
    a.input(1) must be ("world")
  }


}


