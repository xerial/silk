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
import java.{lang => jl}
import java.lang.reflect.{ParameterizedType, Field}
import java.util.Date

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
    trace {
      """convert "%s" to %s""" format(v.toString, cl)
    }
    convert(v.toString, cl) must be(v)
  }

  def convAll[T <: Any](cl: Class[T], v: List[T]): Unit = {
    v.foreach(conv(_))
  }

  "TypeUtil" can {
    "convert strings into AnyVals" in {

      conv(true)
      conv(34)
      conv("hello world")

      convAll(classOf[Int], List(1, 100, 2000, 342, -134, 43, 43))
      convAll(classOf[Boolean], List(true, false))
      convAll(classOf[Double], List(1.0, 1.43, 1.0e-10, 4, -43.34e-14))
      convAll(classOf[Float], List(1.0f, 1.43f, 1.0e-10f, 4f, 3.134f, -1234f, -34.34e-3f))
      convAll(classOf[Long], List(1L, 100L, 2000L, 342L, -134L, 43L, 43L))

    }


    "convert strings to Java primitive types" in {
      val m = ClassManifest.fromClass(java.lang.Boolean.TYPE)

      conv(java.lang.Boolean.TRUE)
      conv(java.lang.Boolean.FALSE)
      conv(new java.lang.Integer(1))

      conv(new java.lang.Long(3L))

    }



    "set primitive type fields" in {

      class A {
        var i = 0
        var b = false
        var s = "hello"
        var f = 0.2f
        private var d = 0.01

        def getD = d

        var file: File = new File("sample.txt")
      }

      val a = new A
      def update(param: String, value: Any) {
        val f = a.getClass.getDeclaredField(param)
        setField(a, f, value)
      }

      update("i", 10)
      update("b", "true")
      update("s", "hello world")
      update("f", 0.1234f)
      update("d", 0.134)
      update("file", "helloworld.txt")

      a.i must be(10)
      a.b must be(true)
      a.s must be("hello world")
      a.f must be(0.1234f)
      a.getD must be(0.134)
      a.file.getName must be("helloworld.txt")

    }


    def getField(obj: Any, name: String): Field = {
      obj.getClass.getDeclaredField(name)
    }

    "set enumeration values" in {

      object Fruit extends Enumeration {
        val Apple, Banana = Value
      }
      import Fruit._
      class E {
        var fruit = Apple
      }

      basicTypeOf(Apple.getClass) should be(BasicType.Enum)

      val e = new E
      setField(e, getField(e, "fruit"), "Banana")
      e.fruit must be(Banana)

      setField(e, getField(e, "fruit"), "apple") // Use lowercase
      e.fruit must be(Apple)
    }

    "support updates of Option[T]" in {
      class B {
        var opt: Option[String] = None
      }

      val b = new B
      val f = getField(b, "opt")
      isOption(f.getType) should be(true)
      setField(b, f, "hello world")

      b.opt.isDefined must be(true)
      b.opt.get must be("hello world")

      debug(b.opt)
    }

    "support updates of Option[Integer]" in {
      class C {
        // Option[Int] cannot be used since Int is a primitive type and will be erased
        // as Option<java.lang.Object>
        var num: Option[jl.Integer] = None
      }
      val c = new C
      val f = getField(c, "num")
      setField(c, f, "1345")

      //val t = getTypeParameters(f)
      //t(0) should be (classOf[Int])
      debug(c.num)

      c.num.isDefined must be(true)
      c.num.get must be(1345)
    }


    "look up parameters in constructors" in {
      class Opt(val i: Option[Int]) {
      }

      val o = new Opt(Some(3))
      o.i match {
        case None =>
        case Some(x) => debug {
          "It's an integer:" + x
        }
      }

      val field = o.getClass.getDeclaredField("i")

      //getType(field.getGenericType)


      if (field.getType == classOf[Option[_]]) {
        val optionFieldType = field.getGenericType.asInstanceOf[ParameterizedType].getActualTypeArguments()(0)
        debug {
          optionFieldType.toString
        }
      }

    }

    import TypeUtilTest._


    "detect classes that can create new instances" in {

      val l = List(classOf[Int], classOf[String],
        classOf[jl.Integer], classOf[Boolean], classOf[Float], classOf[Double],
        classOf[Char], classOf[Byte], classOf[Long], classOf[Short]
      )

      l.foreach {
        c =>
          trace("type is %s".format(c))
          canInstantiate(c) must be(true)
      }

      canInstantiate(classOf[A]) must be(true)
    }

    "find int type in Tuple" in {
      trace {
        getTypeParameters(classOf[T].getDeclaredField("t"))(0)
      }
    }

    "detect primitive array type" in {
      val a = Array[Int](0, 1, 4)
      TypeUtil.isArray(a.getClass) must be(true)
      TypeUtil.isSeq(a.getClass) must be(false)

      class A(id: Int, name: String)
      val b = new A(1, "leo").asInstanceOf[Array[_]]
      TypeUtil.isArray(classOf[A]) must be(false)
    }
  }

  "TypeUtil" when {
    "it creates zero values" can {
      "support primitive types" in {
        zero(classOf[Int]) must be(0)
        zero(classOf[jl.Integer]) must be(new jl.Integer(0))
        zero(classOf[String]) must be("")
        zero(classOf[Boolean]) must be(true)
        zero(classOf[Float]) must be(0f)
        zero(classOf[Double]) must be(0.0)
        zero(classOf[Char]) must be(0.toChar)
      }

      "support arrays" in {
        val a = zero(classOf[Array[Int]])
        a.getClass must be(classOf[Array[Int]])
        for (p <- TypeUtil.scalaPrimitiveTypes) {
          val arrayType = p.newArray(0).getClass
          val z = zero(arrayType)
          z.getClass must be(arrayType)
        }
      }

      "support maps" in {
        val m = zero(classOf[Map[Int, String]])
        TypeUtil.isMap(m.getClass) must be (true)
      }

      "support seqs" in {
        val z = zero(classOf[Seq[Int]])
        TypeUtil.isSeq(z.getClass) must be (true)
      }
      
      "support tuple" in {
        val t = (1, 2, "a")
        trace { "tuple type: " + t.getClass }
        val z = zero(t.getClass)
        trace { "created tuple: " + z.toString }
        TypeUtil.isTuple(z.getClass) must be (true)
      }
      
      "support option" in {
        val o = zero(classOf[Option[_]])
        TypeUtil.isOption(o.getClass) must be (true)
        o must be (None)
      }

      import TypeUtilTest._
      "support simple class A(val id:Int=1)" in {
        val a = zero(classOf[A])
        a.getClass must be (classOf[A])
        val newA = new A
        a.id must be (newA.id)
      }

      "support class that lacks some default values" in {
        val b = zero(classOf[B])
        debug { b }
        b.getClass must be (classOf[B])
        
        val newB = new B(r=0.345)
        b.f must  be (newB.f)
        b.opt must be (None)
      }

      "support option parameters in a class" in {
        val c = zero(classOf[C])
        c.getClass must be (classOf[C])
        c.opt must be (Some(true))
      }

      "avoid inner classes" in {
        class Sample(val i:Int=10)
        val s = zero(classOf[Sample])
        s should be (null)
      }

    }

    "it converts Class[_] into BasicType" should {
      import TimeMeasure._
      "improve the conversion speed" in {
        val r = 100
        val t = time("basic type conversion", repeat=1000) {
          block("naive", repeat=r) {
            for(p <- TypeUtil.scalaPrimitiveTypes)
              TypeUtil.toBasicType(p)
          }
          block("cached", repeat=r) {
            for(p <- TypeUtil.scalaPrimitiveTypes)
              basicTypeOf(p)
          }
        }
        t("cached") must be < (t("naive"))
      }

    }
  }

}

object TypeUtilTest {

  class A(val id: Int = 1)
  class B(val f:Float=0.1f, val r:Double, val opt:Option[Boolean] = None)
  class C(val opt:Option[Boolean] = Some(true))

  class T(val t: (Int, String), val a: Array[Int]) {
    def get: Int = t._1
  }

}
