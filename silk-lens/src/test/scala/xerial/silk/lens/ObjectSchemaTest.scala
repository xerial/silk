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

import xerial.silk.util.SilkSpec
import java.lang.reflect.ParameterizedType
import tools.scalap.scalax.rules.scalasig.{TypeRefType, MethodSymbol}

//--------------------------------------
//
// ObjectSchemaTest.scala
// Since: 2012/01/17 10:17
//
//--------------------------------------

object ObjectSchemaTest {

  class A(val id: Int, val name: String)

  class C(val flag: Option[Int])

}

class B(val id: Int, val flag: Option[Boolean])

/**
 * @author leo
 */
class ObjectSchemaTest extends SilkSpec {

  import ObjectSchemaTest._

  "ObjectSchema" should {
    "enumerate all fields" in {

      val s = new ObjectSchema(classOf[A])
      debug {
        s.toString
      }
      s.name must be(classOf[A].getSimpleName)
      s.fullName must be(classOf[A].getName)

      val attr = s.attributes
      attr.length must be(2)
      attr(0).name must be("id")
      attr(0).rawType must be(classOf[Int])
      attr(1).name must be("name")
      attr(1).rawType must be(classOf[String])

    }

    "lookup option fields" in {
      val b = new B(1, Some(true))

      val s = ObjectSchema(classOf[B])
      debug(s)
      val t = s.getAttribute("flag").rawType.getGenericInterfaces


      debug {
        classOf[B].getDeclaredField("flag").getGenericType.asInstanceOf[ParameterizedType].getActualTypeArguments()(0)
      }

    }

    "detect ScalaSignature" in {
      val sig = ObjectSchema.detectSignature(classOf[B])
      debug {
        "signature of B\n" + sig
      }

      val ss = sig.get.symbols
      val ms = ss.filter(sym => sym.isInstanceOf[MethodSymbol]).map(_.asInstanceOf[MethodSymbol])
      //debug { ms.mkString("\n") }

      val param: Seq[MethodSymbol] = ms.filter(_.isParam)
      val paramRef = param.map(_.symbolInfo.info)

      val paramType = paramRef.map(sig.get.parseEntry(_)).map {
        case x: TypeRefType => Some(x)
      }
      debug {
        paramType.map {
          x => (x.get.symbol, x.get.typeArgs)
        }.mkString("\n")
      }



      //      val sig2 = ObjectSchema.detectSignature(classOf[C])
      //      debug { "signature of C\n" + sig2 }
      //
      //      val sig3 = ObjectSchema.detectSignature(Class.forName("xerial.silk.lens.ObjectSchemaTest"))
      //      debug { "signature of object\n" + sig3 }
      //

    }
  }

  trait ClassFixture {
    val cg = classOf[SampleGlobalClass]
    val co = classOf[ScalaClassLensTest.SampleObjectClass]
    val params = Array(
      ("id", classOf[Int]),
      ("flag", classOf[Option[Int]]),
      ("list", classOf[Array[String]]),
      ("map", classOf[Map[String, Float]]))
  }

  "ScalaClassLens" should {

    "find class definition containing ScalaSignature" in {
      new ClassFixture {
        val s1 = ScalaClassLens.detectSignature(cg)
        val s2 = ScalaClassLens.detectSignature(co)

        s1 should be('defined)
        s2 should be('defined)
      }
    }

    "find parameters defined in global classes" in {
      new ClassFixture {
        val p = ScalaClassLens.findParameters(cg)

        debug { p.mkString(", ") }

        p.size must be (4)

        for(((name, t), i) <- params.zipWithIndex) {
          p(i).name must be (name)
          when("type is " + t)
          p(i).rawType.isAssignableFrom(t)
        }
      }
    }

    "find parameters defined in object classes" in {
      new ClassFixture {
        val p = ScalaClassLens.findParameters(co)
        debug { p.mkString(", ") }

        p.size must be (4)

        for(((name, t), i) <- params.zipWithIndex) {
          p(i).name must be (name)
          when("type is " + t)
          p(i).rawType.isAssignableFrom(t)
        }
      }
    }

    "find root constructor" in {
      val c1 = ScalaClassLens.getConstructor(classOf[ScalaClassLensTest.SampleObjectClass])
      debug { c1 }
      val c2 = ScalaClassLens.getConstructor(classOf[SampleGlobalClass])
      debug { c2 }
    }


  }

}

class SampleGlobalClass(val id: Int, val flag: Option[Int], val list: Array[String], val map: Map[String, Float])

object ScalaClassLensTest {

  class SampleObjectClass(val id: Int, val flag: Option[Int], val list: Array[String], val map: Map[String, Float])
  class Dummy(val name:String)
}
