package xerial.silk.text.writer

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

import xerial.silk.util.{Logger, SilkFlatSpec}
import xerial.silk.writer.SilkTextWriter


//--------------------------------------
//
// SilkWriterTest.scala
// Since: 2012/01/17 13:52
//
//--------------------------------------

/**
 * @author leo
 */
class SilkWriterTest extends SilkFlatSpec {

  import SilkWriterTest._

  class A(val id: Int, val name: String)

  "SilkTextWriter" should "output class contents" in {
    val a = new A(1, "leo")
    val silk = SilkTextWriter.toSilk(a)
    debug {
      silk
    }
  }

  "SilkTextWriter" should "output array value" in {
    val a = Array[Int](0, 1, 10, 50)
    val silk = SilkTextWriter.toSilk(a)
    debug {
      silk
    }
  }

  "int" should "not be boxed" in {
    val a = Array[Int](1, 2, 3)
    debug {
      "Array[Int] class:%s" % (a.getClass)
    }

    debug {
      "a(0):%s" % a(0).getClass
    }

    def wrap(obj: Any) = {
      debug {
        "wrap Any:%s" % obj.getClass
      }
    }
    def wrapVal(obj: AnyVal) = {
      debug {
        "wrap AnyVal:%s" % obj.getClass
      }
    }

    wrap(a(0))
    wrapVal(a(0))

    def wrapGeneric[A](obj: A)(implicit m: Manifest[A]) = {
      debug {
        "wrap generic:%s" % obj.getClass
      }
      debug {
        "manifest %s" % m.toString
      }
    }

    def wrapSpecialized[@specialized(Int) A](obj: A) = {
      debug {
        "wrap specialized:%s" % obj.getClass
      }
    }

    //debug { "cast to Int:%s" % classOf[Int].cast(a(0)).getClass}
    wrapGeneric(a(0))
    wrapSpecialized(a(0))

    val b = new B(a(0))
    val b2 = new B[Int](a(0))

    val cm = ClassManifest.fromClass(a.getClass.getComponentType)
    debug {
      "array manifest %s" % cm.arrayManifest
    }

    val e = a.asInstanceOf[Array[_]]
    debug {
      e(0).getClass.getName
    }

    val f = a.asInstanceOf[Array[Int]]
    debug {
      f(0).getClass.getName
    }


    def writeVal(v: AnyVal) = {
      debug {
        "writeVal:" + v.getClass
      }
    }
    writeVal(a(0))

    def writeInt(v: Int) = {
      debug {
        "writeInt:" + v.getClass
      }
    }

    writeInt(a(0))

  }

  "SilkWriter" should "output named objects" in {
    // name:(object data)
    // (no name):(object data)


  }


}

object SilkWriterTest extends Logger {

  class B[@specialized(Int) T](val v: T) {
    debug("specialized class " + v.getClass)
  }

}