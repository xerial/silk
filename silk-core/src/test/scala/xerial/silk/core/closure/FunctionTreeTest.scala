/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// FunctionTreeTest.scala
// Since: 2013/05/01 10:21 AM
//
//--------------------------------------

package xerial.silk.core.closure

import xerial.silk.core.SilkSpec

import scala.reflect.runtime.{universe=>ru}


object FunctionSet {

  def sayHello(v:Int) = for(i <- 0 until v) println("hello")

}


/**
 * @author Taro L. Saito
 */
class FunctionTreeTest extends SilkSpec {

  def twice(v:Int) : Int = v * 2
  def square(w:Int) : Int = w * w

  import ru._

  "FunctionTree" should {

    "retrieve function expr" taggedAs("rt") in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val result = for(v <- r) yield twice(v)
      debug(showRaw(result.tree))
      val f = result.functionCall
      f.valName shouldBe "v"
    }

    "retrieve nested map" in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val r1 = r.map(twice)
      trace(r1.tree)
      val fc1 = r1.functionCall
      fc1.valName shouldBe "v"

      val r2 = r1.map(square)
      trace(r2.tree)
      val fc2 = r2.functionCall
      fc2.valName shouldBe "w"
    }

    "find nested function call" in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val r1 = r.map(v => square(twice(v)))
      trace(r1.tree)
      val fc1 = r1.functionCall
      trace(fc1)
      val calledMethods = FunctionTree.collectMethodCall(fc1.body)
      trace(showRaw(fc1.body))

      trace("called methods:\n" + calledMethods.mkString("\n"))
      calledMethods.size shouldBe 1
      val m1 = calledMethods.head
      m1.cls shouldBe IdentRef("FunctionTreeTest")
      m1.methodName shouldBe "square"

      val nestedMethodCalls = FunctionTree.collectMethodCall(m1.body)
      nestedMethodCalls.size shouldBe 1
      val m2 = nestedMethodCalls.head
      m2.cls shouldBe IdentRef("FunctionTreeTest")
      m2.methodName shouldBe "twice"
    }

    "find inline function" in {
      val in = new SilkIntSeq(Seq(1, 2, 3))
      val m = for(a <- in) yield { a * 2 }
      trace(showRaw(m.tree))
      val mc = FunctionTree.collectMethodCall(m.tree)
      mc.size shouldBe 1
      val h = mc.head
      trace(h)
      h.cls shouldBe IdentRef("a")
      h.methodName shouldBe "$times"
    }

    "find object function call" in {
      val in = new SilkIntSeq(Seq(1, 2, 3))
      val r = for(a <- in) yield { FunctionSet.sayHello(a) }
      debug(showRaw(r.tree))
      val mc = FunctionTree.collectMethodCall(r.tree)
      mc.size shouldBe 1
      val h = mc.head
      trace(h)
      h.cls shouldBe IdentRef("FunctionSet")
      h.methodName shouldBe ("sayHello")
    }

    "find function calls in a program sequence" taggedAs("seq") in {

      val in = new SilkIntSeq(Seq(1, 2, 3))
      val r = for(a <- in) yield {
        val b = twice(a)
        FunctionSet.sayHello(b - 1)
      }
      debug(showRaw(r.tree))
      val mc = FunctionTree.collectMethodCall(r.tree)
      debug(mc)
      mc.size shouldBe 2
    }




  }



}