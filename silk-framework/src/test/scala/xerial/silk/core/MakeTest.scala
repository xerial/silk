//--------------------------------------
//
// MakeTest.scala
// Since: 2013/12/16 4:43 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec
import xerial.silk.{Weaver, Silk}

object MakeTest {

  import Silk._

  trait MakeExample1 {

    def javaList = file("silk-core/src/**/*.java")
    def srcDir = dir("silk-core/src/**/scala")

    def grep = for(f <- javaList; line <- open(f).lines if line.startsWith("import")) yield {
      line
    }

  }


}

import MakeTest._

/**
 * @author Taro L. Saito
 */
class MakeTest extends SilkSpec {

  implicit val env = Weaver.inMemoryWeaver


  "Silk" should {

    "provide Makefile-like syntax" in {
      val w = Silk.workflow[MakeExample1]

      val javaList = w.javaList.get
      debug(javaList.mkString(", "))
      javaList should not be empty

      val dirs = w.srcDir.get
      debug(dirs.mkString(", "))
      dirs should not be empty

      val grep = w.grep.get
      debug(grep.mkString(", "))
      grep should not be empty


      val g = CallGraph(w.grep)
      debug(g)
    }

  }

}