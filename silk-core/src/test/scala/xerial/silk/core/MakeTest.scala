//--------------------------------------
//
// MakeTest.scala
// Since: 2013/12/16 4:43 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec
import xerial.silk.{Silk, SilkEnv}

object MakeTest {

  import Silk._

  trait MakeExample1 {

    def classList = {
      files("*")
    }

  }


}

import MakeTest._

/**
 * @author Taro L. Saito
 */
class MakeTest extends SilkSpec {

  implicit val env = SilkEnv.inMemoryEnv



  "Silk" should {

    "provide Makefile-like syntax" in {
      val w = Silk.workflow[MakeExample1]

      val clsList = w.classList.get
      debug(clsList.mkString(", "))


    }

  }

}