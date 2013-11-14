//--------------------------------------
//
// SilkMainTest.scala
// Since: 2012/11/15 5:56 PM
//
//--------------------------------------

package xerial.silk.weaver

import xerial.lens.cui.ClassOptionSchema
import xerial.lens.ObjectSchema
import xerial.silk.util.SilkSpec
import xerial.silk.{SilkUtil, Silk}


import xerial.silk.Silk._

class SilkSample {

  def in(n:Int) = (0 until n).toSilk
}


/**
 * @author leo
 */
class SilkMainTest extends SilkSpec {
  "SilkMain" should {
    "have version command" in {
      val cl = ClassOptionSchema(classOf[SilkMain])
      trace(s"SilkMain options: ${cl.options.mkString(", ")}")

      val schema = ObjectSchema(classOf[SilkMain])
      trace(s"SilkMain constructor: ${schema.constructor}")

      val out = captureOut {
        SilkMain.main("version")
      }
      debug(out)

      out should (include(SilkUtil.getVersion))
    }

    "display short message" in {
      val out = captureOut {
        SilkMain.main("")
      }

      out should (include("silk"))
      out should (include(SilkMain.DEFAULT_MESSAGE))
    }

    "pass command line args to eval" taggedAs("eval") in {

      SilkMain.main("eval SilkSample:in -n 10")

      SilkMain.main("eval xerial.silk.weaver.SilkSample:in -n 10")

    }


  }
}