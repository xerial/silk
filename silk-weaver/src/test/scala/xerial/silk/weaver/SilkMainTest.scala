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


  }
}