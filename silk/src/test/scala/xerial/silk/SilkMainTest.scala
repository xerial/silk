//--------------------------------------
//
// SilkMainTest.scala
// Since: 2012/11/15 5:56 PM
//
//--------------------------------------

package xerial.silk

import util.SilkSpec
import xerial.lens.cui.ClassOptionSchema
import xerial.lens.ObjectSchema

/**
 * @author leo
 */
class SilkMainTest extends SilkSpec {
  "SilkMain" should {
    "have version command" in {
      val cl = ClassOptionSchema(classOf[SilkMain])
      debug("SilkMain options: %s", cl.options.mkString(", "))

      val schema = ObjectSchema(classOf[SilkMain])
      debug("SilkMain constructor: %s", schema.constructor)

      val out = captureOut {
        SilkMain.main("version")
      }
      debug(out)
    }
  }
}