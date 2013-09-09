//--------------------------------------
//
// SilkUtilTest.scala
// Since: 2013/07/26 10:18 AM
//
//--------------------------------------

package xerial.silk

import xerial.silk.util.SilkSpec
import xerial.silk.framework.Host

/**
 * @author Taro L. Saito
 */
class SilkUtilTest extends SilkSpec {

  "SilkUtil" should {

    "check the installation of Silk" in {
      val installed = SilkUtil.isSilkInstalled(Host("localhost", "127.0.0.1"))
      debug(s"silk installation: $installed")
    }

  }
}