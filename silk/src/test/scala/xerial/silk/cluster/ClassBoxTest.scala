//--------------------------------------
//
// ClassBoxTest.scala
// Since: 2012/12/20 10:34 AM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class ClassBoxTest extends SilkSpec {

  "ClassBox" should {
    "enumerate entries in classpath" in {
      val cb = ClassBox.current
      debug("md5sum of classbox: %s", cb.md5sum)
      
    }
  }
}