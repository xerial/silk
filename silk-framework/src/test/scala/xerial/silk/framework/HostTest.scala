//--------------------------------------
//
// HostTest.scala
// Since: 2013/08/24 14:14
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class HostTest extends SilkSpec {

  "Host" should {
    "parse host line" in {
      val h = Host.parseHostsLine("localhost")
      h should be ('defined)
      h.get.name shouldBe "localhost"
      val h2 = Host.parseHostsLine("localhost 192.168.1.1")
      h2 should be ('defined)
      h2.get.name shouldBe "localhost"
      h2.get.address shouldBe "192.168.1.1"
    }
  }

}