//--------------------------------------
//
// SigarUtilTest.scala
// Since: 2013/12/30 21:14
//
//--------------------------------------

package xerial.silk.sigar

import org.scalatest.FunSuite
import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class SigarUtilTest extends SilkSpec {

  "SigarUtil" should {
    "load native lib" in {
      val s = SigarUtil.sigar
      val cpu = s.getCpuInfoList.map(_.toString).mkString(", ")
      info(cpu)
    }

    "show load average" in {
      val loadAve = SigarUtil.loadAverage
      info(s"load average: ${loadAve.mkString(", ")}")
    }

    "show free memory" in {
      val free = SigarUtil.freeMemory
      info(s"free memory: $free")
    }

  }
}