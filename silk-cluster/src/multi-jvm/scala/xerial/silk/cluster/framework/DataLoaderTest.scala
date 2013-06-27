//--------------------------------------
//
// DataLoaderTest.scala
// Since: 2013/06/22 14:29
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.Cluster3Spec
import xerial.silk.SilkEnv

/**
 * @author Taro L. Saito
 */
object DataLoaderTest {

  def loadFile = "DataLoader should distribute in-memory data"
}

import DataLoaderTest._

class DataLoaderTestMultiJvm1 extends Cluster3Spec {
  loadFile in {
    start { env=>
      SilkEnv.silk { e =>

        val data = e.newSilk(0 until 1000, 4)
        val twice = data.map(x => x * 2)

        val result = e.run(twice)
        //info(s"result: $result")

        val result2 = e.run(twice)
        //info(s"run again: $result2")

        val toStr = twice.map(x => s"[${x.toString}]")
        val result3 = e.run(toStr)
        info(s"toStr: ${result3.size}")
      }
    }
  }
}

class DataLoaderTestMultiJvm2 extends Cluster3Spec {
  loadFile in {
    start { env=>

    }
  }

}


class DataLoaderTestMultiJvm3 extends Cluster3Spec {
  loadFile in {
    start { env=>

    }
  }

}


