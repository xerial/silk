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

  def loadFile = "DataLoader should load files"
}

import DataLoaderTest._

class DataLoaderTestMultiJvm1 extends Cluster3Spec {
  loadFile in {
    start { env=>
      SilkEnv.silk { e =>

        val data = e.newSilk(Seq(1, 2, 3, 4))
        val twice = data.map(x => x * 2)

        e.run(twice)
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