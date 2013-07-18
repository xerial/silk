//--------------------------------------
//
// DataLoaderTest.scala
// Since: 2013/06/22 14:29
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.{RangePartitioner, Cluster3Spec}
import xerial.silk.{Silk, SilkEnv}
import scala.util.Random

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
      val e = Silk.env

      // Scatter data to 3 nodes
      val N = 100000
      val data = e.scatter(for(i <- 0 until N) yield Random.nextInt(N), 3)

      val twice = data.map(_ * 2)

      val result = twice.get
      //info(s"result: $result")

      val result2 = twice.get
      //info(s"run again: $result2")

      val filtered = twice.filter(_ % 5 == 0)
      val reduced = filtered.reduce(math.max(_, _))
      val resultr = reduced.get
      info(s"reduce result: $resultr")

      val toStr = filtered.map(x => s"[${x.toString}]")
      val result3 = toStr.get
      info(s"toStr: ${result3.size}")


      val sorting = data.sorted(new RangePartitioner(3, data))
      val sorted = sorting.get
      info(s"sorted: size ${sorted.size}, ${sorted.take(100).mkString(", ")} ")
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



