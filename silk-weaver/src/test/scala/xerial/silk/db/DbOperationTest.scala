//--------------------------------------
//
// DbOperationTest.scala
// Since: 2013/10/23 4:20 PM
//
//--------------------------------------

package xerial.silk.db

import xerial.silk.util.SilkSpec
import xerial.silk.weaver.StandaloneCluster
import xerial.silk.Silk
import xerial.silk.weaver.StandaloneCluster.ClusterHandle

/**
 * @author Taro L. Saito
 */
class DbOperationTest extends SilkSpec {

  var handle : Option[ClusterHandle] = None
  before {
    handle = Some(StandaloneCluster.startTestCluster)
    Silk.init()
  }

  after {
    Silk.cleanUp
    handle map(_.stop)
  }

  import Silk._

  "DbOperation" should {

    "compute hash join" in {

      val a = Seq((1, "A"), (2, "B"), (3, "C")).toSilk
      val b = Seq((1, "apple"), (1, "armond"), (3, "cocoa")).toSilk

      val join = HashJoin(a, b, { x : (Int, String) => x._1}, { x : (Int, String) => x._1 } )
      val result = join.get
      debug(result.mkString(", "))
    }


  }


}