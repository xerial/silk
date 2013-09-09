//--------------------------------------
//
// DAGScheduleTest.scala
// Since: 2013/09/06 1:14 PM
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec
import xerial.silk.Silk

/**
 * @author Taro L. Saito
 */
class ScheduleGraphTest extends SilkSpec {

  before {
    Silk.setEnv(new InMemoryEnv)
  }


  "DAGSchedule" should {

    "create a graph from Silk" in {

      val in = Silk.newSilk(Seq(0, 1))
      val a = in.map(_ * 2)

      val s = ScheduleGraph(a)
      info(s)
    }


  }



}