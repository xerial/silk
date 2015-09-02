//--------------------------------------
//
// DAGScheduleTest.scala
// Since: 2013/09/06 1:14 PM
//
//--------------------------------------

package xerial.silk.framework.scheduler

import xerial.silk.core.{SilkSpec, Silk}
import Silk._

/**
 * @author Taro L. Saito
 */
class ScheduleGraphTest extends SilkSpec {

  "DAGSchedule" should {

    "create a graph from Silk" in {

      val in = Seq(0, 1).toSilk
      val a = in.map(_ * 2)

      val s = ScheduleGraph(a)
      info(s)
    }


  }



}