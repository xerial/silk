//--------------------------------------
//
// TaskSchedulerTest.scala
// Since: 2013/10/22 4:22 PM
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec
import xerial.silk.Silk

/**
 * @author Taro L. Saito
 */
class TaskSchedulerTest extends SilkSpec {
  import Silk._

  "TaskScheduler" should {
    "find eligible nodes" in {

      val t = new TaskSchedulerComponent with SilkFramework {
        def scheduler = new TaskScheduler {}
      }

      val in = Seq(0, 1, 2, 3, 4, 5).toSilk
      val e = in.map(_+1).map(_*2).filter(_<3)

      t.scheduler.eval(e)


    }
  }
}