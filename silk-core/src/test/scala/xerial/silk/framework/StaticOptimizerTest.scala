//--------------------------------------
//
// StaticOptimizerTest.scala
// Since: 2013/10/21 9:34
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec
import xerial.silk.Silk

/**
 * @author Taro L. Saito
 */
class StaticOptimizerTest extends SilkSpec {

  "StaticOptimizer" should {

    "merge functions" in {

      val in = Silk.newSilk(Seq(1, 2, 3))
      val f = in.map(_+1).map(_*2)
      val f1 = ScheduleGraph(f)
      debug(f1)

      val optimizer = new DeforestationOptimizer
      val fo = optimizer.optimize(f)
      val f2 = ScheduleGraph(fo)
      debug(f2)

      f2.nodes.size should be < f1.nodes.size
    }

    "optimze map-map inside a tree" in {

      val in = Silk.newSilk(Seq(1, 2, 3))
      val f = in.map(_+1).map(_*2).filter(_<5)
      val g1 = ScheduleGraph(f)
      debug(g1)
      
      val optimizer = new DeforestationOptimizer
      val fo = optimizer.optimize(f)
      val g2 = ScheduleGraph(fo)
      debug(g2)

      g2.nodes.size should be < g1.nodes.size

    }

  }
}