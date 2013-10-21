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
      val g1 = ScheduleGraph(f)
      debug(g1)

      val optimizer = new DeforestationOptimizer
      val fo = optimizer.optimize(f)
      val go = ScheduleGraph(fo)
      debug(go)

      go.nodes.size should be < g1.nodes.size
    }

    "optimize map-map inside a tree" in {

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

    "optimize map-map-map" in {
      val in = Silk.newSilk(Seq(1, 2, 3))
      val f = in.map(_+1).map(_*2).map(_.toString)
      val optimizer = new DeforestationOptimizer
      val fo = optimizer.optimize(f)
      val go = ScheduleGraph(fo)
      debug(go)

    }

  }
}