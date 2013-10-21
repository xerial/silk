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
      debug(f)

      val optimizer = new DeforestationOptimizer
      val fo = optimizer.optimize(f)

      debug(fo)

    }

  }
}