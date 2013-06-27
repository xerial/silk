//--------------------------------------
//
// NestedMapTest.scala
// Since: 2013/06/27 10:18 AM
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.Cluster3Spec
import xerial.silk.SilkEnv

/**
 * @author Taro L. Saito
 */
object NestedMapTest {
  def nestedCode = "NestedCode should be evaluated"
}

class NestedMapTestMultiJvm1 extends Cluster3Spec {
  NestedMapTest.nestedCode in {
    start { env=>
      SilkEnv.silk{ e =>

        val in = 0 until 10
        val in2 = Seq("a", "b", "c")

        val data = e.newSilk(in, 4)
        val anotherData = e.newSilk(in2, 2)

        val nested = data.map { x =>
          anotherData.map(y => (x, y))
        }

        val expected = in.map{ x => in2.map(y => (x, y))}
        info(s"expected result: $expected")
        val result = e.run(nested)
        info(s"nested result: $result")
      }
    }
  }

}


class NestedMapTestMultiJvm2 extends Cluster3Spec {
  NestedMapTest.nestedCode in {
    start { env => }
  }
}

class NestedMapTestMultiJvm3 extends Cluster3Spec {
  NestedMapTest.nestedCode in {
    start { env => }
  }
}