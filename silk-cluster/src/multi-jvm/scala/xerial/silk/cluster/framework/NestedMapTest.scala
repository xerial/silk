//--------------------------------------
//
// NestedMapTest.scala
// Since: 2013/06/27 10:18 AM
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.Cluster3Spec
import xerial.silk.SilkEnv
import xerial.silk.framework.ops.{MapOp, CallGraph}
import xerial.silk.core.{ClosureSerializer, SilkSerializer}
import xerial.core.log.{LogLevel, LoggerFactory}

/**
 * @author Taro L. Saito
 */
object NestedMapTest {
  def nestedCode = "NestedCode should be evaluated"

}

class NestedMapCode(@transient e:SilkEnv) extends Serializable {

  val data = e.newSilk(Seq(1, 2))
  val anotherData = e.scatter(Seq("a", "b", "c"), 2)

  def nested = data.map { x =>
    val a = anotherData.map(ai => (x, ai))
    a
  }

}


class NestedMapTestMultiJvm1 extends Cluster3Spec {
  NestedMapTest.nestedCode in {
    start { env=>
      SilkEnv.silk{ e =>

        val w = new NestedMapCode(e)

        info(s"op:${w.nested}")
        val result = e.run(w.nested)
        info(s"nested result: $result")

//        val expected = in.map{ x => in2.map(y => (x, y))}
//        info(s"expected result: $expected")
//        val g = CallGraph(nested)
//        debug(s"call graph:\n$g")
//
//        val result = e.run(nested)

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