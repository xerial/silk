//--------------------------------------
//
// NestedMapTest.scala
// Since: 2013/06/27 10:18 AM
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.Cluster3Spec
import xerial.silk.{Silk, SilkEnv}

/**
 * @author Taro L. Saito
 */
object NestedMapTest {
  def nestedCode = "NestedCode should be evaluated"

}

class NestedMapCode(implicit env:SilkEnv) extends Serializable {

  val data = Silk.newSilk(Seq(1, 2))
  val anotherData = Silk.scatter(Seq("a", "b", "c"), 2)

  def nested = data.map { x =>
    val a = anotherData.map(ai => (x, ai))
    a
  }

}


class NestedMapTestMultiJvm1 extends Cluster3Spec {
  NestedMapTest.nestedCode in {
    start { env=>
      implicit val e = env
      val w = new NestedMapCode

      info(s"op:${w.nested}")
      val result = w.nested.get
      info(s"nested result: $result")
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