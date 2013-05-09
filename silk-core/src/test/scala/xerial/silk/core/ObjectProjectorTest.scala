//--------------------------------------
//
// ObjectProjectorTest.scala
// Since: 2013/04/03 5:29 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec

object ObjectProjectorTest {
  case class A(id:Int, name:String, score:Int)
}

/**
 * @author Taro L. Saito
 */
class ObjectProjectorTest extends SilkSpec {

  import ObjectProjectorTest._

  "ObjectProjector" should {
    "create a projected class" in {

      //val cl = ObjectProjector.projectedClass(classOf[A], Seq("id", "name"))
      val a = A(1, "leo", 100)
      val ap = ObjectProjector.project(a, Seq("id", "name"))

      ap.id shouldBe 1
      ap.name shouldBe "leo"
      ap.score shouldBe 0

    }
  }

}