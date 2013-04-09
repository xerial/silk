

package xerial.silk.cluster

import org.scalatest._
import xerial.silk.util.SilkSpec


class ScatterTestMultiJvm1 extends SilkSpec {

  "scatter" should {

    "distribute data" in {
      info("hello")
    }
  }
}

class ScatterTestMultiJvm2 extends SilkSpec {

  "scatter" should {
    "distribute data" in {
      info("hello")
    }
  }

}