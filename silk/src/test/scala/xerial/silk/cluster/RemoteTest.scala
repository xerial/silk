//--------------------------------------
//
// RemoteTest.scala
// Since: 2012/12/21 1:55 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.core.log.Logger


object RemoteTest extends Logger {
  def f = { () => info("hello world!") }

}

/**
 * @author Taro L. Saito
 */
class RemoteTest extends SilkSpec {
  "Remote" should {
    "run command" in {
      Remote.run(ClosureSerializer.serializeClosure(RemoteTest.f))
    }

    "run deserialized function of Nothing input" in {
      val out = captureErr {
        Remote.run(ClosureSerializer.serializeClosure(RemoteTest.f))
      }
      out should (include ("hello world!"))
    }


    "run Function0" in {
      val cl = xerial.silk.at(localhost){ info("hello") }
      info("class:%s", cl)
    }

    "report function0 class" in {
      var cl : Class[_] = null
      val out = captureOut {
        val l = LazyF0({ println("hello function0") })
        cl = l.functionClass
      }
      info("function0 class:%s", cl)
      out should (not include "hello function0")
    }

    "wrap closures" taggedAs("closure") in {
      val out = captureOut {
        ClosureSerializer.serializeClosure({ info("closure is evaluated"); println("hello function0") })
      }
      out should (not include "hello function0")
    }



  }
}