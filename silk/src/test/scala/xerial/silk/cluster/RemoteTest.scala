//--------------------------------------
//
// RemoteTest.scala
// Since: 2012/12/21 1:55 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.silk.core.{LazyF0, SilkSerializer}
import xerial.core.log.Logger
import runtime.BoxedUnit


object RemoteTest extends Logger {
  def f = { () => info("hello world!") }

}

/**
 * @author Taro L. Saito
 */
class RemoteTest extends SilkSpec {
  "Remote" should {
    "run command" in {
      Remote.run(Thread.currentThread.getContextClassLoader, SilkSerializer.serializeClosure(RemoteTest.f))
    }

    "run deserialized function of Nothing input" in {
      val out = captureErr {
        Remote.run(Thread.currentThread.getContextClassLoader, SilkSerializer.serializeClosure(RemoteTest.f))
      }
      out should (include ("hello world!"))
    }

    "run Function0" in {
      val cl = xerial.silk.at2(localhost){ info("hello") }
      info("class:%s", cl)
      val cl2 = xerial.silk.at2(localhost){ () => info("hello") }
      info("class:%s", cl2)
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
        SilkSerializer.serializeClosure({ info("closure is evaluated"); println("hello function0") })
      }
      out should (not include "hello function0")
    }



  }
}