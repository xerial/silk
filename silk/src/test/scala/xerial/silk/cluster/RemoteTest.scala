//--------------------------------------
//
// RemoteTest.scala
// Since: 2012/12/21 1:55 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.silk.core.SilkSerializer
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

  }
}