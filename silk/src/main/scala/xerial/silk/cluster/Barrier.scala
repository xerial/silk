//--------------------------------------
//
// Barrier.scala
// Since: 2013/04/10 4:20 PM
//
//--------------------------------------

package xerial.silk.cluster

import java.util.concurrent.CyclicBarrier
import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
class Barrier(numThreads:Int) extends Logger {

  import scala.collection.JavaConversions._
  val barrier = new java.util.concurrent.ConcurrentHashMap[String, CyclicBarrier]()

  def enter(name:String) {
    val b : CyclicBarrier = synchronized {
      barrier.getOrElseUpdate(name, new CyclicBarrier(numThreads))
    }
    debug(f"[Thread-${Thread.currentThread.getId}] entering barrier: $name")
    b.await
  }

}