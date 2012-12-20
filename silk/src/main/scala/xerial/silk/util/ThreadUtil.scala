//--------------------------------------
//
// ThreadUtil.scala
// Since: 2012/12/18 2:01 PM
//
//--------------------------------------

package xerial.silk.util

import java.util.concurrent.{TimeUnit, Executors}

/**
 * Helper methods for writing threaded test codes
 * @author Taro L. Saito
 */
object ThreadUtil {

  def newManager(numThreads:Int) = new ThreadManager(numThreads)

  class ThreadManager(numThreads:Int) {

    val manager = Executors.newFixedThreadPool(numThreads)

    def submit[U](f: => U) {
      manager.submit(new Runnable {
        def run {
          f
        }
      })
    }

    def awaitTermination(duration:Int=1, unit:TimeUnit=TimeUnit.SECONDS, maxAwait:Int = 10) : Boolean = {
      manager.shutdown
      var count = 0
      while(count < maxAwait && !manager.awaitTermination(1, TimeUnit.SECONDS)) {
        count += 1
      }
      val success = count < maxAwait
      if(!success)
        manager.shutdownNow
      success
    }

    /**
     * Await until all submitted tasks are finished
     */
    def join = {
      manager.shutdown
      while(!manager.awaitTermination(1, TimeUnit.SECONDS)) {}
    }

  }


}