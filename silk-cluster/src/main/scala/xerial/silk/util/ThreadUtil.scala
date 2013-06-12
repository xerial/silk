//--------------------------------------
//
// ThreadUtil.scala
// Since: 2012/12/18 2:01 PM
//
//--------------------------------------

package xerial.silk.util

import java.util.concurrent.{ThreadFactory, TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicInteger
import java.lang.String

/**
 * Helper methods for writing threaded test codes
 * @author Taro L. Saito
 */
object ThreadUtil {

  def newManager(numThreads:Int) = new ThreadManager(numThreads)

  private final val poolNumber: AtomicInteger = new AtomicInteger(1)

  class DaemonThreadFactory extends ThreadFactory {
    private val s: SecurityManager = System.getSecurityManager
    private val group: ThreadGroup = if ((s != null)) s.getThreadGroup else Thread.currentThread.getThreadGroup
    private val threadNumber: AtomicInteger = new AtomicInteger(1)
    private val namePrefix: String = "pool-" + poolNumber.getAndIncrement + "-thread-"

    def newThread(r: Runnable): Thread = {
      val t: Thread = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
      t.setDaemon(true)
      if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
      return t
    }
  }

  class ThreadManager(numThreads:Int, useDaemonThread:Boolean=false) {

    val manager = if(useDaemonThread)
      Executors.newFixedThreadPool(numThreads, new DaemonThreadFactory)
    else
      Executors.newFixedThreadPool(numThreads)

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

    def shutdownNow = {
      manager.shutdownNow
    }

  }


}