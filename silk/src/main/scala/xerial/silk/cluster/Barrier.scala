//--------------------------------------
//
// Barrier.scala
// Since: 2013/04/10 4:20 PM
//
//--------------------------------------

package xerial.silk.cluster

import java.util.concurrent.{TimeoutException, TimeUnit, CyclicBarrier}
import xerial.core.log.Logger
import java.io.{FileFilter, File}
import xerial.larray.{LArray, MMapMode, MappedLByteArray}
import scala.collection.concurrent.TrieMap
import xerial.core.util.StopWatch

/**
 * @author Taro L. Saito
 */
class Barrier(numThreads:Int, timeoutMillis:Long = TimeUnit.SECONDS.toMillis(30)) extends Logger {

  val barrier = new TrieMap[String, CyclicBarrier]()

  def enter(name:String) {
    val b : CyclicBarrier = synchronized {
      barrier.getOrElseUpdate(name, new CyclicBarrier(numThreads))
    }
    trace(f"[Thread-${Thread.currentThread.getId}] entering barrier: $name")
    b.await(timeoutMillis, TimeUnit.MILLISECONDS)
  }

}

trait ProcessBarrier extends Logger {
  def numProcesses:Int
  def processID:Int
  def lockFolder:File = new File("target/lock")

  import scala.concurrent.duration._

  protected def timeout = 20.seconds

  def cleanup = {
    val lockFile = Option(lockFolder.listFiles(new FileFilter {
      def accept(pathname: File) = pathname.getName.endsWith(".barrier")
    })) getOrElse(Array.empty[File])

    while(lockFile.exists(_.exists())) {
      lockFile.filter(_.exists()) map (_.delete())
      Thread.sleep(50)
    }
  }

  def enterBarrier(name:String) {
    info(s"[Process: ${processID}] entering barrier: $name")

    if(!lockFolder.exists)
      lockFolder.mkdirs
    val lockFile = new File(lockFolder, s"$name.barrier")
    lockFile.deleteOnExit()
    val l = LArray.mmap(lockFile, 0, numProcesses, MMapMode.READ_WRITE)
    l(processID-1) = 1.toByte
    l.flush

    def timeoutError(message:String) {
      l(processID-1) = -1.toByte
      l.flush
      l.close()
      throw new TimeoutException(message)
    }

    val s = new StopWatch
    s.reset
    while(!l.forall(_ == 1.toByte)) {
      if(s.getElapsedTime >= timeout.toSeconds)
        timeoutError(s"Timeout on barrier $name")
      else if(l.find(_ == -1.toByte).isDefined)
        timeoutError(s"Another process has timed out on barrier $name")

      Thread.sleep(10)
    }
    info(s"exit barrier: $name")
    l.close
  }



}