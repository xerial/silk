

package xerial.silk.cluster

import org.scalatest._
import xerial.silk.util.SilkSpec
import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.core.io.IOUtil
import xerial.silk.cluster.ZooKeeper._
import scala.Some
import xerial.silk.cluster.SilkClient.ClientInfo
import xerial.silk.core.Silk
import xerial.silk.cluster
import xerial.silk.util.ThreadUtil.ThreadManager


trait ScatterSpec extends SilkSpec with ProcessBarrier {

  def numProcesses = 2
  def processID = {
    val n = getClass.getSimpleName
    val p = "[0-9]".r
    val id = p.findAllIn(n).toSeq.last.toInt
    id
  }

  before {
    cleanup
  }

}


class ScatterTestMultiJvm1 extends ScatterSpec {


  "scatter" should {

    "distribute data" in {
      info("hello")

      val l = LArray.of[Int](10)
      import xerial.larray._
      import xerial.silk._

      StandaloneCluster.withCluster {

        val shared = LArray.mmap(new File("target/clientport"), 0, 12, MMapMode.READ_WRITE)
        shared.putInt(0, config.silkClientPort)
        shared.putInt(4, config.zk.clientPort)
        shared.putInt(8, config.dataServerPort)
        shared.flush
        shared.close

        enterBarrier("ready")

        for(i <- 0 Until l.size) l(i) = i.toInt

        enterBarrier("clientIsReady")

        val nodeList = Silk.hosts
        warn(s"node list: ${nodeList.mkString(", ")}")

        for(n <- nodeList; client <- SilkClient.remoteClient(n.host, n.port)) {
          warn(s"send a ping to $n")
          client ! SilkClient.ReportStatus
        }


        // data on memory
        val sharedMemoryFile = new File(cluster.config.silkTmpDir, "sample.dat")
        val sharedMemory = LArray.mmap(sharedMemoryFile, 0, l.byteLength, MMapMode.READ_WRITE)
        l.copyTo(0, sharedMemory, 0, l.byteLength)

        // Register the data to DataServer
        for(localClient <- SilkClient.remoteClient(StandaloneCluster.lh)) {
          localClient ! SilkClient.RegisterData(sharedMemoryFile)
        }

        Thread.sleep(1000)
        // Send file location to JVM2
        var index = 0L
        val blockSize = math.ceil(l.byteLength / 2.toDouble).toLong
        for(n <- nodeList; client <- SilkClient.remoteClient(n.host, n.port)) {
          val offset = blockSize * index
          val size = math.min(blockSize, l.byteLength - offset)
          // TODO resolve true DataServer port (by reading from ZooKeeper)
          client ! SilkClient.DownloadDataFrom(StandaloneCluster.lh, cluster.config.dataServerPort, sharedMemoryFile, offset, size)
          index += 1
        }

        // file data

        //val s = l.toSilk.map(_*2)
        //s.execute()

        Thread.sleep(3000)


        for(n <- nodeList; client <- SilkClient.remoteClient(n.host, n.port)) {
          client ! SilkClient.Terminate
        }
      }
    }
  }
}

class ScatterTestMultiJvm2 extends ScatterSpec {

  xerial.silk.configureLog4j

  "scatter" should {
    "distribute data" in {

      enterBarrier("ready")
      val shared = LArray.mmap(new File("target/clientport"), 0, 12, MMapMode.READ_ONLY)
      val clientPort = shared.getInt(0)
      val zkClientPort = shared.getInt(4)
      val dataServerPort = shared.getInt(8)
      shared.close

      val zk = new ZkEnsembleHost(StandaloneCluster.lh)
      val tmpDir : File = IOUtil.createTempDir(new File("target"), "silk-tmp2").getAbsoluteFile
      withConfig(Config(silkHome=tmpDir, silkClientPort = IOUtil.randomPort, dataServerPort = dataServerPort, zk=ZkConfig(clientPort=zkClientPort))) {

        val t = new ThreadManager(2)
        val b = new Barrier(2)
        t.submit {
          b.enter("startClient")
          SilkClient.startClient(Host("localhost2", "127.0.0.1"), s"127.0.0.1:$zkClientPort")
        }
        t.submit {
          b.enter("startClient")
          Thread.sleep(2000)
          enterBarrier("clientIsReady")
        }
        t.join
      }
    }
  }

}