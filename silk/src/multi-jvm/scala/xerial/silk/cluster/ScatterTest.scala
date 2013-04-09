

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.core.io.IOUtil
import xerial.silk.cluster.ZooKeeper._
import scala.Some
import xerial.silk.cluster.SilkClient.ClientInfo
import xerial.silk.core.Silk
import xerial.silk.cluster

class ScatterTestMultiJvm1 extends SilkSpec {

  "scatter" should {

    "distribute data" in {
      info("hello")

      val l = LArray.of[Int](10)
      import xerial.larray._

      StandaloneCluster.withCluster {

        for(i <- 0 Until l.size) l(i) = i.toInt

        Thread.sleep(3000)

        val nodeList = Silk.hosts
        warn(s"node list: ${nodeList.mkString(", ")}")

        for(n <- nodeList; client <- SilkClient.remoteClient(n.host, n.port)) {
          warn(s"send a ping to $n")
          client ! SilkClient.Status
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

class ScatterTestMultiJvm2 extends SilkSpec {

  "scatter" should {
    "distribute data" in {

      Thread.sleep(2000)

      val zk = new ZkEnsembleHost(StandaloneCluster.lh)
      val tmpDir : File = IOUtil.createTempDir(new File("target"), "silk-tmp2").getAbsoluteFile
      withConfig(Config(silkHome=tmpDir, silkClientPort = IOUtil.randomPort, dataServerPort = IOUtil.randomPort)) {

        SilkClient.startClient(Host("localhost2", "127.0.0.1"), zk.connectAddress)

        for(client <- SilkClient.localClient) {
          warn("in loop")

          // retrieve file location from JVM1
          //val sharedMemory = LArray.mmap(sharedMemoryFile, 0, l.byteLength, MMapMode.READ_WRITE)



          Thread.sleep(3000)
        }
      }
    }
  }

}