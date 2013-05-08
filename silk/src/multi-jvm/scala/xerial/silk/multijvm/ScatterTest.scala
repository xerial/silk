

package xerial.silk.multijvm

import xerial.silk.util.SilkSpec
import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.core.io.IOUtil
import xerial.silk.cluster.ZooKeeper._
import scala.Some
import xerial.silk.cluster.SilkClient.{OK, ClientInfo}
import xerial.silk.cluster
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.silk.cluster._
import xerial.silk.flow.Silk

class ScatterTestMultiJvm1 extends Cluster2Spec {

  "scatter" should {

    "distribute data" in {
      val l = LArray.of[Int](10)
      import xerial.larray._

      start { client =>

        // data on memory
        val sharedMemoryFile = new File(cluster.config.silkTmpDir, "sample.dat")
        val sharedMemory = LArray.mmap(sharedMemoryFile, 0, l.byteLength, MMapMode.READ_WRITE)
        l.copyTo(0, sharedMemory, 0, l.byteLength)

        // RegisterClassBox the data to the local DataServer
        client ! SilkClient.RegisterFile(sharedMemoryFile)
        val dsPort = config.dataServerPort

        // Send file location to JVM2
        var index = 0L
        val blockSize = math.ceil(l.byteLength / 2.toDouble).toLong
        for(n <- _root_.Silk.hosts; remote <- SilkClient.remoteClient(n.host, n.port)) {
          val offset = blockSize * index
          val size = math.min(blockSize, l.byteLength - offset)

          val reply = remote ? SilkClient.DownloadDataFrom(StandaloneCluster.lh, dsPort, sharedMemoryFile, offset, size)
          reply match {
            case OK =>
            case _ => fail(s"failed to download data from port:$dsPort")
          }
          index += 1
        }
      }
    }
  }
}

class ScatterTestMultiJvm2 extends Cluster2Spec {

  xerial.silk.configureLog4j

  "scatter" should {
    "distribute data" in {
      start { client =>


      }
    }
  }

}