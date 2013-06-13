

package xerial.silk.cluster

import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.silk.cluster.SilkClient.OK
import xerial.silk._

class ScatterTestMultiJvm1 extends Cluster2Spec {

  "scatter" should {

    "distribute data" in {
      val l = LArray.of[Int](10)
      import xerial.larray._

      start {
        env =>

        // data on memory
        val sharedMemoryFile = new File(cluster.config.silkTmpDir, "sample.dat")
        val sharedMemory = LArray.mmap(sharedMemoryFile, 0, l.byteLength, MMapMode.READ_WRITE)
        l.copyTo(0, sharedMemory, 0, l.byteLength)

        // RegisterClassBox the data to the local DataServer
        env.clientActor ! SilkClient.RegisterFile(sharedMemoryFile)
        val dsPort = config.dataServerPort

        // Send file location to JVM2
        var index = 0L
        val blockSize = math.ceil(l.byteLength / 2.toDouble).toLong
        for(n <- hosts; remote <- SilkClient.remoteClient(n.host, n.clientPort)) {
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

  xerial.silk.cluster.configureLog4j

  "scatter" should {
    "distribute data" in {
      start { env =>


      }
    }
  }

}