

package xerial.silk.cluster

import org.scalatest._
import xerial.silk.util.SilkSpec
import xerial.larray.LArray
import java.io.File
import xerial.core.io.IOUtil


class ScatterTestMultiJvm1 extends SilkSpec {

  "scatter" should {

    "distribute data" in {
      info("hello")

      val l = LArray.of[Int](10)
      import xerial.larray._
      import xerial.silk._

      StandaloneCluster.withCluster {

        for(i <- 0 Until l.size) l(i) = i.toInt

        Thread.sleep(10000)
        // TODO
        //val s = l.toSilk.map(_*2)
        //s.execute()
      }
    }
  }
}

class ScatterTestMultiJvm2 extends SilkSpec {

  "scatter" should {
    "distribute data" in {

      Thread.sleep(5000)

      val zk = new ZkEnsembleHost(StandaloneCluster.lh)
      val tmpDir : File = IOUtil.createTempDir(new File("target"), "silk-tmp2").getAbsoluteFile
      withConfig(Config(silkHome=tmpDir, silkClientPort = IOUtil.randomPort)) {
        info("starting a new Silk client")
        val client = SilkClient.startClient(Host("localhost", localhost.address), zk.connectAddress)


      }
    }
  }

}