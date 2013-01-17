//--------------------------------------
//
// ClusterCommandTest.scala
// Since: 2012/12/19 11:55 AM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.{ThreadUtil, SilkSpec}
import java.io.{FileWriter, BufferedWriter, PrintWriter, File}
import xerial.core.io.IOUtil
import xerial.silk.SilkMain

/**
 * @author Taro L. Saito
 */
class ClusterCommandTest extends SilkSpec {

  xerial.silk.suppressLog4jwarning

  "ClusterCommand" should {
    "read zookeeper-ensemble file" in {
      val t = File.createTempFile("tmp-zookeeper-ensemble", "")
      t.deleteOnExit()
      val w = new PrintWriter(new BufferedWriter(new FileWriter(t)))
      val servers = for(i <- 0 until 3) yield
        "localhost:%d:%d".format(IOUtil.randomPort, IOUtil.randomPort)
      servers.foreach(w.println(_))
      w.flush
      w.close

      val serversInFile = ZooKeeper.readHostsFile(t.getPath).getOrElse(Seq.empty)
      serversInFile map (_.connectAddress) should be (servers)

      val isStarted = ZooKeeper.isAvailable(serversInFile)
      isStarted should be (false)
    }

    "run zkStart" taggedAs("zkStart") in {
      val t = ThreadUtil.newManager(2)
      t.submit {
        val ret = SilkMain.main("cluster zkStart -i 0 127.0.0.1:2888:3888")
        ret should be (0)
      }

      t.submit{
        Thread.sleep(2000)
        SilkMain.main("cluster zkStop")
      }

      val success = t.awaitTermination(maxAwait=10)
      success should be (true)
    }
  }
}
