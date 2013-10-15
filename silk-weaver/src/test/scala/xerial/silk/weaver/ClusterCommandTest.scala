//--------------------------------------
//
// ClusterCommandTest.scala
// Since: 2012/12/19 11:55 AM
//
//--------------------------------------

package xerial.silk.weaver

import xerial.silk.util.{ThreadUtil, SilkSpec}
import java.io.{FileWriter, BufferedWriter, PrintWriter, File}
import xerial.core.io.IOUtil
import xerial.silk.cluster._

/**
 * @author Taro L. Saito
 */
class ClusterCommandTest extends SilkSpec {

  "ClusterCommand" should {
    "read zookeeper-ensemble file" in {
      val t = File.createTempFile("tmp-zookeeper-ensemble", "", new File("target"))
      t.deleteOnExit()
      val w = new PrintWriter(new BufferedWriter(new FileWriter(t)))
      val servers = for(i <- 0 until 3) yield
        "localhost:%d:%d".format(IOUtil.randomPort, IOUtil.randomPort)
      servers.foreach(w.println(_))
      w.flush
      w.close

      val serversInFile = ZooKeeper.readHostsFile(t.getPath).getOrElse(Seq.empty)
      val connectAddress = (serversInFile map (_.serverAddress)).toArray
      connectAddress should be (servers.toArray)

      val isStarted = ZooKeeper.isAvailable(serversInFile)
      isStarted should be (false)
    }

    "run zkStart" taggedAs("zkStart") in {
      val b = new Barrier(2)

      val t = ThreadUtil.newManager(2)

      val tmp = File.createTempFile("tmp-zookeeper-ensemble", "", new File("target"))
      tmp.deleteOnExit()

      withConfig(
        Config(
          silkClientPort = IOUtil.randomPort,
          dataServerPort = IOUtil.randomPort,
          zk = ZkConfig(
            clientPort=IOUtil.randomPort,
            quorumPort = IOUtil.randomPort,
            leaderElectionPort = IOUtil.randomPort
          ))) {

        val zkAddr = s"127.0.0.1:${config.zk.quorumPort}:${config.zk.leaderElectionPort}"

        t.submit {
          b.enter("start")
          val ret = SilkMain.main(s"cluster zkStart -i 0 $zkAddr -p ${config.zk.clientPort} --home=${tmp}")
          b.enter("terminated")
          ret should be (0)
        }

        t.submit{
          b.enter("start")
          Thread.sleep(10000)
          SilkMain.main(s"cluster zkStop 127.0.0.1 -p ${config.zk.clientPort}")
          b.enter("terminated")
        }
      }


      val success = t.awaitTermination(maxAwait=30)
      success should be (true)
    }
  }
}
