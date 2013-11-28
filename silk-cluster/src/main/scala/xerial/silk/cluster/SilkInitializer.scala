//--------------------------------------
//
// SilkInitializer.scala
// Since: 2013/11/11 5:29 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.Guard
import xerial.core.log.Logger
import xerial.silk.framework._
import xerial.silk.{Silk, SilkException, SilkEnv}
import java.util.UUID
import xerial.core.io.IOUtil
import xerial.silk.cluster.store.DataServer
import xerial.silk.core.IDUtil


object SilkInitializer {

}

class SilkInitializer(cfg:SilkClusterFramework#Config, zkConnectString:String) extends Guard with Logger with IDUtil { self =>
  private val isReady = newCondition
  private var started = false
  private var inShutdownPhase = false
  private val toTerminate = newCondition

  private[silk] var framework : SilkClusterFramework = null

  import SilkCluster._
  import xerial.silk._

  private val t = new Thread(new Runnable {
    def run() {
      self.info("Initializing Silk")

      if(!ZooKeeper.isAvailable(zkConnectString)) {
        warn(s"No ZooKeeper is found at $zkConnectString")
        guard { isReady.signalAll() }
        return
      }

      val f = new SilkClusterFramework {
        override val config = cfg
        override lazy val zkConnectString = self.zkConnectString
      }

      // Use a temporary node name to distinguish settings from SilkClient running in this node.
      val hostname = s"localhost-${UUID.randomUUID.prefix}"
      setLocalHost(Host(hostname, localhost.address))

      for{
        zkc <- ZooKeeper.zkClient(f.config.zk, zkConnectString)
        as <- ActorService(localhost.address, IOUtil.randomPort)
        ds <- DataServer(f.config.home.silkTmpDir, IOUtil.randomPort, f.config.cluster.dataServerKeepAlive)
      } yield {
        framework = new SilkService {
          val config = cfg
          override lazy val zkConnectString = self.zkConnectString
          val zk = zkc
          val dataServer = ds
          val actorSystem = as
        }


        //env = new SilkEnvImpl(zk, actorSystem, dataServer)
        //Silk.setEnv(env)
        started = true

        guard {
          isReady.signalAll()
        }

        guard {
          while(!inShutdownPhase) {
            toTerminate.await()
          }
          started = false
        }
      }
    }
  })
  t.setDaemon(true)

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() = { self.stop }
  }))


  private[silk] def start = {
    t.start
    guard {
      isReady.await()
    }
    if(!started)
      throw SilkException.error("Failed to initialize Silk")
    framework
  }

  def stop {
    guard {
      if(started) {
        self.info("Terminating Silk")
        inShutdownPhase = true
        toTerminate.signalAll()
      }
    }
  }



}
