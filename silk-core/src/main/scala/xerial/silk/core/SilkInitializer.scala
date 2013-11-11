//--------------------------------------
//
// SilkInitializer.scala
// Since: 2013/11/11 5:29 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.Guard
import xerial.core.log.Logger
import xerial.silk.framework._
import xerial.silk.{Silk, SilkException, SilkEnv}
import java.util.UUID
import xerial.core.io.IOUtil


object SilkInitializer {

}

class SilkInitializer(zkConnectString:String) extends Guard with Logger with IDUtil { self =>
  private val isReady = newCondition
  private var started = false
  private var inShutdownPhase = false
  private val toTerminate = newCondition

  private var env : SilkEnv = null

  import Silk._
  import xerial.silk._

  private val t = new Thread(new Runnable {
    def run() {
      self.info("Initializing Silk")

      if(!ZooKeeper.isAvailable(zkConnectString)) {
        warn(s"No ZooKeeper is found at $zkConnectString")
        guard { isReady.signalAll() }
        return
      }

      withConfig(Config.testConfig(zkConnectString)) {
        // Use a temporary node name to distinguish settings from SilkClient running in this node.
        val hostname = s"localhost-${UUID.randomUUID.prefix}"
        setLocalHost(Host(hostname, localhost.address))

        for{
          zk <- ZooKeeper.defaultZkClient
          actorSystem <- ActorService(localhost.address, IOUtil.randomPort)
          dataServer <- DataServer(IOUtil.randomPort, config.dataServerKeepAlive)
        } yield {
          env = new SilkEnvImpl(zk, actorSystem, dataServer)
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
    }
  })
  t.setDaemon(true)

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() = { self.stop }
  }))


  private[silk] def start : SilkEnv = {
    t.start
    guard {
      isReady.await()
    }
    if(!started)
      throw SilkException.error("Failed to initialize Silk")
    env
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
