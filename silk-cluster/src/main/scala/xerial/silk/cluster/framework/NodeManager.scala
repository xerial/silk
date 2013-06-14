//--------------------------------------
//
// NodeManager.scala
// Since: 2013/06/13 12:30
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.silk.mini.SilkMini
import xerial.silk.framework.Node
import java.util.concurrent.TimeUnit
import xerial.silk.{TimeOut, SilkException}
import xerial.silk.util.ThreadUtil.ThreadManager
import com.netflix.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.EventType
import com.netflix.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import com.netflix.curator.framework.CuratorFramework
import xerial.silk.cluster.ZkPath
import xerial.core.util.JavaProcess
import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
trait ClusterNodeManager extends ClusterManagerComponent {
  self: ZooKeeperService =>

  type NodeManager = NodeManagerImpl
  val nodeManager : NodeManager = new NodeManagerImpl

  import xerial.silk.cluster.config

  def clientIsActive(nodeName: String) = {
    nodeManager.getNode(nodeName) map { n =>
      val jps = JavaProcess.list
      jps.exists(ps => ps.id == n.pid && config.silkClientPort == n.clientPort)
    } getOrElse false
  }

  class NodeManagerImpl extends NodeManagerAPI with Logger {
    val nodePath = config.zk.clusterNodePath

    def getNode(nodeName:String) : Option[Node] = {
      zk.get(nodePath / nodeName).map {
        SilkMini.deserializeObj(_).asInstanceOf[Node]
      }
    }


    def nodes = {
      val registeredNodeNames = zk.ls(nodePath)
      val nodes = for(node <- registeredNodeNames) yield {
        val n = zk.read(nodePath / node)
        SilkMini.deserializeObj(n).asInstanceOf[Node]
      }
      nodes
    }

    def addNode(n: Node) {
      info(s"Register a new node: $n")
      zk.set(nodePath / n.name, SilkMini.serializeObj(n))
    }

    def removeNode(nodeName: String) {
      if(!zk.isClosed)
        zk.remove(nodePath / nodeName)
    }
  }
}

/**
 * An implementation of ResourceManager that runs on SilkMaster
 */
trait ClusterResourceManager extends ResourceManagerComponent with LifeCycle {
  self: ZooKeeperService =>

  type ResourceManager = ResourceManagerImpl
  val resourceManager = new ResourceManagerImpl

  private val resourceMonitor = new ResourceMonitor

  class ResourceMonitor extends PathChildrenCacheListener with Logger {

    import xerial.silk.cluster.config
    val nodePath = config.zk.clusterNodePath
    val pathMonitor = new PathChildrenCache(zk.curatorFramework, nodePath.path, true)
    pathMonitor.getListenable.addListener(this)

    def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {

      def updatedNode = SilkMini.deserializeObj(event.getData.getData).asInstanceOf[Node]

      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val newNode = updatedNode
          debug(s"Node attached: $newNode")
          resourceManager.addResource(newNode.resource)
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val nodeName = ZkPath(event.getData.getPath).leaf
          debug(s"Node detached: $nodeName")
          resourceManager.lostResourceOf(nodeName)
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          info(s"child node data is updated: ${updatedNode}")
        case PathChildrenCacheEvent.Type.CONNECTION_LOST =>
        case other => warn(s"unhandled event type: $other")
      }
    }

    def start = {
      trace("Start ResourceMonitor")
      pathMonitor.start

    }
    def close = {
      trace("Stop ResourceMonitor")
      pathMonitor.close()
    }
  }

  abstract override def startup {
    super.startup
    resourceMonitor.start
  }
  abstract override def teardown {
    resourceMonitor.close
    super.teardown
  }

  class ResourceManagerImpl extends ResourceManagerAPI with Guard with Logger {
    val resourceTable = collection.mutable.Map[String, NodeResource]()
    var lruOfNodes = List[String]()
    val update = newCondition

    /**
     * Acquire the specified amount of resources from some host. This operation is blocking until
     * the resource will be available
     * @return
     */
    def acquireResource(r:ResourceRequest): NodeResource = guard {
      @volatile var acquired: NodeResource = null

      if(r.nodeName.isDefined) {
        val targetNode = r.nodeName.get
        // Try to acquire a resource from a target node
        val maxTrial = 3
        var numTrial = 0

        while(acquired == null && numTrial < maxTrial) {
          resourceTable.get(targetNode) match {
            case Some(res) if res.isEnoughFor(r) =>
              acquired = res.adjustFor(r)
            case None =>
              numTrial += 1
              update.await(10, TimeUnit.SECONDS)
          }
        }
      }

      val maxTrial = 3
      var numTrial = 0

      // Find resource from all nodes
      while(acquired == null && numTrial <= maxTrial) {
        lruOfNodes.map(resourceTable(_)).find(_.isEnoughFor(r)) match {
          case Some(resource) =>
            acquired = resource.adjustFor(r)
          case None =>
            numTrial += 1
            update.await(10, TimeUnit.SECONDS)
        }
      }

      if(acquired == null)
        throw new TimeOut("acquireResource")
      else {
        val remaining = resourceTable(acquired.nodeName) - acquired
        resourceTable += remaining.nodeName -> remaining
        // TODO improve the LRU update performance
        lruOfNodes = lruOfNodes.filter(_ != acquired.nodeName) :+ acquired.nodeName
        acquired
      }
    }

    def addResource(r:NodeResource) : Unit = guard {
      trace(s"add: $r")
      resourceTable.get(r.nodeName) match {
        case Some(x) =>
          resourceTable += r.nodeName -> (x + r)
        case None =>
          resourceTable += r.nodeName -> r
      }
      // TODO: improve the LRU update performance
      lruOfNodes = r.nodeName :: lruOfNodes.filter(_ != r.nodeName)
      update.signalAll()
    }

    def releaseResource(r: NodeResource) : Unit = guard {
      trace(s"released: $r")
      resourceTable.get(r.nodeName) match {
        case Some(x) =>
          resourceTable += r.nodeName -> (x + r)
          // TODO: improve the LRU update performance
          lruOfNodes = r.nodeName :: lruOfNodes.filter(_ != r.nodeName)
        case None =>
          // The node for the resource is already detached
        }
      update.signalAll()
    }


    def lostResourceOf(nodeName:String) : Unit = guard {
      trace(s"dropped: $nodeName")
      resourceTable.remove(nodeName)
      lruOfNodes = lruOfNodes.filter(_ == nodeName)
      update.signalAll()
    }

  }

}
