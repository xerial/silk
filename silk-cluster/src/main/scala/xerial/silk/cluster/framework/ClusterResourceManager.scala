package xerial.silk.cluster.framework

import xerial.silk.framework._
import com.netflix.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCache, PathChildrenCacheListener}
import xerial.core.log.Logger
import com.netflix.curator.framework.CuratorFramework
import xerial.silk.core.SilkSerializer
import xerial.silk.cluster.ZkPath
import xerial.silk.util.Guard
import java.util.concurrent.TimeUnit
import xerial.silk.framework.NodeResource
import xerial.silk.framework.ResourceRequest
import xerial.silk.framework.Node
import xerial.silk.TimeOut

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

      def updatedNode = SilkSerializer.deserializeObj(event.getData.getData).asInstanceOf[Node]

      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val newNode = updatedNode
          debug(s"Node attached: $newNode")
          resourceManager.addResource(newNode, newNode.resource)
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


}

class ResourceManagerImpl extends ResourceManagerAPI with Guard with Logger {
  val resourceTable = collection.mutable.Map[String, NodeResource]()
  val nodeTable = collection.mutable.Map[String, Node]()
  var lruOfNodes = List[String]()


  val update = newCondition

  /**
   * Acquire the specified amount of resources from some host. This operation is blocking until
   * the resource will be available
   * @return
   */
  def acquireResource(r:ResourceRequest): NodeResource = guard {
    @volatile var acquired: NodeResource = null

    val maxTrial = 3

    if(r.nodeName.isDefined) {
      val targetNode = r.nodeName.get
      // Try to acquire a resource from a target node
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

    var numTrial = 0

    // Find resource from all nodes
    while(acquired == null && numTrial < maxTrial) {
      lruOfNodes.map(resourceTable(_)).find(_.isEnoughFor(r)) match {
        case Some(resource) =>
          acquired = resource.adjustFor(r)
        case None =>
          numTrial += 1
          warn(s"No enough resource is found for $r")
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

  def getNodeRef(nodeName:String) : Option[NodeRef] = guard {
    nodeTable.get(nodeName).map(_.toRef)
  }

  def addResource(node:Node, r:NodeResource) : Unit = guard {
    trace(s"add: $r")
    nodeTable += node.name -> node
    resourceTable.get(r.nodeName) match {
      case Some(x) =>
        resourceTable += r.nodeName -> (x + r)
      case None =>
        resourceTable += r.nodeName -> r
    }
    // TODO: improve the LRU update performance
    lruOfNodes = r.nodeName :: lruOfNodes.filter(_ != r.nodeName)
    update.signal()
  }

  def releaseResource(r: NodeResource) : Unit = guard {

    resourceTable.get(r.nodeName) match {
      case Some(x) =>
        val remaining = x + r
        resourceTable += r.nodeName -> remaining
        // TODO: improve the LRU update performance
        lruOfNodes = r.nodeName :: lruOfNodes.filter(_ != r.nodeName)
        debug(s"released: $r, remaining: $remaining, lruOrNodes: $lruOfNodes")
      case None =>
      // The node for the resource is already detached
    }
    update.signal()
  }


  def lostResourceOf(nodeName:String) : Unit = guard {
    trace(s"dropped: $nodeName")
    resourceTable.remove(nodeName)
    lruOfNodes = lruOfNodes.filter(_ == nodeName)
    update.signal()
  }

}

