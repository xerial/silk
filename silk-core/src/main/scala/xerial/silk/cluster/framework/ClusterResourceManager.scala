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
import xerial.silk.{SilkException, TimeOut}
import scala.collection.mutable
import xerial.silk.util.ThreadUtil.ThreadManager

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


case class WaitingRequest(r:ResourceRequest, var numTrial: Int = 0, future:SilkFuture[Either[Exception, NodeResource]]) {
  def nodeName = r.nodeName
}

sealed trait ResourceRequestResult {
  def isAvailable : Boolean
}
case class ResourceAvailable(r:NodeResource) extends ResourceRequestResult {
  def isAvailable = true
}
case object ResourceNotAvailable extends ResourceRequestResult {
  def isAvailable = false
}

class ResourceManagerImpl extends ResourceManagerAPI with Guard with Logger {
  private val resourceTable = collection.mutable.Map[String, NodeResource]()
  private val nodeTable = collection.mutable.Map[String, Node]()
  private var lruOfNodes = List[String]()
  private val update = newCondition
  private val queueIsNotEmpty = newCondition

  private val requestQueue = mutable.Queue.empty[WaitingRequest]

  private val maxTrial = 3

  private val tm = new ThreadManager(1, useDaemonThread = true)
  tm.submit {
    // Request handler
    trace("Started resource request handler")
    guard {
      while(true) {
        if(requestQueue.isEmpty)
          queueIsNotEmpty.await()

        val req = requestQueue.head

        debug(s"Processing request: $req")

        def findResource : ResourceRequestResult = {
          if(req.nodeName.isDefined && req.numTrial < maxTrial) {
            // Try to acquire a resource from a target node
            val targetNode = req.nodeName.get
            resourceTable.get(targetNode) match {
              case Some(r) if r.isEnoughFor(req.r) => ResourceAvailable(r.adjustFor(req.r))
              case None =>
                ResourceNotAvailable
            }
          }
          else
            findFromAllResources
        }

        def findFromAllResources = {
          // Find an available resource from the all nodes
          lruOfNodes.map(resourceTable(_)).find(_.isEnoughFor(req.r)) match {
            case Some(resource) =>
              ResourceAvailable(resource.adjustFor(req.r))
            case None =>
              val lruNode = lruOfNodes.head
              warn(s"No enough resource is found for ${req.r}, pick an LRU node: $lruNode")
              ResourceAvailable(NodeResource(lruNode, 0, -1))
          }
        }

        // Callback to the sender
        findResource match {
          case ResourceAvailable(acquired) =>
            // Processed this request
            requestQueue.dequeue()
            val remaining = resourceTable(acquired.nodeName) - acquired
            resourceTable += remaining.nodeName -> remaining
            // TODO improve the LRU update performance
            lruOfNodes = lruOfNodes.filter(_ != acquired.nodeName) :+ acquired.nodeName
            req.future.set(Right(acquired))
          case ResourceNotAvailable =>
            val updated = update.await(10, TimeUnit.SECONDS)
            if(!updated) {
              req.numTrial += 1
              if(req.numTrial >= maxTrial) {
                 requestQueue.dequeue()
                 req.future.set(Left(new TimeOut("acquireResource")))
              }
            }
        }
      }
    }
  }


  /**
   * Acquire the specified amount of resources from some host. This operation is blocking until
   * the resource will be available
   * @return
   */
  def acquireResource(r:ResourceRequest): NodeResource = {
    val req = WaitingRequest(r, 0, new SilkFutureMultiThread[Either[Exception, NodeResource]]())

    guard {
      trace(s"Add request to queue: $r")
      requestQueue += req
      queueIsNotEmpty.signalAll()
    }

    req.future.get match {
      case Left(e) => throw e
      case Right(r) => r
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
    update.signalAll()
  }

  def releaseResource(r: NodeResource) : Unit = guard {

    resourceTable.get(r.nodeName) match {
      case Some(x) =>
        val remaining = x + r
        resourceTable += r.nodeName -> remaining
        // TODO: improve the LRU update performance
        lruOfNodes = (r.nodeName :: lruOfNodes.filter(_ != r.nodeName)).reverse
        debug(s"released: $r, remaining: $remaining, lruOrNodes: $lruOfNodes")
      case None =>
      // The node for the resource is already detached
    }
    update.signalAll()
  }


  def lostResourceOf(nodeName:String) : Unit = guard {
    trace(s"dropped: $nodeName")
    resourceTable.remove(nodeName)
    lruOfNodes = lruOfNodes.filter(_ != nodeName)
    update.signalAll()
  }

}

