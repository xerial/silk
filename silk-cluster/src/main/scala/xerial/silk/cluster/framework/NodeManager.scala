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

/**
 * @author Taro L. Saito
 */
trait ClusterNodeManager extends ClusterManagerComponent {
  self:SilkFramework with ZooKeeperService with ResourceManagerComponent =>

  type NodeManager = NodeManagerImpl
  val nodeManager : NodeManager

  import xerial.silk.cluster.config

  class NodeManagerImpl extends NodeManagerAPI {

    val nodePath = config.zk.clusterNodePath

    def nodes = {
      val registeredNodeNames = zk.ls(nodePath)
      val nodes = for(node <- registeredNodeNames) yield {
        val n = zk.read(nodePath / node)
        SilkMini.deserializeObj(n).asInstanceOf[Node]
      }
      nodes
    }

    def addNode(n: Node) {
      zk.set(nodePath / n.name, SilkMini.serializeObj(n))
    }

    def removeNode(n: Node) {
      zk.remove(nodePath / n.name)
    }
  }
}

trait ClusterResourceManager extends ResourceManagerComponent {
  self:SilkFramework with ClusterManagerComponent =>

  type ResourceManager = ResourceManagerImpl
  val resourceManager = new ResourceManagerImpl

  class ResourceManagerImpl extends ResourceManagerAPI with Guard {

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
        throw new TimeOut("acqurieResource")
      else {
        val remaining = resourceTable(acquired.nodeName) - acquired
        resourceTable += remaining.nodeName -> remaining
        // TODO improve the LRU update performance
        lruOfNodes = lruOfNodes.filter(_ != acquired.nodeName) :+ acquired.nodeName
        acquired
      }
    }

    def releaseResource(r: NodeResource) : Unit = guard {
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


    def lostResource(nodeName:String) : Unit = guard {
      resourceTable.remove(nodeName)
      lruOfNodes = lruOfNodes.filter(_ == nodeName)
      update.signalAll()
    }

  }

}
