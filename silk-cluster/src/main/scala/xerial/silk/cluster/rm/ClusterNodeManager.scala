//--------------------------------------
//
// ClusterNodeManager.scala
// Since: 2013/06/13 12:30
//
//--------------------------------------

package xerial.silk.cluster.rm

import xerial.core.util.JavaProcess
import xerial.core.log.Logger
import scala.util.Random
import xerial.silk.cluster._
import xerial.silk.framework.{SilkFramework, SilkSerializer, Node, NodeManagerComponent}
import xerial.silk.framework.Node

/**
 * @author Taro L. Saito
 */
trait ClusterNodeManager extends NodeManagerComponent
{
  self: SilkClusterFramework with ZooKeeperService =>

  type NodeManager = NodeManagerImpl
  val nodeManager : NodeManager = new NodeManagerImpl

  import SilkSerializer._

  def clientIsActive(nodeName: String) = {
    nodeManager.getNode(nodeName) map { n =>
      val jps = JavaProcess.list
      jps.exists(ps => ps.id == n.pid && config.cluster.silkClientPort == n.clientPort)
    } getOrElse false
  }

  class NodeManagerImpl extends NodeManagerAPI with Logger {
    val nodePath = config.zk.clusterNodePath

    def getNode(nodeName:String) : Option[Node] = {
      zk.get(nodePath / nodeName).map(_.deserializeAs[Node])
    }

    def numNodes = {
      val registeredNodeNames = zk.ls(nodePath)
      registeredNodeNames.length
    }

    def randomNode : Node = {
      val registeredNodeNames = zk.ls(nodePath)
      val i = Random.nextInt(registeredNodeNames.length)
      val target = registeredNodeNames(i)
      val n = zk.read(nodePath / target)
      n.deserializeAs[Node]
    }

    def nodes = {
      val registeredNodeNames = zk.ls(nodePath)
      val nodes = for(node <- registeredNodeNames) yield {
        val n = zk.read(nodePath / node)
        n.deserializeAs[Node]
      }
      nodes
    }

    def addNode(n: Node) {
      info(s"Register a new node: $n")
      zk.set(nodePath / n.name, n.serialize)
    }

    def removeNode(nodeName: String) {
      if(!zk.isClosed)
        zk.remove(nodePath / nodeName)
    }
  }
}


