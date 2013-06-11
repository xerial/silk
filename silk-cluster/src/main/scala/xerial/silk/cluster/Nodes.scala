//--------------------------------------
//
// Nodes.scala
// Since: 2013/06/11 22:39
//
//--------------------------------------

package xerial.silk.cluster

/**
 * @author Taro L. Saito
 */
trait Nodes extends xerial.silk.framework.Nodes {
  type Node = NodeInfo
  case class NodeInfo(name:String, address:String, clientPort:Int, dataServerPort:Int) extends NodeAPI
}

