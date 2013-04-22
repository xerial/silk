//--------------------------------------
//
// ProgramTree.scala
// Since: 2013/04/22 15:17
//
//--------------------------------------

package xerial.silk.core

import xerial.larray.LArray
import java.util.UUID

object ProgramTree {



  abstract class Node(val name:String)

  case class Data(uuid:UUID) extends Node(uuid.toString)

  class FuncNode(override val name:String, val input:Seq[Node], val output:Seq[Node])
    extends Node(name) {
    override def toString = s"FuncNode($name, input:[${input.mkString(", ")}], output:[${output.mkString(", ")}])"
  }
  class MapNode(override val name:String, input:Node, val output:Node)
    extends Node(name) {
    override def toString = s"MapNode($name, input:$input, outout:$output)"
  }
  case class GatherNode(override val input:Seq[Node], out:Node) extends FuncNode("gather", input, Seq(out))
  case class ScatterNode(in:Node, override val output:Seq[Node]) extends FuncNode("scatter", Seq(in), output)

}


/**
 * @author Taro L. Saito
 */
class ProgramTree {

}