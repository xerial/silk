//--------------------------------------
//
// CallGraph.scala
// Since: 2013/06/16 16:23
//
//--------------------------------------

package xerial.silk.core

import java.util.UUID
import xerial.lens.TypeUtil
import xerial.core.log.Logger

//object CallGraph extends Logger {
//
//  private class CallGraphBuilder {
//    var nodeTable = collection.mutable.Map[UUID, Silk[_]]()
//    var edgeTable = collection.mutable.Map[UUID, Set[UUID]]()
//
//    def containNode[A](n: Silk[A]): Boolean = {
//      nodeTable.contains(n.id)
//    }
//
//    def addNode[A](n: Silk[A]) {
//      nodeTable += n.id -> n
//    }
//
//    def addEdge[A, B](from: Silk[A], to: Silk[B]) {
//      addNode(from)
//      addNode(to)
//      val outNodes = edgeTable.getOrElseUpdate(from.id, Set.empty)
//      edgeTable += from.id -> (outNodes + to.id)
//    }
//
//    def result: CallGraph = {
//      new CallGraph(nodeTable.values.toSeq.sortBy(_.id), edgeTable.map {
//        case (i, lst) => i -> lst.toSeq
//      }.toMap)
//    }
//  }
//
//
//
//
//  import scala.reflect.runtime.{universe => ru}
//  import ru._
//
//  def createCallGraph[A](op: Silk[A]): CallGraph = {
//    val g = new CallGraphBuilder
//
//    def loop(node: Silk[_]) {
//      if (!g.containNode(node)) {
//        g.addNode(node)
//        // Add edge from input Silk data
//        for (in <- node.inputs if in != null) {
//          loop(in)
//          g.addEdge(in, node)
//        }
//      }
//    }
//    loop(op)
//    g.result
//  }
//
//  def isSilkType[A](cl: Class[A]): Boolean = classOf[Silk[_]].isAssignableFrom(cl)
//  private[silk] val mirror = ru.runtimeMirror(Thread.currentThread.getContextClassLoader)
//
//  def zero[A](cl: Class[A]) = cl match {
//    case f if isSilkType(f) =>
//      Silk.empty
//    case _ => TypeUtil.zero(cl)
//  }
//
//  def apply[A](op: Silk[A]) = createCallGraph(op)
//  def graphOf[A](op:Silk[A]) = createCallGraph(op)
//
//  /**
//   * Find a part of the silk tree
//   * @param silk
//   * @param targetName
//   * @tparam A
//   * @return
//   */
//  def collectTarget[A](silk:Silk[A], targetName:String) : Seq[Silk[_]] = {
//    info(s"Find target {$targetName} from $silk")
//    val g = graphOf(silk)
//    debug(s"call graph: $g")
//
//    g.nodes.filter(_.name == targetName)
//  }
//
//  def findTarget[A](silk:Silk[A], targetName:String) : Option[Silk[_]] = {
//    val matchingOps = collectTarget(silk, targetName)
//    matchingOps.size match {
//      case v if v > 1 => throw new IllegalArgumentException(s"more than one target is found for $targetName")
//      case other => matchingOps.headOption
//    }
//  }
//
//  def descendantsOf[A](silk:Silk[A]) : Set[Silk[_]] = {
//    val g = graphOf(silk)
//    g.descendantsOf(silk)
//  }
//
//  def descendantsOf[A](silk:Silk[A], targetName:String) : Set[Silk[_]] = {
//    val g = graphOf(silk)
//    findTarget(silk, targetName) match {
//      case Some(x) => g.descendantsOf(x)
//      case None => Set.empty
//    }
//  }
//
//
//}
//
///**
// * @author Taro L. Saito
// */
//class CallGraph(val nodes: Seq[Silk[_]], val edges: Map[UUID, Seq[UUID]]) {
//
//  private val idToSilkTable = nodes.map(x => x.id -> x).toMap
//  private val silkToIdTable = nodes.map(x => x -> x.id).toMap
//
//  private def idPrefix(uuid: UUID) = uuid.toString.substring(0, 8)
//
//  def node(id:UUID) : Silk[_] = idToSilkTable(id)
//
//  override def toString = {
//    val s = new StringBuilder
//    s append "[nodes]\n"
//    for (n <- nodes)
//      s append s" $n\n"
//
//    s append "[edges]\n"
//    val edgeList = for((src, lst) <- edges; dest <- lst) yield src -> dest
//    for((src, dest) <- edgeList.toSeq.sortBy(p => (p._2, p._1))) {
//      s append s" [${idPrefix(src)}] ${node(src).summary} -> [${idPrefix(dest)}] ${node(dest).summary}\n"
//    }
//    s.toString
//  }
//
//  def childrenOf(op: Silk[_]): Seq[Silk[_]] = {
//    val childIDs = edges.getOrElse(op.id, Seq.empty)
//    childIDs.map(cid => idToSilkTable(cid))
//  }
//
//  def descendantsOf(op: Silk[_]): Set[Silk[_]] = {
//    var seen = Set.empty[Silk[_]]
//    def loop(current: Silk[_]) {
//      if (seen.contains(current))
//        seen += current
//      for (child <- childrenOf(current))
//        loop(child)
//    }
//    loop(op)
//    seen
//  }
//
//}