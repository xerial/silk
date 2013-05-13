//--------------------------------------
//
// CallGraph.scala
// Since: 2013/05/10 4:14 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.core.log.Logger
import xerial.lens.TypeUtil
import scala.reflect.runtime.{universe=>ru}
import ru._

/**
 * @author Taro L. Saito
 */
object CallGraph extends Logger {

  import SilkFlow._





  private def mirror = ru.runtimeMirror(Thread.currentThread.getContextClassLoader)

  def apply[A](contextClass:Class[A], dataflow:Any) : CallGraph = {
    debug(s"context class: ${contextClass.getName}")
    val b = new Builder
    b.traverse(None, None, dataflow)
    b.build
  }



  private class Builder {

    import ru._

    var visited = Set.empty[Any]

    val g = new CallGraph

    private def findValDefs(fExpr:ru.Expr[_]) : List[ValDef] = {
      fExpr match {
        case Expr(Function(valDef, body)) =>
          valDef map { case v @ValDef(mod, name, a1, a2) => v }
        case _ => List.empty
      }
    }

    def traverse(parentContext:Option[DataFlowNode], childContext:Option[DataFlowNode], a:Any) {
      if(visited.contains(a))
        return

      visited += a

      a match {
        case f:SilkFlow[_, _] =>
          debug(s"find\n${f.toSilkString}")
        case _ =>
      }

      def updateGraph(n:DataFlowNode) {
        for(p <- parentContext)
          g.connect(p, n)
        for(c <- childContext)
          g.connect(n, c)
      }

      def traverseMap[A,B](sf:SilkFlow[_, _], prev:Silk[A], f:A=>B, fExpr:ru.Expr[_]) {
        val n = g.add(DFNode(sf, findValDefs(fExpr)))
        updateGraph(n)
        traverse(None, Some(n), prev)
        val mc = FunctionTree.collectMethodCall(fExpr.tree)
        debug(s"Method call: $mc")
        fExpr.staticType match {
          case t @ TypeRef(prefix, symbol, List(from, to)) =>
            val inputCl = mirror.runtimeClass(from)
            val z = TypeUtil.zero(inputCl)
            val nextExpr = f.asInstanceOf[Any => Any].apply(z)
            traverse(Some(n), None, nextExpr)
          case other => warn(s"unknown type: ${other}")
        }
      }

      a match {
        case fm @ FlatMap(prev, f, fExpr) =>
          traverseMap(fm, prev, f, fExpr)
        case mf @ MapFun(prev, f, fExpr) =>
          traverseMap(mf, prev, f, fExpr)
        case s @ SaveToFile(prev) =>
          val n = DataSourceNode(s)
          updateGraph(n)
          traverse(None, Some(n), prev)
        case s @ ShellCommand(sc, args, argExpr) =>
          val n = g.add(CmdNode(s))
          updateGraph(n)
          argExpr.foreach(traverse(Some(n), None, _))
        case c @ CommandOutputStream(cmd) =>
          val n = g.add(DataSourceNode(c))
          updateGraph(n)
          traverse(None, Some(n), cmd)
        case f:SilkFlow[_, _] =>
          warn(s"not yet implemented ${f.getClass.getSimpleName}")
        case e:ru.Expr[_] =>
          trace(s"Traverse expr: ${showRaw(e)}")
          // Track variable references
          e match {
            case Expr(Ident(term)) =>
              val st = e.staticType
              trace(s"static type: $st")
            case Expr(Select(cls, term)) =>
              val st = e.staticType
              trace(s"static type: $st")
              val rc = mirror.runtimeClass(st)
              if(classOf[Silk[_]].isAssignableFrom(rc)) {
                trace(s"silk type input: $rc")
                val rn = RefNode(resolveClass(cls), term.decoded, rc)
                for(p <- parentContext)
                  g.connect(rn, p) // rn is referenced in parent
              }
            case _ => warn(s"unknown expr type: ${showRaw(e)}")
          }

        case e =>
          // ignore
      }
    }

    def resolveClass(t:ru.Tree) : Option[MethodOwnerRef] = {
      t match {
        case This(typeName) => Some(ThisTypeRef(typeName.decoded))
        case Ident(refName) => Some(IdentRef(refName.decoded))
        case _ => None
      }
    }


    def build : CallGraph = g

  }

}

trait DataFlowNode
case class DFNode[A, B](flow:SilkFlow[A,B], valDefs:List[ValDef]) extends DataFlowNode
case class CmdNode(ShellCommand:ShellCommand) extends DataFlowNode
case class DataSourceNode[A, B](flow:SilkFlow[A, B]) extends DataFlowNode
case class RefNode[A](owner:Option[MethodOwnerRef], name:String, targetType:Class[A]) extends DataFlowNode

class CallGraph() extends Logger {

  private var nodeCount = 0

  private var nodeTable = Map[DataFlowNode, Int]()
  private var edges = Map[DataFlowNode, DataFlowNode]()

  def id(n:DataFlowNode) = nodeTable.getOrElse(n, -1)

  override def toString = {
    val b = new StringBuilder
    b.append("[nodes]\n")
    for((n, id) <- nodeTable.toSeq.sortBy(_._2)) {
      b.append(f"[$id]: $n\n")
    }
    b.append("[edges]\n")
    for((f, t) <- edges) {
      t match {
        case DFNode(flow, vd) if vd.size == 1 =>
          b.append(s"${id(f)} -> ${vd.head.name.decoded}:${id(t)}\n")
        case _ => b.append(s"${id(f)} -> ${id(t)}\n")
      }
    }
    b.result
  }

  def add(n:DataFlowNode) : DataFlowNode = {
    if(!nodeTable.contains(n)) {
      val newID = nodeCount + 1
      nodeCount += 1
      nodeTable += n -> newID
      trace(s"Add node: [$newID]:$n")
    }
    n
  }

  def connect(from:DataFlowNode, to:DataFlowNode) {
    add(from)
    add(to)
    edges += from -> to
  }

}


