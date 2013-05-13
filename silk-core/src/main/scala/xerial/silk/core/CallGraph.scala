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
    b.traverse(None, dataflow)
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

    def traverse(contextNode:Option[DataFlowNode], a:Any) {
      if(visited.contains(a))
        return

      visited += a

      a match {
        case f:SilkFlow[_, _] =>
          debug(s"find\n${f.toSilkString}")
        case _ =>
      }

      a match {
        case fm @ FlatMap(prev, f, fExpr) =>
          val n = g.add(DFNode(fm, findValDefs(fExpr)))
          traverse(None, prev)
          val mc = FunctionTree.collectMethodCall(fExpr.tree)
          debug(s"Method call: $mc")

          fExpr.staticType match {
            case t @ TypeRef(prefix, symbol, List(from, to)) =>
              val inputCl = mirror.runtimeClass(from)
              val z = TypeUtil.zero(inputCl)
              val nextExpr = f.asInstanceOf[Any => Any].apply(z)
              traverse(Some(n), nextExpr)
            case other => warn(s"unknown type: ${other}")
          }
        case mf @ MapFun(prev, f, fExpr) =>
          traverse(None, prev)
          val n = g.add(DFNode(mf, findValDefs(fExpr)))
          fExpr.staticType match {
            case t @ TypeRef(prefix, symbol, List(from, to)) =>
              val inputCl = mirror.runtimeClass(from)
              val z = TypeUtil.zero(inputCl)
              val nextExpr = f.asInstanceOf[Any => Any].apply(z)
              traverse(Some(n), nextExpr)
            case other => warn(s"unknown type: ${other}")
          }
        case s @ SaveToFile(prev) =>
          traverse(None, prev)
        case s @ ShellCommand(sc, args, argExpr) =>
          val n = g.add(CmdNode(s))
          argExpr.foreach(traverse(Some(n), _))
        case c @ CommandOutputStream(cmd) =>
          val n = g.add(DataSourceNode(c))
          traverse(None, cmd)
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
                for(c <- contextNode)
                  g.connect(RefNode(resolveClass(cls), term.decoded, rc), c)
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
  private var nodes = Set[DataFlowNode]()
  private var edges = Map[DataFlowNode, DataFlowNode]()

  override def toString = {
    val b = new StringBuilder
    for((f, t) <- edges) {
      b.append(s"$f -> $t\n")
    }
    b.result
  }

  def add(n:DataFlowNode) : DataFlowNode = {
    if(!nodes.contains(n)) {
      trace(s"Add node: $n")
      nodes += n
    }
    n
  }

  def connect(from:DataFlowNode, to:DataFlowNode) {
    add(from)
    add(to)
    edges += from -> to
  }

}


