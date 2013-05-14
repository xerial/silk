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
    val b = new Builder(contextClass)
    b.traverse(None, None, dataflow)
    b.build
  }



  private class Builder[A](contextClass:Class[A]) {

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

      def updateGraph(n:DataFlowNode) {
        for(p <- parentContext)
          g.connect(p, n)
        for(c <- childContext)
          g.connect(n, c)
      }

      def traverseMap[A,B](sf:SilkFlow[_, _], prev:Silk[A], f:A=>B, fExpr:ru.Expr[_]) {
        val n = g.add(FNode(sf, findValDefs(fExpr)))
        updateGraph(n)
        traverse(None, Some(n), prev)
        fExpr.staticType match {
          case t @ TypeRef(prefix, symbol, List(from, to)) =>
            val inputCl = mirror.runtimeClass(from)

            val z = inputCl match {
              case f if classOf[Silk[_]].isAssignableFrom(f) =>
                Silk.empty
              case _ => TypeUtil.zero(inputCl)
            }
            val nextExpr = f.asInstanceOf[Any => Any].apply(z)
            //trace(s"inputCl:$inputCl, nextExpr:$nextExpr")
            traverse(Some(n), None, nextExpr)
          case other => warn(s"unknown type: ${other}")
        }
      }

      def traverseCmdArg(c:DataFlowNode, e:ru.Expr[_]) {
        //trace(s"traverse cmd arg: ${showRaw(e)}")

        def traceType(st:ru.Type, cls:Option[MethodOwnerRef], term:ru.Name) {
          val rc = mirror.runtimeClass(st)
          if(classOf[Silk[_]].isAssignableFrom(rc)) {
            //trace(s"silk type input: $rc")
            val rn = RefNode(None, term.decoded, rc)
            g.connect(rn, c) // rn is referenced in the context
          }
        }

        e match {
          case Expr(i @ Ident(term)) =>
            traceType(e.staticType, None, term)
          case Expr(Select(cls, term)) =>
            traceType(e.staticType, resolveClass(cls), term)
          case _ => warn(s"unknown expr type: ${showRaw(e)}")
        }
      }


      debug(s"visited $a")

      a match {
        case fm @ FlatMap(prev, f, fExpr) =>
          traverseMap(fm, prev, f, fExpr)
        case mf @ MapFun(prev, f, fExpr) =>
          traverseMap(mf, prev, f, fExpr)
        case mf @ Foreach(prev, f, fExpr) =>
          traverseMap(mf, prev, f, fExpr)
        case mf @ WithFilter(prev, f, fExpr) =>
          traverseMap(mf, prev, f, fExpr)
        case s @ ShellCommand(sc, args, argExpr) =>
          val n = g.add(DNode(s))
          updateGraph(n)
          argExpr.foreach(traverseCmdArg(n, _))
        case cs @ CommandSeq(prev, next) =>
          val n = g.add(DNode(cs))
          updateGraph(n)
          traverse(None, Some(n), prev)
          traverse(Some(DNode(prev)), None, next)
        case j @ Join(l, r, k1, k2) =>
          val n = g.add(DNode(j))
          updateGraph(n)
          traverse(None, Some(n), l)
          traverse(None, Some(n), r)
        case z @ Zip(p, o) =>
          val n = g.add(DNode(z))
          updateGraph(n)
          traverse(None, Some(n), p)
          traverse(None, Some(n), o)
        case c @ CommandOutputStream(cmd) =>
          val n = g.add(DNode(c))
          updateGraph(n)
          traverse(None, Some(n), cmd)
        case r @ RawInput(in) =>
          val n = g.add(DNode(r))
          updateGraph(n)
        case s @ SingleInput(e) =>
          val n = g.add(DNode(s))
          updateGraph(n)
        case w: WithInput[_] =>
          val n = g.add(DNode(w.asInstanceOf[Silk[_]]))
          traverse(None, Some(n), w.prev)
        case f:SilkFlow[_, _] =>
          warn(s"not yet implemented ${f.getClass.getSimpleName}")
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
case class FNode[A, B](flow:SilkFlow[A,B], valDefs:List[ValDef]) extends DataFlowNode
case class DNode[A](flow:Silk[A]) extends DataFlowNode
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
        case FNode(flow, vd) if vd.size == 1 =>
          b.append(s"${id(f)} -> (${vd.head.name.decoded} => ${id(t)})\n")
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


