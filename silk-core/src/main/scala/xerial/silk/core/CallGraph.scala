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
    trace(s"context class: ${contextClass.getName}")
    val b = new Builder(contextClass)
    b.traverseParent(None, None, dataflow)
    b.build
  }



  case class Context(boundVariable:Set[String] = Set.empty, freeVariable:Set[String]=Set.empty) {
  }

  private class Builder[A](contextClass:Class[A]) {

    import ru._

    var visited = Set.empty[(Context, Any)]

    val g = new CallGraph

    private def findValDefs(fExpr:ru.Expr[_]) : List[ValDef] = {
      fExpr match {
        case Expr(Function(valDef, body)) =>
          valDef map { case v @ValDef(mod, name, a1, a2) => v }
        case _ => List.empty
      }
    }

    def traverseParent(parentNode:Option[DataFlowNode], childNode:Option[DataFlowNode], a:Any)
     = traverse(parentNode, childNode, Context(), a, isForward=false)


    def traverse(parentNode:Option[DataFlowNode], childNode:Option[DataFlowNode], context:Context, a:Any, isForward:Boolean = true) {

     val t = (context, a)
     if(visited.contains(t))
       return

      visited += t

      def updateGraph(n:DataFlowNode) {
        for(p <- parentNode)
          g.connect(p, n)
        for(c <- childNode)
          g.connect(n, c)
      }

      def isSilkType[A](cl:Class[A]) : Boolean = classOf[Silk[_]].isAssignableFrom(cl)
      def zero[A](cl:Class[A]) = cl match {
        case f if isSilkType(f) =>
          Silk.empty
        case _ => TypeUtil.zero(cl)
      }

      def traverseMap[A,B](sf:SilkFlow[_, _], prev:Silk[A], f:A=>B, fExpr:ru.Expr[_]) {
        val vd = findValDefs(fExpr)
        var boundVariables : Set[String] = context.boundVariable ++ vd.map(_.name.decoded)
        var freeVariables = context.freeVariable
        val n = g.add(FNode(sf, vd))
        updateGraph(n)
        traverse(None, Some(n), Context(Set.empty, Set.empty), prev)

        // Traverse variable references
        object VarRefTraverse extends Traverser {
          override def traverse(tree: ru.Tree) {
            tree match {
              case Ident(name) =>
                val nm = name.decoded
                if(!boundVariables.contains(nm)) {
                  freeVariables += nm
                  trace(s"Accessed ${nm}")
                }
              case _ => super.traverse(tree)
            }
          }
        }
        debug(s"fExpr:${showRaw(fExpr)}")
        VarRefTraverse.traverse(fExpr.tree)

        fExpr.staticType match {
          case t @ TypeRef(prefix, symbol, List(from, to)) =>
            val inputCl = mirror.runtimeClass(from)
            if(isSilkType(mirror.runtimeClass(to))) {
              // Run the function to obtain its result by using a dummy input
              val z = zero(inputCl)
              val nextExpr = f.asInstanceOf[Any => Any].apply(z)
              // Replace the dummy input
              val ne = nextExpr match {
                case f:SilkFlow.WithInput[_] if f.prev.isRaw =>
                  f.copyWithoutInput
                case _ =>
                  nextExpr
              }
              traverse(Some(n), None, Context(boundVariables, freeVariables), ne)
            }
          case other => warn(s"unknown type: ${other}")
        }

      }

      def traverseCmdArg(c:DataFlowNode, e:ru.Expr[_]) {
        trace(s"traverse cmd arg: ${showRaw(e)}")

        def traceType(st:ru.Type, cls:Option[MethodOwnerRef], term:ru.Name) {
          val rc = mirror.runtimeClass(st)
          if(isSilkType(rc)) {
            val rn = RefNode(cls, term.decoded, rc)
            g.connect(rn, c) // rn is referenced in the context
            import scala.tools.reflect.ToolBox
            val t = mirror.mkToolBox()
            val ref = t.eval(e.tree)
            traverseParent(None, Some(rn), ref)
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


      trace(s"visited $a")

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
          traverseParent(None, Some(n), prev)
          traverse(Some(DNode(prev)), None, context, next)
        case j @ Join(l, r, k1, k2) =>
          val n = g.add(DNode(j))
          updateGraph(n)
          traverseParent(None, Some(n), l)
          traverse(None, Some(n), Context(), r)
        case z @ Zip(p, o) =>
          val n = g.add(DNode(z))
          updateGraph(n)
          traverseParent(None, Some(n), p)
          traverseParent(None, Some(n), o)
        case c @ LineInput(cmd) =>
          val n = g.add(DNode(c))
          updateGraph(n)
          traverseParent(None, Some(n), cmd)
        case r @ RawInput(in) =>
          val n = g.add(DNode(r))
          updateGraph(n)
        case s @ RawInputSingle(e) =>
          val n = g.add(DNode(s))
          updateGraph(n)
        case w: WithInput[_] =>
          val n = g.add(DNode(w.asInstanceOf[Silk[_]]))
          updateGraph(n)
          traverseParent(None, Some(n), w.prev)
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
case class FNode[A, B](flow:SilkFlow[A,B], valDefs:List[ValDef]) extends DataFlowNode {
  override def toString = {
    val s = new StringBuilder
    s.append(s"val ${valDefs.map(v => v.name.decoded).mkString(", ")} =\n${flow.toSilkString}")
    s.result
  }
}
case class DNode[A](flow:Silk[A]) extends DataFlowNode {
  override def toString = flow.toString
}

case class RefNode[A](owner:Option[MethodOwnerRef], name:String, targetType:Class[A]) extends DataFlowNode {
}


class CallGraph() extends Logger {

  private var nodeCount = 0

  private var nodeTable = Map[DataFlowNode, Int]()
  private var edges = Set[(DataFlowNode, DataFlowNode)]()

  def id(n:DataFlowNode) = nodeTable.getOrElse(n, -1)

  override def toString = {
    val b = new StringBuilder
    b.append("[nodes]\n")
    for((n, id) <- nodeTable.toSeq.sortBy(_._2)) {
      b.append(f"[$id]: ${n}\n")
    }
    b.append("[edges]\n")
    for((f, t) <- edges.toSeq.sortBy{ case (a:DataFlowNode, b:DataFlowNode) => (id(b), id(a))}) {
      (f, t) match {
        case (_, FNode(flow, vd)) if vd.size == 1 =>
          b.append(s"${id(f)} -> ${id(t)} => ${vd.head.name.decoded}\n")
        case (RefNode(_, name, _), _) =>
          b.append(s"${name}:${id(f)} -> ${id(t)}\n")
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
    trace(s"connect ${id(from)} -> ${id(to)}")

    edges += from -> to
  }

}


