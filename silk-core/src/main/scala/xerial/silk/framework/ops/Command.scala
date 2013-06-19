//--------------------------------------
//
// Command.scala
// Since: 2013/06/19 3:34 PM
//
//--------------------------------------

package xerial.silk.framework.ops
import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.{universe => ru}


object CommandImpl {

  class Helper[C <: Context](val c:C) {
    import c.universe._

    def argExprSeq = {
      val exprGenSeq = c.prefix.tree match {
        case Apply(_, args) =>
          for(a <- args) yield {
            val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(a))
            c.Expr[Expr[ru.Expr[_]]](t).tree
          }
        case _ => Seq.empty
      }
      val argExprSeq = c.Expr[Seq[ru.Expr[_]]](Apply(Select(reify{Seq}.tree, newTermName("apply")), exprGenSeq.toList))
      argExprSeq
    }
  }

  def mOutputLines(c:Context) = {
    import c.universe._
    val argExprSeq = new Helper[c.type](c).argExprSeq
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].lineOp(argExprSeq.splice) }
  }


  def toSilkImpl(c:Context) = {
    import c.universe._
    val argExprSeq = new Helper[c.type](c).argExprSeq
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].withArgs(argExprSeq.splice) }
  }

}

case class PreSilkCommand(sc:StringContext, args:Seq[Any]) {
  def lines : SilkOutputLines = macro CommandImpl.mOutputLines
  def toSilk : SilkCommand = macro CommandImpl.toSilkImpl

  private[silk] def withArgs(argExprs:Seq[ru.Expr[_]]) = SilkCommand(sc, args, argExprs)
  private[silk] def lineOp(argExprs:Seq[ru.Expr[_]]) = SilkOutputLines(sc, args, argExprs)
}


case class SilkCommand(sc:StringContext, args:Seq[Any], argsExpr:Seq[ru.Expr[_]]) {
  def cmdString = sc.s(args:_*)
}
case class SilkOutputLines(sc:StringContext, args:Seq[Any], argsExpr:Seq[ru.Expr[_]]) {
  def cmdString = sc.s(args:_*)
}

/**
 * @author Taro L. Saito
 */
object Command {

  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) = PreSilkCommand(sc, args)
  }

}