//--------------------------------------
//
// Command.scala
// Since: 2013/06/19 3:34 PM
//
//--------------------------------------

package xerial.silk.framework.ops
import scala.language.experimental.macros
import scala.language.existentials
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

    def frefTree = {
      val helper = new SilkOps.MacroHelper[c.type](c)
      helper.createFContext.tree.asInstanceOf[c.Tree]
    }

  }

  def mOutputLines(c:Context) = {
    import c.universe._
    val helper = new Helper[c.type](c)
    val argExprSeq = helper.argExprSeq
    val fref = c.Expr[FContext[_]](helper.frefTree)
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].lineOp(fref.splice, argExprSeq.splice) }
  }


  def toSilkImpl(c:Context) = {
    import c.universe._
    val helper = new Helper[c.type](c)
    val argExprSeq = helper.argExprSeq
    val fref = c.Expr[FContext[_]](helper.frefTree)
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].withArgs(fref.splice, argExprSeq.splice) }
  }

  def toFileImpl(c:Context) = {
    import c.universe._
    val helper = new Helper[c.type](c)
    val argExprSeq = helper.argExprSeq
    val fref = c.Expr[FContext[_]](helper.frefTree)
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].fileOp(fref.splice, argExprSeq.splice) }
  }

}

case class PreSilkCommand(sc:StringContext, args:Seq[Any]) {
  def lines : CommandOutputLinesOp = macro CommandImpl.mOutputLines
  def toSilk : CommandOp = macro CommandImpl.toSilkImpl
  def file : CommandOutputFileOp = macro CommandImpl.toFileImpl

  private[silk] def withArgs(fref:FContext[_], argExprs:Seq[ru.Expr[_]]) = CommandOp(fref, sc, args, argExprs)
  private[silk] def lineOp(fref:FContext[_], argExprs:Seq[ru.Expr[_]]) = CommandOutputLinesOp(fref, sc, args, argExprs)
  private[silk] def fileOp(fref:FContext[_], argExprs:Seq[ru.Expr[_]]) = CommandOutputFileOp(fref, sc, args, argExprs)
}


case class CommandOp(override val fref: FContext[_], sc:StringContext, args:Seq[Any], argsExpr:Seq[ru.Expr[_]]) extends SilkOps[Any](fref) {
  def cmdString = sc.s(args:_*)
  def lines = CommandOutputLinesOp(fref, sc, args, argsExpr)
}
case class CommandOutputLinesOp(override val fref: FContext[_], sc:StringContext, args:Seq[Any], argsExpr:Seq[ru.Expr[_]]) extends SilkOps[String](fref) {
  def cmdString = sc.s(args:_*)
}
case class CommandOutputFileOp(override val fref: FContext[_], sc:StringContext, args:Seq[Any], argsExpr:Seq[ru.Expr[_]]) extends SilkOps[String](fref) {
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