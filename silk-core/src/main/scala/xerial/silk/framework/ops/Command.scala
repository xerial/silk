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
import xerial.silk.SilkException


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
      val helper = new SilkMacros.MacroHelper[c.type](c)
      helper.createFContext.tree.asInstanceOf[c.Tree]
    }

  }

  def mOutputLines(c:Context) = {
    import c.universe._
    val helper = new Helper[c.type](c)
    val argExprSeq = helper.argExprSeq
    val fref = c.Expr[FContext](helper.frefTree)
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].lineOp(fref.splice, argExprSeq.splice) }
  }


  def toSilkImpl(c:Context) = {
    import c.universe._
    val helper = new Helper[c.type](c)
    val argExprSeq = helper.argExprSeq
    val fref = c.Expr[FContext](helper.frefTree)
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].withArgs(fref.splice, argExprSeq.splice) }
  }

  def toFileImpl(c:Context) = {
    import c.universe._
    val helper = new Helper[c.type](c)
    val argExprSeq = helper.argExprSeq
    val fref = c.Expr[FContext](helper.frefTree)
    reify { c.prefix.splice.asInstanceOf[PreSilkCommand].fileOp(fref.splice, argExprSeq.splice) }
  }

}

case class PreSilkCommand(sc:StringContext, args:Seq[Any]) {
  def lines : CommandOutputLinesOp = macro CommandImpl.mOutputLines
  def toSilk : CommandOp = macro CommandImpl.toSilkImpl
  def file : CommandOutputFileOp = macro CommandImpl.toFileImpl
  def &&[A](next:Silk[A]) : CommandOp = SilkException.NA

  private[silk] def withArgs(fref:FContext, argExprs:Seq[ru.Expr[_]]) = CommandOp(fref, sc, args, argExprs)
  private[silk] def lineOp(fref:FContext, argExprs:Seq[ru.Expr[_]]) = CommandOutputLinesOp(fref, sc, args, argExprs)
  private[silk] def fileOp(fref:FContext, argExprs:Seq[ru.Expr[_]]) = CommandOutputFileOp(fref, sc, args, argExprs)
}

trait CommandHelper  {
  val fc: FContext
  val sc:StringContext
  val args:Seq[Any]

  def argSize = args.size
  def arg(i:Int) : Any = args(i)
  def argSeq : Seq[Any] = args

  def cmdString = {
    val b = new StringBuilder
    val zip = sc.parts.zipAll(args, "", null)
    for((f, v) <- zip) {
      b.append(f)
      if(v != null)
        b.append(v)
    }
    b.result()
  }


  def templateString = {
    val b = new StringBuilder
    val zip = sc.parts.zipAll(args, "", null)
    for((f, v) <- zip) {
      b.append(f)
      if(v != null)
        b.append("${}")
    }
    b.result()
  }

}


case class CommandOp(override val fc: FContext, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]])
  extends SilkSingle[Any](fc) with CommandHelper {
  def lines = CommandOutputLinesOp(fc, sc, args, argsExpr)
  def file = CommandOutputFileOp(fc, sc, args, argsExpr)

}
case class CommandOutputLinesOp(override val fc: FContext, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]])
  extends SilkSeq[String](fc) with CommandHelper {
}
case class CommandOutputFileOp(override val fc: FContext, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]])
  extends SilkSingle[String](fc) with CommandHelper {
}

/**
 * @author Taro L. Saito
 */
object Command {

  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) = PreSilkCommand(sc, args)
  }


}