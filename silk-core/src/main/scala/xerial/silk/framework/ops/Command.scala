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
import xerial.silk._
import java.util.UUID



object CommandImpl {

  class Helper[C <: Context](val c:C) {

    def frefTree = {
      val helper = new SilkMacros.MacroHelper[c.type](c)
      helper.createFContext.tree.asInstanceOf[c.Tree]
    }

  }

  def mCommand(c:Context)(args:c.Expr[Any]*) = {
    import c.universe._
    val helper = new Helper[c.type](c)
    val argSeq = c.Expr[Seq[Any]](Apply(Select(reify{Seq}.tree, newTermName("apply")), args.map(_.tree).toList))
    val exprGenSeq = for(a <- args) yield {
      val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(a.tree))
      c.Expr[Expr[ru.Expr[_]]](t).tree
    }
    val argExprSeq = c.Expr[Seq[ru.Expr[_]]](Apply(Select(reify{Seq}.tree, newTermName("apply")), exprGenSeq.toList))
    val fref = c.Expr[FContext](helper.frefTree)
    reify { CommandOp(SilkUtil.newUUID, fref.splice, c.Expr[CommandBuilder](c.prefix.tree).splice.sc, argSeq.splice, argExprSeq.splice, reify{None}.splice) }
  }

}

trait Command {

  def cmdString : String
}

trait CommandHelper extends Command {
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

case class CommandResource(cpu:Int, memory:Long)


case class CommandOp(id:UUID, fc: FContext, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]], resource:Option[CommandResource])
  extends SilkSingle[Any] with CommandHelper {
  def lines = CommandOutputLinesOp(SilkUtil.newUUID, fc, sc, args, argsExpr)
  def file = CommandOutputFileOp(SilkUtil.newUUID, fc, sc, args, argsExpr)
  def string = CommandOutputStringOp(SilkUtil.newUUID, fc, sc, args, argsExpr)
  def &&[A](next:Command) : CommandSeqOp[A] = CommandSeqOp(SilkUtil.newUUID, fc, next, sc, args, argsExpr)

  def cpu(numCPU:Int) : CommandOp = {
    val newResource = resource match {
      case Some(r) => CommandResource(numCPU, r.memory)
      case None => CommandResource(numCPU, -1)
    }
    CommandOp(id, fc, sc, args, argsExpr, Some(newResource))
  }
  def memory(mem:Long) : CommandOp = {
    val newResource = resource match {
      case Some(r) => CommandResource(r.cpu, mem)
      case None => CommandResource(1, mem)
    }
    CommandOp(id, fc, sc, args, argsExpr, Some(newResource))
  }


}

case class CommandOutputStringOp(id:UUID, fc:FContext, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]])
 extends SilkSingle[String] with CommandHelper {

}

case class CommandOutputLinesOp(id:UUID, fc: FContext, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]])
  extends SilkSeq[String] with CommandHelper {
}
case class CommandOutputFileOp(id:UUID, fc: FContext, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]])
  extends SilkSingle[String] with CommandHelper {
}

case class CommandSeqOp[A](id:UUID, fc:FContext, next: Command, sc:StringContext, args:Seq[Any], @transient argsExpr:Seq[ru.Expr[_]])
 extends SilkSingle[Any] with CommandHelper {

  override def cmdString = {
    s"${super.cmdString} && ${next.cmdString}"
  }
}
