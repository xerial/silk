//--------------------------------------
//
// Command.scala
// Since: 2013/06/19 3:34 PM
//
//--------------------------------------

package xerial.silk.core
import scala.language.existentials
import xerial.silk._
import java.util.UUID
import xerial.core.log.Logger
import java.io.File


trait Command {


  def cmdString(implicit weaver:Weaver) : String
  def templateString : String

  def commandInputs : Seq[Silk[_]]
}

object Command {

  def templateString(sc:StringContext) = {
    val b = new StringBuilder
    for(p <- sc.parts) {
      b.append(p)
      b.append("${}")
    }
    b.result()
  }

  def silkInputs(args:Seq[Any]) = {
    val b = Seq.newBuilder[Silk[_]]
    for((a, index) <- args.zipWithIndex) {
      if(CallGraph.isSilkType(a.getClass)) {
        b += a.asInstanceOf[Silk[_]]
      }
    }
    b.result
  }

}


trait CommandHelper extends Command {
  self: Silk[_] =>


  def fc: FContext
  def sc:StringContext
  def args:Seq[Any]
  //def argsExpr:Seq[ru.Expr[_]]

  def argSize = args.size
  def arg(i:Int) : Any = args(i)
  def argSeq : Seq[Any] = args

  def cmdString(implicit weaver:Weaver) : String = {
    val b = new StringBuilder
    val zip = sc.parts.zipAll(args, "", null)
    for((f, v) <- zip) {
      b.append(f)
      val vv = v match {
        case s:SilkSingle[_] => weaver.weave(s)
        case s:SilkSeq[_] => weaver.weave(s)
        case _ => v
      }
      if(vv != null)
        b.append(vv)
    }
    b.result()
  }

  def templateString = Command.templateString(sc)

  def commandInputs : Seq[Silk[_]] = Command.silkInputs(args)


}

import scala.reflect.runtime.{universe=>ru}


case class CommandResource(cpu:Int, memory:Long)


case class CommandOp(id:UUID, fc: FContext, sc:StringContext, args:Seq[Any], resource:Option[CommandResource])
  extends SilkSingle[Any] with CommandHelper {

  override def inputs = commandInputs

  def lines : SilkSeq[String] = CommandOutputLinesOp(SilkUtil.newUUIDOf(classOf[CommandOutputLinesOp], fc, id), fc, sc, args)
  def file = CommandOutputFileOp(SilkUtil.newUUIDOf(classOf[CommandOutputFileOp], fc, id), fc, sc, args)
  def string : SilkSingle[String] = CommandOutputStringOp(SilkUtil.newUUIDOf(classOf[CommandOutputStringOp], fc, id), fc, sc, args)
  def &&[A](next:Command) : CommandSeqOp[A] = CommandSeqOp(SilkUtil.newUUIDOf(classOf[CommandSeqOp[_]], fc, id), fc, next, sc, args)

  def cpu(numCPU:Int) : CommandOp = {
    val newResource = resource match {
      case Some(r) => CommandResource(numCPU, r.memory)
      case None => CommandResource(numCPU, -1)
    }
    CommandOp(id, fc, sc, args, Some(newResource))
  }
  def memory(mem:Long) : CommandOp = {
    val newResource = resource match {
      case Some(r) => CommandResource(r.cpu, mem)
      case None => CommandResource(1, mem)
    }
    CommandOp(id, fc, sc, args, Some(newResource))
  }

}



case class CommandOutputStringOp(id:UUID, fc:FContext, sc:StringContext, args:Seq[Any])
 extends SilkSingle[String] with CommandHelper {
  override def inputs = commandInputs

}

case class CommandOutputLinesOp(id:UUID, fc: FContext, sc:StringContext, args:Seq[Any])
  extends SilkSeq[String] with CommandHelper {
  override def inputs = commandInputs

}


case class CommandOutputFileOp(id:UUID, fc: FContext, sc:StringContext, args:Seq[Any])
  extends SilkSingle[String] with CommandHelper with Logger {
  override def inputs = commandInputs

  override def toString = s"[$idPrefix] CommandOutputFileOp(${fc}, [${templateString}])"
}

case class CommandSeqOp[A](id:UUID, fc:FContext, next: Command, sc:StringContext, args:Seq[Any])
 extends SilkSingle[Any] with CommandHelper {

  override def toString = s"[$idPrefix] CommandSeqOp(${fc}, [${templateString}])"
  override def inputs = commandInputs ++ next.commandInputs

  override def cmdString(implicit weaver:Weaver) = {
    s"${super.cmdString} => ${next}"
  }

  override def templateString = {
    s"${super.templateString} => ${next}"
  }

}


case class ListFilesOp(id:UUID, fc:FContext, pattern:String) extends SilkSeq[File]
case class ListDirsOp(id:UUID, fc:FContext, pattern:String) extends SilkSeq[File]
