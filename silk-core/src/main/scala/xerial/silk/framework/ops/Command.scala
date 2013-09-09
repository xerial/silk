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
import xerial.silk.util.MacroUtil
import xerial.core.log.Logger


trait Command {


  def cmdString : String
  def templateString : String

  def commandInputs : Seq[Silk[_]]
}

trait CommandHelper extends Command {

  def fc: FContext
  def sc:StringContext
  def args:Seq[Any]
  //def argsExpr:Seq[ru.Expr[_]]

  def argSize = args.size
  def arg(i:Int) : Any = args(i)
  def argSeq : Seq[Any] = args

  def cmdString = {
    val b = new StringBuilder
    val zip = sc.parts.zipAll(args, "", null)
    for((f, v) <- zip) {
      b.append(f)
      val vv = v match {
        case s:SilkSingle[_] => s.get
        case s:SilkSeq[_] => s.get
        case _ => v
      }
      if(vv != null)
        b.append(vv)
    }
    b.result()
  }

  def templateString = {
    val b = new StringBuilder
    for(p <- sc.parts) {
      b.append(p)
      b.append("${}")
    }
    b.result()
  }



  def commandInputs : Seq[Silk[_]] = {
    val b = Seq.newBuilder[Silk[_]]
    for((a, index) <- args.zipWithIndex) {
      //val st = a.staticType
      if(CallGraph.isSilkType(a.getClass)) {
      //if(CallGraph.isSilkType(MacroUtil.mirror.runtimeClass(st))) {
        b += a.asInstanceOf[Silk[_]]
      }
    }
    b.result
  }

}

import scala.reflect.runtime.{universe=>ru}
import ru._


case class CommandResource(cpu:Int, memory:Long)


case class CommandOp(id:UUID, fc: FContext, sc:StringContext, args:Seq[Any], resource:Option[CommandResource])
  extends SilkSingle[Any] with CommandHelper {
  def lines : SilkSeq[String] = CommandOutputLinesOp(SilkUtil.newUUID, fc, sc, args)
  def file = CommandOutputFileOp(SilkUtil.newUUID, fc, sc, args)
  def string : SilkSingle[String] = CommandOutputStringOp(SilkUtil.newUUID, fc, sc, args)
  def &&[A](next:Command) : CommandSeqOp[A] = CommandSeqOp(SilkUtil.newUUID, fc, next, sc, args)

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

  override def inputs = commandInputs
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

  override def toString = s"[$idPrefix] CommandOutputFileOp(${fc}, [${templateString}])"
  override def inputs = commandInputs

}

case class CommandSeqOp[A](id:UUID, fc:FContext, next: Command, sc:StringContext, args:Seq[Any])
 extends SilkSingle[Any] with CommandHelper {

  override def toString = s"[$idPrefix] CommandSeqOp(${fc}, [${templateString}])"
  override def inputs = commandInputs ++ next.commandInputs

  override def cmdString = {
    s"${super.cmdString} => ${next}"
  }

  override def templateString = {
    s"${super.templateString} => ${next}"
  }

}
