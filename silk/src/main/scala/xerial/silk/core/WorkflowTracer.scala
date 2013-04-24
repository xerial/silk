//--------------------------------------
//
// WorkflowTracer.scala
// Since: 2013/04/16 3:53 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.core.SilkWorkflow._
import org.objectweb.asm._
import tree.analysis.{BasicValue, Analyzer, SimpleVerifier}
import tree.{MethodInsnNode, MethodNode}
import xerial.silk.cluster.ClosureSerializer
import xerial.core.log.Logger
import java.lang.reflect.Method
import xerial.lens.TypeUtil
import scala.language.existentials
import xerial.silk.core.SilkWorkflow.FlowMap
import xerial.silk.core.SilkWorkflow.ShellCommand
import scala.Some
import xerial.silk.core.SilkWorkflow.CommandSeq

trait FunctionRef {
  def name : String
}
case class Function2Ref[A, B](name:String, f:A=>B) extends FunctionRef
case class MethodRef(cl:Class[_], name:String, desc:String) extends FunctionRef {
  override def toString = s"MethodRef(cl:$cl, name:${name}, desc:${desc})"
}

case class SilkDependency(ref:FunctionRef, method:IndexedSeq[MethodRef]) {
  override def toString = s"${ref.name} => ${method.map(_.toString).mkString(", ")}}"
}


/**
 *
 * @author Taro L. Saito
 */
object WorkflowTracer extends Logger {

  import ClosureSerializer._

  /**
   * Extract method that matches the given name and returns SilkSingle or Silk type data
   * @param name
   * @param argLen
   * @return
   */
  private def methodFilter(name:String, argLen:Int) : MethodRef => Boolean = { m : MethodRef =>
    def isTarget : Boolean = m.name == name && Type.getArgumentTypes(m.desc).length == argLen
    def isSilkFlow : Boolean = {
      val rt = Class.forName(Type.getReturnType(m.desc).getClassName, false, Thread.currentThread.getContextClassLoader)
      debug(s"return type: $rt")
      isSilkType(rt)
    }
    isTarget && isSilkFlow
  }

  private def isSilkType[A](cl:Class[A]) : Boolean = {
    return classOf[Silk[_]].isAssignableFrom(cl)
  }

  def generateSilkFlow[A](cl:Class[A], methodName:String) : Option[Silk[A]] = {
    try {
      for(m <- cl.getDeclaredMethods.find(m => m.getName == methodName && isSilkType(m.getReturnType))) yield {

        val zeroArgs = (for(p <- m.getParameterTypes) yield TypeUtil.zero(p).asInstanceOf[AnyRef]).toSeq
        val co = TypeUtil.companionObject(cl).getOrElse(null)
        val blankSchedule : Silk[A] = (m.invoke(co, zeroArgs:_*)).asInstanceOf[Silk[A]]
        blankSchedule
      }
    }
    catch {
      case e: Exception =>
        trace(e)
        None
    }
  }


  def traceMethodFlow[A](cl:Class[A], methodName:String) : Option[SilkDependency] = {
    cl.getDeclaredMethods.find(m => m.getName == methodName && isSilkType(m.getReturnType)).flatMap { m : Method =>
      m.getReturnType match {
//        case c if c.isAssignableFrom(classOf[FlowMap[_, _]]) && m.getParameterTypes.length == 0 =>
//          val flow = m.invoke(TypeUtil.companionObject(cl).getOrElse(null)).asInstanceOf[FlowMap[_, _]]
//          traceFlow(methodName, flow.f)
        case _ =>
          val visitor = new ClassTracer(cl, methodFilter(m.getName, m.getParameterTypes.length))
          getClassReader(cl).accept(visitor, 0)

          if(visitor.found.isEmpty)
            None
          else
            Some(SilkDependency(MethodRef(cl, methodName, ""), visitor.found.toIndexedSeq))
      }
    }
  }

  def traceSilkFlow[A, B](name:String, silk:SilkFlow[A, B]) : Option[SilkDependency] = {

    def findFromArg[T](fName:String, arg:T) : Seq[SilkDependency] = {
      val cl = arg.getClass
      if(!isSilkType(cl))
        return Seq.empty

      debug(s"arg: $arg")
      find(fName, arg.asInstanceOf[Silk[_]])
    }

    def find[P](fName:String, current:Silk[P]) : Seq[SilkDependency] = {
      current match {
        case CommandSeq(prev, next) =>
          find(fName, prev) ++ find(fName, next)
        case sc : ShellCommand =>
          sc.argSeq.flatMap(arg => findFromArg(fName, arg))
        case f:SilkFile => Seq.empty
        case _ =>
          warn(s"unknown flow type: $current")
          Seq.empty
      }
    }

    debug(s"traceSilkFlow name:$name, $silk")

    find(name, silk)
    None
  }


  /**
   * Trace silk flow dependencies appearing inside the function
   * @param f
   * @tparam P
   * @tparam Q
   */
  def traceFlow[P, Q](name:String, f:P => Q) : Option[SilkDependency] = {
    val cl = f.getClass

    val visitor = new ClassTracer(cl, methodFilter("apply", 1))
    ClosureSerializer.getClassReader(cl).accept(visitor, 0)

    if(visitor.found.isEmpty)
      None
    else
      Some(SilkDependency(Function2Ref(name, f), visitor.found.toIndexedSeq))
  }

  import ClosureSerializer.MethodCall



  class ClassTracer[A](cl:Class[A], cond: MethodRef => Boolean) extends ClassVisitor(Opcodes.ASM4) {
    val owner = cl.getName
    val contextClasses = {
      val parents = for(p <- Seq(cl.getSuperclass) ++ cl.getInterfaces
          if !p.getName.startsWith("scala.") && !p.getName.startsWith("java.")) yield p.getName
      parents.toSet + cl.getName
    }

    var found = Set[MethodRef]()

    debug(s"trace $owner")

    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
      if(cond(MethodRef(cl, name, desc)))
        new MethodTracer(access, name, desc, signature, exceptions)
      else
        null
    }

    class MethodTracer(access:Int, name:String, desc:String, signature:String, exceptions:Array[String]) extends MethodVisitor(Opcodes.ASM4, new MethodNode(Opcodes.ASM4, access, name, desc, signature, exceptions)) {
      override def visitEnd() {
        super.visitEnd()
        debug(s"analyze method ${name}${desc}")
        try {
          val a = new Analyzer(new SimpleVerifier)
          val mn = mv.asInstanceOf[MethodNode]
          a.analyze(owner, mn)
          val inst = for(i <- 0 until mn.instructions.size()) yield mn.instructions.get(i)
          for((f, m:MethodInsnNode) <- a.getFrames.zip(inst) if f != null) {
            val stack = (for (i <- 0 until f.getStackSize) yield f.getStack(i).asInstanceOf[BasicValue].getType.getClassName).toIndexedSeq
            val mc = MethodCall(m.getOpcode, m.name, m.desc, owner, stack)
            debug(s"Found ${mc.toReportString}")
            val ownerName = clName(m.owner)
            val methodOwner = Class.forName(ownerName, false, Thread.currentThread.getContextClassLoader)
            val ref = MethodRef(methodOwner, m.name, m.desc)
            // Search also anonymous functions defined in the target class
            if(contextClasses.exists(c => ownerName.startsWith(c)))
              found += ref
          }
        }
        catch {
          case e:Exception => warn(e)
        }
      }
    }

  }






}