//--------------------------------------
//
// WorkflowTracer.scala
// Since: 2013/04/16 3:53 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.core.SilkWorkflow.{FlowMap, SilkFlow}
import org.objectweb.asm._
import tree.analysis.{BasicValue, Analyzer, SimpleVerifier}
import tree.{MethodInsnNode, MethodNode}
import xerial.silk.cluster.ClosureSerializer
import xerial.core.log.Logger
import java.lang.reflect.Method
import xerial.lens.TypeUtil
import scala.language.existentials

trait FunctionRef {
  def name : String
}
case class Function2Ref[A, B](name:String, f:A=>B) extends FunctionRef
case class MethodRef(cl:Class[_], name:String, desc:String) extends FunctionRef

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
    cl match {
      case c if c == classOf[SilkSingle[_]] => true
      case c if c == classOf[Silk[_]] => true
      case _ => false
    }
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


  def traceSilkFlow[A](cl:Class[A], methodName:String) : Option[SilkDependency] = {
    cl.getDeclaredMethods.find(m => m.getName == methodName && isSilkType(m.getReturnType)).flatMap { m : Method =>
      m.getReturnType match {
        case c if c.isAssignableFrom(classOf[FlowMap[_, _]]) && m.getParameterTypes.length == 0 =>
          val flow = m.invoke(TypeUtil.companionObject(cl).getOrElse(null)).asInstanceOf[FlowMap[_, _]]
          traceSilkFlow(methodName, flow.f)
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


  /**
   * Trace silk flow dependencies appearing inside the function
   * @param f
   * @tparam P
   * @tparam Q
   */
  def traceSilkFlow[P, Q](name:String, f:P => Q) : Option[SilkDependency] = {
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

    var found = List[MethodRef]()

    debug(s"trace $owner")

    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {

      if(cond(MethodRef(cl, name, desc)))
        new MethodTracer(access, name, desc)
      else
        null
    }

    class MethodTracer(access:Int, name:String, desc:String) extends MethodVisitor(Opcodes.ASM4, new MethodNode(Opcodes.ASM4, access, name, desc, null, null)) {
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
            val methodOwner = Class.forName(clName(m.owner), false, Thread.currentThread.getContextClassLoader)
            val ref = MethodRef(methodOwner, m.name, m.desc)
            found = ref :: found
          }
        }
        catch {
          case e:Exception => warn(e)
        }
      }
    }

  }






}