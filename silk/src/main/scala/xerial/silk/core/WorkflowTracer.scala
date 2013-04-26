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
import xerial.silk.cluster.{LazyF0, ClosureSerializer}
import xerial.core.log.Logger
import java.lang.reflect.{Modifier, Method}
import scala.language.existentials
import xerial.silk.core.SilkWorkflow.FlowMap
import xerial.silk.core.SilkWorkflow.ShellCommand
import scala.Some
import xerial.silk.core.SilkWorkflow.CommandSeq

trait FunctionRef {
  def name : String
}
case class Function2Ref[A, B](name:String, f:A=>B) extends FunctionRef
case class MethodRef(cl:Class[_], opcode:Int, flags:Int, name:String, desc:String, stack:IndexedSeq[String]) extends FunctionRef {
  override def toString = s"MethodRef(cl:${cl.getName}, name:${name}, desc:${desc}, stack:[${stack.mkString(", ")}])"
  def toReportString = s"MethodCall[$opcode]:${name}${desc}\n -owner:${cl.getName}${if(stack.isEmpty) "" else "\n -stack:\n  -" + stack.mkString("\n  -")}"

}

case class SilkDependency(ref:FunctionRef, method:IndexedSeq[MethodRef]) {
  override def toString = s"${ref.name} =>\n${method.map(_.toString).mkString("\n")}}"
}

case class DependencyGraph(nodes:Set[MethodRef], edges:Map[MethodRef, MethodRef]) extends Logger {
  override def toString = {
    var s = Seq.newBuilder[String]
    for((from, to) <- edges) {
      s += s"${from.name} -> ${to.name}"
    }
    s"[${s.result.mkString(", ")}]"
  }


  def addEdge(from:MethodRef, to:MethodRef) : DependencyGraph = {
    debug(s"add edge: ${from.name} -> ${to.name}")
    DependencyGraph(nodes + from + to, edges + (from -> to))
  }

}



/**
 *
 * @author Taro L. Saito
 */
object WorkflowTracer extends Logger {

  import ClosureSerializer._

  def dependencyGraph[R](f0: => R) : DependencyGraph = {
    val finder = new DependencyFinder
    finder.traceSilkFlow(f0)
  }

  private class DependencyFinder {
    var g = DependencyGraph(Set.empty, Map.empty)

    private def findRelatedMethodCallIn(m:MethodRef) {
      for(mc <- traceMethodCall(m)){// if ! mc.cl.getName.contains("xerial.silk.core.Silk")) {
        g = g.addEdge(mc, m)
      }
    }

    /**
     * Create the dependency graph of functions
     * @param f0
     * @tparam R
     * @return
     */
    def traceSilkFlow[R](f0: => R) : DependencyGraph = {
      val lazyf0 = LazyF0(f0)
      val funcClass = lazyf0.functionClass
      for(m <- resolveMethodCalledByFunctionRef(funcClass)) {
        findRelatedMethodCallIn(m)
        val eval = f0
        if(classOf[SilkFlow[_, _]].isAssignableFrom(eval.getClass))
          traceSilkFlow(m, eval.asInstanceOf[SilkFlow[_, _]])
      }
      g
    }

    def traceSilkFlowF0[R](contextMethod:MethodRef, f0:LazyF0[R]) {
      val funcClass = f0.functionClass
      debug(s"traceSilkFlowF0 ${funcClass.getName}")
      for(m <- resolveMethodCalledByFunctionRef(funcClass)) {
        g.addEdge(m, contextMethod)
      }
      debug("done traceSilkFlowF0")
    }

      /**
     * Trace silk flow dependencies appearing inside the function
     * @param f
     * @tparam P
     * @tparam Q
     */
    def traceSilkFlow[P, Q](contextMethod:MethodRef, f:P => Q)  {
      for(m <- resolveMethodCalledByFunctionRef(f.getClass)) {
        g = g.addEdge(contextMethod, m)
        findRelatedMethodCallIn(m)
      }
    }

    private def traceMethodCall[A](mr:MethodRef) : Seq[MethodRef] = {
      val methodName = mr.name
      val cl = mr.cl
      debug(s"traceMethodCall:${cl.getName}:$methodName")

      val argTypes = Type.getArgumentTypes(mr.desc)
      val mc = findMethodCall(mr.cl, methodFilter(mr.name, argTypes.length))
      debug(s"Found method calls: ${mc.map(_.name).mkString(", ")}")
      val result = filterByContextClasses(mr.cl, mc)
      result
    }

    private def traceSilkFlow[A, B](contextMethod:MethodRef, silk:SilkFlow[A, B]) {

      def findFromArg[T](arg:T) : Unit = {
        val cl = arg.getClass
        if(isSilkType(cl)) {
          debug(s"arg: $arg, type:${arg.getClass}")
          find(arg.asInstanceOf[Silk[_]])
        }
      }

      def find[P](current:Silk[P]) : Unit = {
        trace(s"find dependency in ${current.getClass.getName}")
        current match {
          case CommandSeq(prev, next) =>
            find(prev)
            find(next)
          case sc : ShellCommand =>
            sc.argSeq.foreach(arg => findFromArg(arg))
          case FlowMap(prev, f) =>
            find(prev)
            traceSilkFlow(contextMethod, f)
          case Filter(prev, pred) =>
            find(prev)
            traceSilkFlow(contextMethod, pred)
          case r:RootWrap[_] =>
            traceSilkFlowF0(contextMethod, r.lazyF0)
          case f:SilkFile => Seq.empty
          case _ =>
            warn(s"unknown flow type: $current")
            Seq.empty
        }
      }

      debug(s"traceSilkFlow name:${contextMethod.name}, $silk")
      find(silk)
    }


  }






  /**
   * Extract method that matches the given name and returns SilkSingle or Silk type data
   * @param name
   * @param argLen
   * @return
   */
  private def methodFilter(name:String, argLen:Int) : MethodRef => Boolean = { m : MethodRef =>
    def isTarget : Boolean = m.name == name && Type.getArgumentTypes(m.desc).length == argLen
    def isSilkFlow : Boolean = {
      try {
        val rt = Class.forName(Type.getReturnType(m.desc).getClassName, false, Thread.currentThread.getContextClassLoader)
        isSilkType(rt)
      }
      catch {
        case e:ClassNotFoundException => false
      }
    }
    isTarget && isSilkFlow
  }

  private def isSilkType[A](cl:Class[A]) : Boolean = {
    return classOf[Silk[_]].isAssignableFrom(cl)
  }



  private def resolveMethodCalledByFunctionRef[A](funcClass:Class[A]) : Option[MethodRef] = {
    def isSynthetic(flags:Int) = (flags & 0x1000) != 0

    val mc = findMethodCall(funcClass, {m :MethodRef =>
      m.name == "apply" && !isSynthetic(m.flags)
    })
    mc.headOption
  }


  def findMethodCall[A](cl:Class[A], methodFilter:MethodRef => Boolean) : Seq[MethodRef] = {
    def getClassReader(cl: Class[_]): ClassReader = {
      new ClassReader(cl.getResourceAsStream(
        cl.getName.replaceFirst("^.*\\.", "") + ".class"))
    }

    val visitor = new ClassTracer(cl, methodFilter)
    getClassReader(cl).accept(visitor, 0)
    visitor.found.toSeq
  }

  /**
   * Extract method calls within the scope of a given class
   * @param cl
   * @param methodCall
   * @tparam A
   * @return
   */
  def filterByContextClasses[A](cl:Class[A], methodCall:Seq[MethodRef]) : Seq[MethodRef] = {
    val contextClasses = {
      val parents =
        for(p <- Seq(cl.getSuperclass) ++ cl.getInterfaces
            if !p.getName.startsWith("scala.") && !p.getName.startsWith("java.")) yield p.getName

      parents.toSet + cl.getName  + "xerial.silk.core"
    }
    def isTargetClass(clName:String) : Boolean =
      contextClasses.exists(c => clName.startsWith(c))

    def isTarget(m:MethodRef) : Boolean = {
      if(m.name == "<init>")
        return false
      val argTypes = Type.getArgumentTypes(m.desc)
      val argLength = argTypes.length
      m.opcode match {
        case Opcodes.INVOKESTATIC =>
          // v1, v2, ...
          val args = m.stack.takeRight(argLength)
          args exists isTargetClass
        case _ =>
          // o, v1, v2, ...
          val args = m.stack.takeRight(argLength+1)
          args exists isTargetClass
      }
    }

    methodCall filter isTarget
  }


  class ClassTracer[A](cl:Class[A], cond: MethodRef => Boolean) extends ClassVisitor(Opcodes.ASM4) {
    val owner = cl.getName
    var found = Set[MethodRef]()

    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
      if(cond(MethodRef(cl, -1, access, name, desc, IndexedSeq.empty))) {
        trace(s"Searching method ${name}${desc} in class $owner")
        new MethodTracer(access, name, desc, signature, exceptions)
      }
      else
        null
    }

    class MethodTracer(access:Int, name:String, desc:String, signature:String, exceptions:Array[String])
      extends MethodVisitor(Opcodes.ASM4, new MethodNode(Opcodes.ASM4, access, name, desc, signature, exceptions)) {
      override def visitEnd() {
        super.visitEnd()
        //trace(s"analyze method ${name}${desc}")
        try {
          val a = new Analyzer(new SimpleVerifier)
          val mn = mv.asInstanceOf[MethodNode]
          a.analyze(owner, mn)
          val inst = for(i <- 0 until mn.instructions.size()) yield mn.instructions.get(i)
          for((f, m:MethodInsnNode) <- a.getFrames.zip(inst) if f != null) {
            val stack = (for (i <- 0 until f.getStackSize) yield f.getStack(i).asInstanceOf[BasicValue].getType.getClassName).toIndexedSeq
            val ownerName = clName(m.owner)
            val methodOwner = Class.forName(ownerName, false, Thread.currentThread.getContextClassLoader)
            val ref = MethodRef(methodOwner, m.getOpcode, access, m.name, m.desc, stack)
            trace(s"Found ${ref.toReportString}")
            // Search also anonymous functions defined in the target class
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