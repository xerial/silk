/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// ClosureSerializer.scala
// Since: 2012/12/28 0:22
//
//--------------------------------------

package xerial.silk.cluster

import java.io._
import xerial.silk.core.Silk
import java.lang.reflect.Constructor
import org.objectweb.asm.{MethodVisitor, Opcodes, ClassVisitor, ClassReader, Type}
import collection.mutable.Set
import xerial.core.log.Logger
import xerial.silk.core.SilkSerializer.ObjectDeserializer
import xerial.core.util.DataUnit
import scala.language.existentials
import org.objectweb.asm.tree.{InsnNode, MethodInsnNode, VarInsnNode, MethodNode}
import org.objectweb.asm.tree.analysis._
import org.objectweb.asm.commons.AnalyzerAdapter
import xerial.silk.cluster.asm.NonClassloadingSimpleVerifier

object LazyF0 {
  def apply[R](f: => R) = new LazyF0(f)
}


/**
 * This class is used to obtain the class names of the call-by-name functions (Function0[R]).
 *
 * This wrapper do not directly access the field f (Function0[R]) in order
 * to avoid the evaluation of the function.
 * @param f
 * @tparam R
 */
class LazyF0[R](f: => R) {

  /**
   * Obtain the function class
   * @return
   */
  def functionClass: Class[_] = {
    val field = this.getClass.getDeclaredField("f")
    field.get(this).getClass
  }

  def functionInstance: Function0[R] = {
    this.getClass.getDeclaredField("f").get(this).asInstanceOf[Function0[R]]
  }

  /**
   * We never use this method, but this definition is necessary in order to let the compiler generate the private field 'f' that
   * holds a reference to the call-by-name function.
   * @return
   */
  def eval = f
}


/**
 * Closure serializer
 *
 * @author Taro L. Saito
 */
private[silk] object ClosureSerializer extends Logger {

  private val accessedFieldTable = collection.mutable.Map[Class[_], Map[String, Set[String]]]()

  case class OuterObject(obj: AnyRef, cl: Class[_]) {
    override def toString = s"${cl.getName}"
  }

  private def isClosure(cl: Class[_]) = cl.getName.contains("$anonfun$")

  private def getOuterObjects(obj: AnyRef, cl: Class[_]): List[OuterObject] = {
    for (f <- cl.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      val e = OuterObject(outer, f.getType)
      if (isClosure(e.cl))
        return e :: getOuterObjects(e.obj, e.cl)
      else
        return e :: Nil
    }
    return Nil
  }


  def cleanupClosure[R](f: LazyF0[R]) = {
    val cl = f.functionClass
    trace(s"cleanup closure class: $cl")

//    val outer = getOuterObjects(f.functionInstance, f.functionClass)
//    debug(s"outer: [${outer.mkString(", ")}]")

//    val inner = findInnerFieldAccess(f.functionClass)
//    debug(s"inner: [${inner.mkString(", ")}}]")

    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, findAccessedFieldsInClosure(cl))
    debug(s"accessed fields: ${accessedFields.mkString(", ")}")

    // cleanup unused fields recursively
    val obj_clean = cleanupObject(f.functionInstance, f.functionClass, accessedFields)
    obj_clean
  }

  private def cleanupObject(obj: AnyRef, cl: Class[_], accessedFields: Map[String, Set[String]]) = {
    debug(s"cleanup object: class ${cl.getName}")
    if (cl.isPrimitive)
      obj
    else
      obj match {
        case a: scala.runtime.IntRef => obj
        case a: scala.runtime.ShortRef => obj
        case a: scala.runtime.LongRef => obj
        case a: scala.runtime.FloatRef => obj
        case a: scala.runtime.DoubleRef => obj
        case a: scala.runtime.BooleanRef => obj
        case a: scala.runtime.ByteRef => obj
        case a: scala.runtime.CharRef => obj
        case a: scala.runtime.ObjectRef[_] => obj
        case _ => instantiateClass(obj, cl, accessedFields)
      }
  }


  private def instantiateClass(orig: AnyRef, cl: Class[_], accessedFields: Map[String, Set[String]]): Any = {

    val m = classOf[ObjectStreamClass].getDeclaredMethod("getSerializableConstructor", classOf[Class[_]])
    m.setAccessible(true)
    val constructor = m.invoke(null, cl).asInstanceOf[Constructor[_]]
    if (constructor == null) {
      trace(s"use the original: ${orig.getClass.getName}")
      orig
    }
    else {
      trace("create a blanc instance")
      val obj = constructor.newInstance()
      // copy accessed fields
      val clName = cl.getName
      for (accessed <- accessedFields.getOrElse(clName, Set.empty)) {
        try {
          val f = orig.getClass.getDeclaredField(accessed)
          f.setAccessible(true)
          trace(s"set field $accessed:${f.getType.getName} in $clName")
          val v = f.get(orig)
          val v_cleaned = cleanupObject(v, f.getType, accessedFields)
          f.set(obj, v_cleaned)
        }
        catch {
          case e: NoSuchFieldException =>
            warn(s"no such field: $accessed in class ${cl.getName}")
        }
      }
      obj
    }
  }

  def accessedFieldsInClosure[A, B](target: Class[_], closure: Function[A, B]): Seq[String] = {
    new ParamAccessFinder(target).findFrom(closure)
  }


  /**
   * Find the accessed parameters of the target class in the closure.
   * This function is used for optimizing data retrieval in Silk.
   * @param target
   * @param closure
   * @return
   */
  def accessedFields(target: Class[_], closure: AnyRef): Seq[String] = {
    new ParamAccessFinder(target).findFrom(closure.getClass)
  }

  def accessedFieldsIn[R](f: => R) = {
    val lf = LazyF0(f)
    val cl = lf.functionClass
    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, findAccessedFieldsInClosure(cl))
    debug(s"accessed fields: ${accessedFields.mkString(", ")}")
    accessedFields
  }

  def serializeClosure[R](f: => R): Array[Byte] = {
    val lf = LazyF0(f)
    trace(s"Serializing closure class ${lf.functionClass}")
    val clean = cleanupClosure(lf)
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(clean)
    //o.writeObject(lf.functionInstance)
    o.flush()
    o.close
    b.close
    val ser = b.toByteArray
    trace(s"closure size: ${DataUnit.toHumanReadableFormat(ser.length)}")
    ser
  }

  def deserializeClosure(b: Array[Byte]): AnyRef = {
    val in = new ObjectDeserializer(new ByteArrayInputStream(b))
    val ret = in.readObject()
    in.close()
    ret
  }


  private def getClassReader(cl: Class[_]): ClassReader = {
    new ClassReader(cl.getResourceAsStream(
      cl.getName.replaceFirst("^.*\\.", "") + ".class"))
  }

  private def descName(s: String) = s.replace(".", "/")

  private def clName(s: String) = s.replace("/", ".")

  private class ParamAccessFinder(target: Class[_]) {

    private val targetClassDesc = descName(target.getName)
    private var visitedClass = Set.empty[Class[_]]
    private var currentTarget = List("apply")

    private var accessedFields = Seq.newBuilder[String]
    trace(s"targetClass:${clName(target.getName)}")

    def findFrom[A, B](closure: Function[A, B]): Seq[String] = {
      info(s"findFrom closure:${closure.getClass.getName}")
      findFrom(closure.getClass)
    }

    def findFrom(cl: Class[_]): Seq[String] = {
      if (visitedClass.contains(cl))
        return Seq.empty

      val visitor = new ClassVisitor(Opcodes.ASM4) {
        override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
          if (desc.contains(targetClassDesc) && name.contains(currentTarget.head)) {
            debug(s"visit method ${
              name
            }${desc} in ${clName(cl.getName)}")
            new MethodVisitor(Opcodes.ASM4) {
              override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
                if (opcode == Opcodes.INVOKEVIRTUAL) {

                  trace(s"visit invokevirtual: $opcode ${
                    name
                  }$desc in $owner")
                  if (clName(owner) == target.getName) {
                    debug(s"Found a accessed parameter: $name")
                    accessedFields += name
                  }

                  //info(s"Find the target function: $name")
                  val ownerCls = Class.forName(clName(owner))
                  currentTarget = name :: currentTarget
                  findFrom(ownerCls)
                  currentTarget = currentTarget.tail
                }
              }
            }
          }
          else
            new MethodVisitor(Opcodes.ASM4) {} // empty visitor
        }
      }
      visitedClass += cl
      getClassReader(cl).accept(visitor, 0)

      accessedFields.result
    }

  }

  case class MethodCall(opcode:Int, name:String, desc:String, owner:String, stack:IndexedSeq[String]) {
    def methodDesc = s"$name$desc"
    override def toString = s"MethodCall[$opcode]($name$desc, owner:$owner, stack:[${stack.mkString(", ")}])"
    def toReportString = s"MethodCall[$opcode]:$name$desc\n -owner:$owner${if(stack.isEmpty) "" else "\n -stack:\n  -" + stack.mkString("\n  -")}"
  }

  private[cluster] class ClassScanner(owner:String, targetMethod:String, argStack:IndexedSeq[String]) extends ClassVisitor(Opcodes.ASM4)  {

    info(s"Scanning method:$targetMethod, owner:$owner, stack:$argStack)")

    var accessedFields = Map[String, Set[String]]()
    var found = List[MethodCall]()

    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
      val fullDesc = s"${name}${desc}"
      if (fullDesc != targetMethod)
        null // empty visitor
      else {
        new MethodScanner(access, name, desc, signature, exceptions)
      }
    }

    class MethodScanner(access:Int ,name:String, desc:String, signature:String, exceptions:Array[String])
      extends MethodVisitor(Opcodes.ASM4, new MethodNode(Opcodes.ASM4, access, name, desc, signature, exceptions)) {

      //info(s"visit method $name in class $owner")

      override def visitFieldInsn(opcode: Int, fieldOwner: String, name: String, desc: String) {
        super.visitFieldInsn(opcode, fieldOwner, name, desc)
        if (opcode == Opcodes.GETFIELD || opcode == Opcodes.GETSTATIC) {
          //trace(s"visit field insn: $opcode name:$name, owner:$owner desc:$desc")
          //if (owner.contains(fieldOwner)) {
          debug(s"Found an accessed field: $name in class $owner")
          accessedFields += fieldOwner -> (accessedFields.getOrElse(owner, Set.empty) + name)
          //}
        }
      }


      override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
        super.visitMethodInsn(opcode, owner, name, desc)
        //trace(s"visit method: $opcode $name$desc, owner:$owner")
        if(opcode == Opcodes.ALOAD) {

        }
      }

      override def visitEnd() {
        super.visitEnd()
        try {
          val a = new Analyzer(new SimpleVerifier())
          val mn = mv.asInstanceOf[MethodNode]
          trace(s"analyze: owner:$owner, method:$name")
          a.analyze(owner, mn)
          val inst = for(i <- 0 until mn.instructions.size()) yield mn.instructions.get(i)
          trace(s"instructions ${inst.mkString(", ")}")
          for ((f, m:MethodInsnNode) <- a.getFrames.zip(inst) if f != null) {
            val stack = (for (i <- 0 until f.getStackSize) yield f.getStack(i).asInstanceOf[BasicValue].getType.getClassName).toIndexedSeq
            val local = (for (i <- 0 until f.getLocals) yield f.getLocal(i))
            val mc = MethodCall(m.getOpcode, m.name, m.desc, clName(m.owner), stack)
            debug(s"Found ${mc.toReportString}\n -local\n  -${local.mkString("\n  -")}")
            found = mc :: found
          }
        }
        catch {
          case e:Exception => error(e.getMessage)
        }
      }
    }

  }


  def findAccessedFieldsInClosure(cl:Class[_]) = {
    var visited = Set[MethodCall]()
    var stack = List[MethodCall](MethodCall(Opcodes.INVOKEVIRTUAL, "apply", "()V", cl.getName, IndexedSeq(cl.getName)))
    var accessedFields = Map[String, Set[String]]()
    while(!stack.isEmpty) {
      val mc = stack.head
      stack = stack.tail
      if(!visited.contains(mc)) {
        visited += mc
        trace(s"current head: $mc")
        val methodObj = mc.stack.headOption getOrElse (mc.owner)
        val scanner = new ClassScanner(methodObj, mc.methodDesc, mc.stack)
        val targetCls = Class.forName(methodObj, false, Thread.currentThread().getContextClassLoader)
        getClassReader(targetCls).accept(scanner, ClassReader.SKIP_DEBUG)
        for((cls, lst) <- scanner.accessedFields) {
          accessedFields += cls -> (accessedFields.getOrElse(cls, Set.empty) ++ lst)
        }
        for(m <- scanner.found) {
          stack = m :: stack
        }
      }
    }
    accessedFields
  }

  private def findInnerFieldAccess(cl:Class[_]) : Set[Class[_]] = {
    val f = new InnerFieldAccessFinder(cl)
    f.find
    f.found
  }


  private class InnerFieldAccessFinder(cl:Class[_]) {
    var found = Set[Class[_]]()
    var stack = List[Class[_]](cl)

    private class InitDefScanner extends ClassVisitor(Opcodes.ASM4) {
      var current : String = _
      override def visit(version: Int, access: Int, name: String, signature: String, superName: String, interfaces: Array[String]) {
        info(s"visit $name")
        current = name
      }

      override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) =
        new MethodVisitor(Opcodes.ASM4) {
          override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
            val argTypes = Type.getArgumentTypes(desc)
            debug(s"visit invokespecial: $opcode ${name}, desc:$desc, argTypes:(${argTypes.mkString(",")}) in\nclass ${clName(owner)}")
            if(name == "<init>"
              && argTypes.length > 0
              && argTypes(0).toString.startsWith("L") // for object?
              && argTypes(0).getInternalName == current) {

              val ownerCl = clName(owner)
              info(s"push accessed class: $ownerCl")
              stack = Class.forName(ownerCl, false, Thread.currentThread().getContextClassLoader) :: stack
            }
          }
        }
    }

    def find {
      debug("Scanning inner classes")
      while(!stack.isEmpty) {
        val scanner = new InitDefScanner
        val c = stack.head
        if(!found.contains(c)) {
          found += c
          getClassReader(c).accept(scanner, 0)
        }
        stack = stack.tail
      }
    }
  }


}