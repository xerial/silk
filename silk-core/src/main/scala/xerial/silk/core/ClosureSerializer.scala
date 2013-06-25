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

package xerial.silk.core

import java.io._
import java.lang.reflect.Constructor
import org.objectweb.asm._
import collection.mutable.Set
import xerial.core.log.Logger
import xerial.silk.core.SilkSerializer.ObjectDeserializer
import xerial.core.util.DataUnit
import scala.language.existentials
import org.objectweb.asm.tree.{InsnNode, MethodInsnNode, VarInsnNode, MethodNode}
import org.objectweb.asm.tree.analysis._
import org.objectweb.asm.commons.AnalyzerAdapter
import xerial.lens.Primitive
import java.util.UUID
import xerial.silk.framework.ops.Silk


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

  def cleanupF1[A, B](f: A=>B) : A=>B = {
    val cl = f.getClass
    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, findAccessedFieldsInClosure(cl))
    debug(s"accessed fields: ${accessedFields.mkString(", ")}")

    // cleanup unused fields recursively
    val obj_clean = cleanupObject(f, cl, accessedFields)
    obj_clean.asInstanceOf[A=>B]
  }


  def cleanupClosure[R](f: LazyF0[R]) = {
    val cl = f.functionClass
    trace(s"cleanup closure class: ${cl.getName}")

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

  def cleanupObject(obj: AnyRef, cl: Class[_], accessedFields: Map[String, Set[String]]) = {
    trace(s"cleanup object: class ${cl.getName}")
    if (cl.isPrimitive || cl.isArray)
      obj
    else
      obj match {
        case op: Silk[_] => obj
        case a: UUID => obj
        // var references (xxxRef) must be serialized
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
      trace("create a blank instance")
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
    debug(f"closure size: ${ser.length}%,d")
    ser
  }

  def deserializeClosure(b: Array[Byte]): AnyRef = {
    val in = new ObjectDeserializer(new ByteArrayInputStream(b))
    val ret = in.readObject()
    in.close()
    ret
  }


  private[silk] def getClassReader(cl: Class[_]): ClassReader = {
    new ClassReader(cl.getResourceAsStream(
      cl.getName.replaceFirst("^.*\\.", "") + ".class"))
  }

  private def descName(s: String) = s.replace(".", "/")

  private[silk] def clName(s: String) = s.replace("/", ".")

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
    def methodDesc = s"${name}${desc}"
    override def toString = s"MethodCall[$opcode](${name}${desc}, owner:$owner, stack:[${stack.mkString(", ")}])"
    def toReportString = s"MethodCall[$opcode]:${name}${desc}\n -owner:${owner}${if(stack.isEmpty) "" else "\n -stack:\n  -" + stack.mkString("\n  -")}"
  }

  private[silk] class ClassScanner(owner:String, targetMethod:String, opcode:Int, argStack:IndexedSeq[String]) extends ClassVisitor(Opcodes.ASM4)  {

    trace(s"Scanning method [$opcode] $targetMethod, owner:$owner, stack:$argStack)")


    var accessedFields = Map[String, Set[String]]()
    var found = List[MethodCall]()

    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
      val fullDesc = s"${name}${desc}"

      if (fullDesc != targetMethod)
        null // empty visitor
      else {
        trace(s"visit method: $fullDesc")
        // Replace method descriptor to use argStack variables in Analyzer
        // TODO return type might need to be changed
        val ret = Type.getReturnType(desc)
        def toDesc(t:String) = {
          try {
            t match {
              case arr if arr.endsWith("[]") =>
                arr match {
                  case "byte[]" => "[B"
                  case "int[]" => "[I"
                  case "float[]" => "[F"
                  case "boolean[]" => "[B"
                  case "long[]" => "[J"
                  case "double[]" => "[D"
                  case _ => t
                }
              case _ =>
                val cl = Class.forName(t, false, Thread.currentThread().getContextClassLoader)
                Type.getDescriptor(cl)
            }
          }
          catch {
            case e : Exception => t
          }
        }

        val newArg = opcode match {
          case Opcodes.INVOKESTATIC => argStack.map(toDesc).mkString
          case _ => if(argStack.length > 1) argStack.drop(1).map(toDesc).mkString else ""
        }
        trace(s"Replace desc\nold:$desc\nnew:(${newArg})$ret")
        val newDesc = s"($newArg)$ret"
        val mn = new MethodNode(Opcodes.ASM4, access, name, newDesc, signature, exceptions)
        new MethodScanner(access, name, desc, signature, exceptions, mn)
      }
    }

    class MethodScanner(access:Int ,name:String, desc:String, signature:String, exceptions:Array[String], m:MethodNode)
      extends MethodVisitor(Opcodes.ASM4, m) {

      override def visitFieldInsn(opcode: Int, fieldOwner: String, name: String, desc: String) {
        super.visitFieldInsn(opcode, fieldOwner, name, desc)
        if (opcode == Opcodes.GETFIELD) { // || opcode == Opcodes.GETSTATIC) {
          //trace(s"visit field insn: $opcode name:$name, owner:$owner desc:$desc")
          val fclName = clName(fieldOwner)
          //if(!fclName.startsWith("scala.") && !fclName.startsWith("xerial.core.")) {
            trace(s"Found an accessed field: $name in class $owner")
            accessedFields += fclName -> (accessedFields.getOrElse(fclName, Set.empty) + name)
          //}
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
          //trace(s"instructions ${inst.mkString(", ")}, # of frames:${a.getFrames.length}")
          for ((f, m:MethodInsnNode) <- a.getFrames.zip(inst) if f != null) {
            val stack = (for (i <- 0 until f.getStackSize) yield f.getStack(i).asInstanceOf[BasicValue].getType.getClassName).toIndexedSeq
            val local = (for (i <- 0 until f.getLocals) yield f.getLocal(i))
            val mc = MethodCall(m.getOpcode, m.name, m.desc, clName(m.owner), stack)
            trace(s"Found ${mc.toReportString}\n -local\n  -${local.mkString("\n  -")}")
            found = mc :: found
          }
        }
        catch {
          case e:Exception => error(e)
        }
      }
    }

  }


  def findAccessedFieldsInClosure(cl:Class[_], methodSig:Seq[String] = Seq("()V", "()Ljava/lang/Object;")) = {
    val baseClsName = cl.getName
    var visited = Set[MethodCall]()
    var stack = methodSig.map(MethodCall(Opcodes.INVOKEVIRTUAL, "apply", _, cl.getName, IndexedSeq(cl.getName))).toList
    var accessedFields = Map[String, Set[String]]()
    while(!stack.isEmpty) {
      val mc = stack.head
      stack = stack.tail
      if(!visited.contains(mc)) {
        visited += mc
        try {
          trace(s"current head: $mc")
          val methodOwner= mc.opcode match {
            case Opcodes.INVOKESTATIC => mc.owner
            case _ => mc.stack.headOption getOrElse (mc.owner)
          }
          val scanner = new ClassScanner(methodOwner, mc.methodDesc, mc.opcode, mc.stack)
          val targetCls = Class.forName(methodOwner, false, Thread.currentThread().getContextClassLoader)
          if(Primitive.isPrimitive(targetCls) || targetCls == classOf[AnyRef] || methodOwner.startsWith("java.")) {
            
          }
          else if (methodOwner.startsWith("scala.")) {
            if(mc.desc.contains("scala/Function1;")) {
              for(anonfun <- mc.stack.filter(x => x.startsWith(baseClsName) && x.contains("$anonfun"))) {
                val m = MethodCall(Opcodes.INVOKESTATIC, "apply", "(Ljava/lang/Object;)Ljava/lang/Object;", anonfun, IndexedSeq(anonfun))
                trace(s"add $m to stack")
                stack = m :: stack
              }
            }
          }
          else {
            getClassReader(targetCls).accept(scanner, ClassReader.SKIP_DEBUG)
            for((cls, lst) <- scanner.accessedFields) {
              accessedFields += cls -> (accessedFields.getOrElse(cls, Set.empty) ++ lst)
            }
            for(m <- scanner.found) {
              stack = m :: stack
            }
          }
        }
        catch {
          case e:Exception => warn(e.getMessage)
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