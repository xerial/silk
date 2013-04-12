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
import org.objectweb.asm.tree.MethodNode
import org.objectweb.asm.tree.analysis._
import org.objectweb.asm.commons.AnalyzerAdapter

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
    trace("cleanup closure")
    val cl = f.functionClass
    debug(s"closure class: $cl")

    val outer = getOuterObjects(f.functionInstance, f.functionClass)
    debug(s"outer: [${outer.mkString(", ")}]")

//    val inner = findInnerFieldAccess(f.functionClass)
//    debug(s"inner: [${inner.mkString(", ")}}]")

    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, {
      val finder = new FieldAccessFinder(cl)
      finder.findFrom(cl)
    })
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
    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, {
      val finder = new FieldAccessFinder(cl)
      finder.findFrom(cl)
    })
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

  trait InsnWithFrame
  case class ReturnInsn(opcode:Int) extends InsnWithFrame
  case class MethodInsn(opcode:Int, owner:String, name:String, desc:String) extends InsnWithFrame

  private class FieldAccessFinder(target: Class[_]) {

    private val targetClassDesc = descName(target.getName)
    private var visitedMethod = Set.empty[String]
    private var contextMethods = List("apply()V")

    private val accessedFields = collection.mutable.Map[String, Set[String]]()

    private def contextMethod = contextMethods.head

    private def isPrimitive(cl: Class[_]) = {
      if (cl.isPrimitive)
        true
      else {
        cl.getName match {
          case "java.lang.Integer" => true
          case "java.lang.Short" => true
          case "java.lang.Long" => true
          case "java.lang.Character" => true
          case "java.lang.Boolean" => true
          case "java.lang.Float" => true
          case "java.lang.Double" => true
          case "java.lang.Byte" => true
          case "java.lang.String" => true
          case "scala.Predef$" => true
          case _ => false
        }
      }
    }

    def findFrom(cl: Class[_]): Map[String, Set[String]] = {
      //debug(s"find from ${cl.getName}, target method:${contextMethod}")
      if (visitedMethod.contains(contextMethod) || isPrimitive(cl))
        return Map.empty

      val visitor = new ClassVisitor(Opcodes.ASM4) {
        var currentName: String = _

        override def visit(version: Int, access: Int, name: String, signature: String, superName: String, interfaces: Array[String]) {
          info(s"visit class ${clName(name)}, contextMethod:${contextMethods.head}")
          currentName = name
        }

        override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
          val fullDesc = s"${name}${desc}"
          //trace(s"visitMethod $fullDesc")
          if (fullDesc != contextMethods.head)
            null // empty visitor
          else {
            debug(s"[target] visit method ${name}, desc:${desc}")
            new MethodVisitor(Opcodes.ASM4, new MethodNode(access, name, desc, null, null)) {

              val ib = IndexedSeq.newBuilder[InsnWithFrame]

              override def visitEnd() {
                info(s"method analysis: $name")
                val mn = mv.asInstanceOf[MethodNode]
                val a = new Analyzer(new SimpleVerifier())
                try {
                  val ret = Type.getReturnType(desc)
                  a.analyze(ret.getClassName, mn)
                  val instructions = ib.result
                  debug(s"instruction size: ${instructions.size}")
                  val frames : Array[Frame] = a.getFrames
                  for((f, i) <- frames.zipWithIndex) {
                    val stack = for(i <- 0 until f.getStackSize) yield f.getStack(i).asInstanceOf[BasicValue].getType.getDescriptor
                    val local = for(i <- 0 until f.getLocals) yield f.getLocal(i)
                    val inst = instructions(i)
                    // TODO inst.accept(.. )
                    info(s"[${inst}] frame[$i] stack:${stack.mkString(", ")}")
                  }
                }
                catch {
                  case e :Exception => error(e)
                }
              }

              override def visitFieldInsn(opcode: Int, owner: String, name: String, desc: String) {
                if (opcode == Opcodes.GETFIELD || opcode == Opcodes.GETSTATIC) {
                  trace(s"visit field insn: $opcode name:$name, owner:$owner desc:$desc")
                  val ownerCl = clName(owner)
                  if (cl.getName.contains(ownerCl)) {
                    val newSet = accessedFields.getOrElseUpdate(ownerCl, Set.empty[String]) + name
                    debug(s"Found an accessed field: $name in class $ownerCl: $newSet")
                    accessedFields += ownerCl -> newSet
                  }
                }
              }

              override def visitInsn(opcode:Int) {
                if(Opcodes.IRETURN <= opcode && opcode <= Opcodes.RETURN) {
                  ib += ReturnInsn(opcode)
                }
                super.visitInsn(opcode)
              }

              override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
                ib += MethodInsn(opcode, owner, name, desc)

                if (opcode == Opcodes.INVOKEVIRTUAL || opcode == Opcodes.INVOKESTATIC) {

                  val ret = Type.getReturnType(desc)
                  trace(s"visit invokevirtual: ${name}$desc, ret:$ret")
                  // return type
//                  if(ret.getSort == Type.OBJECT) {
//                    argStack = ret.getClass :: argStack
//                    debug("arg stack :" + argStack.mkString(", "))
//                  }
                  //info(s"Find the target function: $name")
                  val ownerCls = Class.forName(clName(owner))
                  contextMethods = s"${name}${desc}" :: contextMethods
                  //findFrom(ownerCls)
                  contextMethods = contextMethods.tail
                }
                else if (opcode == Opcodes.INVOKEINTERFACE) {
                  trace(s"visit invokeinterface $opcode : ${name}$desc in\nclass ${clName(owner)}")
                  val ownerCls = Class.forName(clName(owner))
                  contextMethods = s"${name}${desc}" :: contextMethods
                  // TODO resolve class implementing this interface
                  //findFrom(ownerCls)
                  contextMethods = contextMethods.tail
                }
                else if (opcode == Opcodes.INVOKESPECIAL) {
                  debug(s"visit invokespecial: $opcode ${name}, desc:$desc in\nclass ${clName(owner)}")
                }
              }
            }
          }
        }
      }
      visitedMethod += contextMethod

      getClassReader(cl).accept(visitor, ClassReader.SKIP_DEBUG)

      accessedFields.toMap
    }
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