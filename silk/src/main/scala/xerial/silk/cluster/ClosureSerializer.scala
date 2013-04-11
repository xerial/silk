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
import org.objectweb.asm.{MethodVisitor, Opcodes, ClassVisitor, ClassReader}
import collection.mutable.Set
import xerial.core.log.Logger
import xerial.silk.core.SilkSerializer.ObjectDeserializer
import xerial.core.util.DataUnit

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
  def functionClass : Class[_] = {
    val field = this.getClass.getDeclaredField("f")
    field.get(this).getClass
  }

  def functionInstance : Function0[R] = {
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


  def cleanupClosure[R](f: LazyF0[R]) = {
    trace("cleanup closure")
    val cl = f.functionClass
    info("closure class: %s", cl)

    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, {
      val finder = new FieldAccessFinder(cl)
      finder.findFrom(cl)
    })
    debug("accessed fields: %s", accessedFields.mkString(", "))

    val m = classOf[ObjectStreamClass].getDeclaredMethod("getSerializableConstructor", classOf[Class[_]])
    m.setAccessible(true)
    val constructor = m.invoke(null, cl).asInstanceOf[Constructor[_]]
    val obj = constructor.newInstance()

    // copy accessed fields
    for((clName, lst) <- accessedFields if clName == cl.getName; accessed <- lst) {
      val fld = cl.getDeclaredField(accessed)
      fld.setAccessible(true)
      val v = fld.get(f.functionInstance)
      val v_cleaned = cleanupObject(v, fld.getType, accessedFields)
      fld.set(obj, v_cleaned)
    }

    obj
  }

  def cleanupObject(obj:AnyRef, cl:Class[_]) {

    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, {
      val finder = new FieldAccessFinder(cl)
      finder.findFrom(cl)
    })
    trace("accessed fields: %s", accessedFields.mkString(", "))
    cleanupObject(obj, cl, accessedFields)
  }

  private def cleanupObject(obj:AnyRef, cl:Class[_], accessedFields:Map[String, Set[String]]) = {
    val clone = instantiateClass(obj, cl, accessedFields)
    clone
  }

  def instantiateClass(orig:AnyRef, cl:Class[_], accessedFields:Map[String, Set[String]]) : Any = {
    val m = classOf[ObjectStreamClass].getDeclaredMethod("getSerializableConstructor", classOf[Class[_]])
    m.setAccessible(true)
    val constructor = m.invoke(null, cl).asInstanceOf[Constructor[_]]
    val obj = constructor.newInstance()

    // copy accessed fields
    for((clName, lst) <- accessedFields if clName == cl.getName; accessed <- lst) {
      val f = orig.getClass.getDeclaredField(accessed)
      f.setAccessible(true)
      val v = f.get(orig)
      val v_cleaned = cleanupObject(v, f.getType, accessedFields)
      f.set(obj, v_cleaned)
    }
    obj
  }


  def accessedFieldsInClosure[A, B](target:Class[_], closure:Function[A, B]) : Seq[String] = {
    new ParamAccessFinder(target).findFrom(closure)
  }


  /**
   * Find the accessed parameters of the target class in the closure.
   * This function is used for optimizing data retrieval in Silk.
   * @param target
   * @param closure
   * @return
   */
  def accessedFields(target:Class[_], closure:AnyRef) : Seq[String] = {
    new ParamAccessFinder(target).findFrom(closure.getClass)
  }

  def accessedFieldsIn[R](f: => R) = {
    val lf = LazyF0(f)
    val cl = lf.functionClass
    val accessedFields = accessedFieldTable.getOrElseUpdate(cl, {
      val finder = new FieldAccessFinder(cl)
      finder.findFrom(cl)
    })
    debug("accessed fields: %s", accessedFields.mkString(", "))
    accessedFields
  }

  def serializeClosure[R](f: => R) = {
    val lf = LazyF0(f)
    trace("Serializing closure class %s", lf.functionClass)
    val clean = cleanupClosure(lf)
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(clean)
    o.flush()
    o.close
    b.close
    val ser = b.toByteArray
    trace("closure size: %s", DataUnit.toHumanReadableFormat(ser.length))
    ser
  }

  def deserializeClosure(b:Array[Byte]) : AnyRef = {
    val in = new ObjectDeserializer(new ByteArrayInputStream(b))
    val ret = in.readObject()
    in.close()
    ret
  }





  private def getClassReader(cl: Class[_]): ClassReader = {
    new ClassReader(cl.getResourceAsStream(
      cl.getName.replaceFirst("^.*\\.", "") + ".class"))
  }

  private def descName(s:String) = s.replace(".", "/")
  private def clName(s:String) = s.replace("/", ".")

  private class ParamAccessFinder(target:Class[_]) {

    private val targetClassDesc = descName(target.getName)
    private var visitedClass = Set.empty[Class[_]]
    private var currentTarget = List("apply")

    private var accessedFields = Seq.newBuilder[String]
    trace(s"targetClass:${clName(target.getName)}")

    def findFrom[A, B](closure:Function[A, B]) : Seq[String] = {
      info(s"findFrom closure:${closure.getClass.getName}")
      findFrom(closure.getClass)
    }

    def findFrom(cl:Class[_]) : Seq[String] = {
      if(visitedClass.contains(cl))
        return Seq.empty

      val visitor = new ClassVisitor(Opcodes.ASM4) {
        override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
          if(desc.contains(targetClassDesc) && name.contains(currentTarget.head)) {
            debug(s"visit method ${name}${desc} in ${clName(cl.getName)}")
            new MethodVisitor(Opcodes.ASM4) {
              override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
                if(opcode == Opcodes.INVOKEVIRTUAL) {

                  trace(s"visit invokevirtual: $opcode ${name}$desc in $owner")
                  if(clName(owner) == target.getName) {
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



  private class FieldAccessFinder(target:Class[_]) extends ClassVisitor(Opcodes.ASM4) {

    private val targetClassDesc = descName(target.getName)
    private var visitedMethod = Set.empty[String]
    private var contextMethods = List("apply()V")

    private val accessedFields = collection.mutable.Map[String, Set[String]]()

    private def contextMethod = contextMethods.head

    def findFrom(cl:Class[_]) : Map[String, Set[String]] = {
      //debug(s"find from ${cl.getName}, target method:${contextMethod}")
      if(visitedMethod.contains(contextMethod))
        return Map.empty

      val visitor = new ClassVisitor(Opcodes.ASM4) {

        info(s"new ClassVisitor: context method = ${contextMethods.head} in\nclass ${cl.getName}")

        override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
          val fullDesc = s"${name}${desc}"
          //trace(s"visitMethod $fullDesc")
          if(fullDesc == contextMethods.head) {
            debug(s"[target] visit method ${name}, desc:${desc} in ${clName(cl.getName)}")
            new MethodVisitor(Opcodes.ASM4) {
              override def visitFieldInsn(opcode: Int, owner: String, name: String, desc: String) {
                if(opcode == Opcodes.GETFIELD || opcode == Opcodes.GETSTATIC) {
                  trace(s"visit field insn: $opcode name:$name, owner:$owner desc:$desc")
                  if(clName(owner) == cl.getName) {
                    debug(s"Found a accessed field: $name in ${target.getName}")
                    val newSet = accessedFields.getOrElseUpdate(target.getName, Set.empty[String]) += name
                    accessedFields += cl.getName -> newSet
                  }
                }
              }
              override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
                if(opcode == Opcodes.INVOKEVIRTUAL || opcode == Opcodes.INVOKEINTERFACE) {
                  trace(s"visit $opcode : ${name}$desc in\nclass ${clName(owner)}")
//                  if(clName(owner) == cl.getName) {
//                    info(s"Found a accessed parameter: $name")
//                    val newSet = accessedFields.getOrElseUpdate(target.getName, Set.empty[String]) += name
//                    accessedFields += cl.getName -> newSet
//                  }
//
                  //info(s"Find the target function: $name")
                  val ownerCls = Class.forName(clName(owner))
                  contextMethods = s"${name}${desc}" :: contextMethods
                  findFrom(ownerCls)
                  contextMethods = contextMethods.tail
                }
                else if (opcode == Opcodes.INVOKESPECIAL) {
                  info(s"visit invokespecial: $opcode ${name}, desc:$desc in\nclass ${clName(owner)}")
                }
              }
            }
          }
          else
            new MethodVisitor(Opcodes.ASM4) {} // empty visitor
        }
      }
      visitedMethod += contextMethod
      getClassReader(cl).accept(visitor, 0)

      accessedFields.toMap
    }
  }


}