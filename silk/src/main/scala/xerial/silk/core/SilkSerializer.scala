//--------------------------------------
//
// SilkSerializer.scala
// Since: 2012/12/05 3:44 PM
//
//--------------------------------------

package xerial.silk.core

import java.io._
import org.objectweb.asm.{Opcodes, ClassVisitor, MethodVisitor, ClassReader}
import collection.mutable.{Set, Map}
import xerial.core.log.Logger


/**
 * @author Taro L. Saito
 */
object SilkSerializer extends Logger {

  def checkClosure(f:AnyRef) {
    val cl = f.getClass
    debug("check closure: %s", cl)

    val finder = new FieldAccessFinder
    getClassReader(cl).accept(finder, 0)

    debug("accessed fields: %s", finder.output)

  }

  /**
   * Find the accessed parameters of the target class in the closure.
   * This function is used for optimizing data retrieval in Silk.
   * @param target
   * @param closure
   * @return
   */
  def accessedFields(target:Class[_], closure:AnyRef) : Seq[String] = {
    val finder = new ObjectParamAccessFinder(target)
    getClassReader(closure.getClass).accept(finder, 0)
    finder.getAccessedParams
  }


  def serializeClosure(f:AnyRef) = {
    checkClosure(f)
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(f)
    o.flush()
    o.close
    b.close
    b.toByteArray
  }

  def deserializeClosure(b:Array[Byte]) : AnyRef = {
    val in = new ObjectInputStream(new ByteArrayInputStream(b))
    val ret = in.readObject()
    in.close()
    ret
  }


  def serialize(silk:AnyRef) : Array[Byte] = {
    val cl = silk.getClass
    debug("serializing %s", cl)
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(silk)
    o.flush()
    o.close
    b.close
    b.toByteArray
  }

  def deserializeAny(b:Array[Byte]) : AnyRef = {


    val in = new ObjectDeserializer(new ByteArrayInputStream(b))
    val ret = in.readObject()
    in.close()
    ret
  }

  class ObjectDeserializer(in:InputStream) extends ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) = {
      Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
    }
  }

  def deserialize(b:Array[Byte]) : Silk[_] = {
    debug("deserialize")
    val in = new ObjectDeserializer(new ByteArrayInputStream(b))
    val ret = in.readObject().asInstanceOf[Silk[_]]
    in.close()
    ret
  }

  private def getClassReader(cl: Class[_]): ClassReader = {
    new ClassReader(cl.getResourceAsStream(
      cl.getName.replaceFirst("^.*\\.", "") + ".class"))
  }

  private class ObjectParamAccessFinder(target:Class[_]) extends ClassVisitor(Opcodes.ASM4) {
    val accessed = Seq.newBuilder[String]
    def getAccessedParams = accessed.result
    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
      info("visit method: %s desc:%s", name, desc)
      new MethodVisitor(Opcodes.ASM4) {

        def clName(s:String) = s.replace("/", ".")

        override def visitFieldInsn(opcode: Int, owner: String, name: String, desc: String) {
          info("visit field insn: %d owner:%s name:%s desc:%s", opcode, owner, name, desc)
        }
        override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
          info("visit method insn: %d owner:%s name:%s desc:%s", opcode, owner, name, desc)
          if(opcode == Opcodes.INVOKEVIRTUAL && clName(owner) == target.getName) {
            accessed += name
          }
        }
      }
    }
  }




  private class FieldAccessFinder() extends ClassVisitor(Opcodes.ASM4) {
    val output = collection.mutable.Map[Class[_], Set[String]]()
    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
      info("visit method: %s desc:%s", name, desc)
      new MethodVisitor(Opcodes.ASM4) {

        def clName(s:String) = s.replace("/", ".")

        override def visitFieldInsn(opcode: Int, owner: String, name: String, desc: String) {
          info("visit field insn: %d owner:%s name:%s desc:%s", opcode, owner, name, desc)
          if(opcode == Opcodes.GETFIELD) {
            for(cl <- output.keys if cl.getName == clName(owner))
              output(cl) += name
          }
        }
        override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
          info("visit method insn: %d owner:%s name:%s desc:%s", opcode, owner, name, desc)
          if (opcode == Opcodes.INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
            for (cl <- output.keys if cl.getName == clName(owner))
              output(cl) += name
            }
          }
      }
    }
  }

}