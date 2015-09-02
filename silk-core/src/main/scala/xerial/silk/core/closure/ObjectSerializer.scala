package xerial.silk.core.closure

import java.io._

import xerial.core.log.Logger
import xerial.silk.core.Silk


/**
 * @author Taro L. Saito
 */
object SilkSerializer extends Logger {

  implicit class SerializeHelper(n:Any) {
    def serialize : Array[Byte] = serializeObj(n)
    def serializeTo(oos:ObjectOutputStream) = {
      oos.writeObject(n)
    }
  }

  implicit class DeserializeHelper(b:Array[Byte]) {
    def deserialize : AnyRef = deserializeObj[AnyRef](b)
    def deserializeAs[A] : A = deserializeObj[A](b)
  }


  def serializeObj(v:Any) = {
    val buf = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(buf)
    oos.writeObject(v)
    oos.close()
    val ba = buf.toByteArray
    ba
  }

  class ObjectDeserializer(in:InputStream, classLoader:ClassLoader=Thread.currentThread().getContextClassLoader) extends ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) = {
      Class.forName(desc.getName, false, classLoader)
    }
  }

  def deserializeObj[A](b:Array[Byte]): A = deserializeObj(new ByteArrayInputStream(b))

  def deserializeObj[A](in:InputStream): A = {
    val ois = new ObjectDeserializer(in)
    val op = ois.readObject().asInstanceOf[A]
    ois.close()
    op
  }

  def serializeFunc[A, B, C](f: (A, B) => C) = serializeObj(f)
  def deserializeFunc[A, B, C](b:Array[Byte]) : (A, B) => C = deserializeObj(b)
  def serializeOp[A](op: Silk[A]) = serializeObj(op)
  def deserializeOp(b: Array[Byte]): Silk[_] = deserializeObj(b)



}
