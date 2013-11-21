//--------------------------------------
//
// Storage.scala
// Since: 2013/11/05 22:46
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk._
import java.util.UUID
import java.io._
import xerial.core.io.IOUtil

/**
 * Storage is an abstraction of the shared storage
 * that is readable and writable from all nodes.
 *
 * @author Taro L. Saito
 */
trait Storage {

  def isExist[A](silk:Silk[A]) : Boolean
  def read[A](silk:Silk[A]) : Seq[A]
  def write[A](silk:Silk[A]) : Unit
}

trait SilkEnvComponent {
  implicit val env : SilkEnv
}

trait MemoryStorage extends Storage with SilkEnvComponent {
  private val m = collection.mutable.Map[UUID, Seq[_]]()

  def isExist[A](silk:Silk[A]) = m.contains(silk.id)

  def write[A](silk:Silk[A]) {
    val seq = silk match {
      case s:SilkSingle[_] => Seq(env.run(s))
      case s:SilkSeq[_] => env.run(s)
    }
    m += silk.id -> seq
  }

  def read[A](silk:Silk[A]) = {
    m.get(silk.id) match {
      case Some(x) => x.asInstanceOf[Seq[A]]
      case None => SilkException.error(s"not found: $silk")
    }
  }

}


abstract class SharedStorage(storageDir: => File)
 extends Storage with SilkEnvComponent with SerializationService {

  import xerial.silk.util.Path._

  private def pathOf[A](silk:Silk[A]) = storageDir / silk.idPrefix

  def isExist[A](silk:Silk[A]) = pathOf(silk).exists()

  def write[A](silk:Silk[A]) = {
    val p = pathOf(silk)
    val (size:Long, in:Seq[A]) = silk match {
      case s:SilkSeq[_] => (env.run(s.size), env.run(s).asInstanceOf[Seq[A]])
      case s:SilkSingle[_] => (1, env.run(s).asInstanceOf[Seq[A]])
    }
    IOUtil.withResource(new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(p)))) {
      fout =>
        fout.writeLong(size)
        for(e <- in) {
          e.serializeTo(fout)
        }
    }
  }

  def read[A](silk:Silk[A]) = {
    val p = pathOf(silk)
    IOUtil.withResource(new ObjectInputStream(new BufferedInputStream(new FileInputStream(p)))) {
      fin =>
        val size = fin.readLong()
        val b = Seq.newBuilder[A]
        var i = 0
        while(i < size) {
          val e = fin.readObject().asInstanceOf[A]
          b += e
          i += 1
        }
        b.result
    }
  }

}


