package xerial.silk

import xerial.silk.framework.ops.FContext
import java.util.UUID
import java.nio.charset.Charset

/**
 * Defines a cluster environment to execute Silk operations
 * @author Taro L. Saito
 */
trait SilkEnv {

  def run[A](op:Silk[A]) : Seq[A]
  def run[A](op:Silk[A], target:String) : Seq[_]
  def eval[A](op:Silk[A]) : Unit

  private[silk] def runF0[R](locality:Seq[String], f: => R) : R


  private val observedFCCount = collection.mutable.Map[String, Int]()

  private lazy val utf8 = Charset.forName("UTF8")

  private[silk] def newID(fc:FContext) : UUID = {
    val refID = fc.refID
    val count = observedFCCount.getOrElseUpdate(refID, 0)
    val newCount = count + 1
    observedFCCount += refID -> newCount
    UUID.nameUUIDFromBytes(s"${refID}[$newCount]".getBytes(utf8))
  }


}

