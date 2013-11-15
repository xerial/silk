package xerial.silk

import java.util.UUID
import xerial.silk.core.FContext

/**
 * Defines a cluster environment to execute Silk operations
 * @author Taro L. Saito
 */
trait SilkEnv extends Serializable {

  def run[A](op:Silk[A]) : Seq[A]
  def run[A](op:Silk[A], target:String) : Seq[_]
  def eval[A](op:Silk[A]) : Unit

  private[silk] def runF0[R](locality:Seq[String], f: => R) : R


  @transient private val observedFCCount = collection.mutable.Map[String, Int]()

  private[silk] def newID(fc:FContext) : UUID = {
    val refID = fc.refID
    val count = observedFCCount.getOrElseUpdate(refID, 0)
    val newCount = count + 1
    observedFCCount += refID -> newCount
    UUID.nameUUIDFromBytes(s"${refID}[$newCount]".getBytes("UTF8"))
  }


}

