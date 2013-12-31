package xerial.silk

import xerial.silk.core.{MapOp, RawSeq, CallGraph}
import scala.collection.GenTraversable
import scala.util.Random
import xerial.core.util.Shell
import scala.sys.process.Process
import xerial.core.log.Logger
import scala.io.Source
import xerial.lens.ConstructorParameter
import xerial.silk._
import xerial.silk.SilkException._

/**
 * Weaver is an interface for evaluating Silk operations.
 * InMemoryWeaver, LocalWeaver, ClusterWeaver
 *
 * @author Taro L. Saito
 */
trait Weaver extends Serializable {

  /**
   * Custom configuration type that is specific to the Weaver implementation
   * For example, if one needs to use local weaver, only the LocalConfig type will be set to this type.
   */
  type Config

  /**
   * Configuration object
   */
  val config : Config


  /**
   * Get the result of Silk
   * @param op
   * @tparam A
   * @return
   */
  def get[A](op:SilkSeq[A]) : Seq[A] = weave(op).get

  /**
   * Get the result of Silk
   * @param op
   * @tparam A
   * @return
   */
  def get[A](op:SilkSingle[A]) : A = weave(op).get

  /**
   * Get the result of a specific target in Silk
   * @param silk
   * @param target
   * @tparam A
   * @return
   */
  def get[A](silk:Silk[A], target:String) : Any = {
    CallGraph.findTarget(silk, target).map {
      case s:SilkSeq[_] => weave(s).get
      case s:SilkSingle[_] => weave(s).get
    } getOrElse { SilkException.error(s"target $target is not found in $silk") }
  }

  /**
   * Start weaving (evaluating) the given Silk.
   * @param op
   * @tparam A
   * @return future reference to the result
   */
  def weave[A](op:SilkSeq[A]) : SilkFuture[Seq[A]] = NA

  /**
   * Start weaving (evaluating) the given Silk.
   * @param op
   * @tparam A
   * @return future reference to the result
   */
  def weave[A](op:SilkSingle[A]) : SilkFuture[A] = NA
  private[silk] def runF0[R](locality:Seq[String], f: => R) : R = NA

}




object Weaver {

  import core._

  def inMemoryWeaver : Weaver = new InMemoryWeaver()

}