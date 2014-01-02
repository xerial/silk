//--------------------------------------
//
// SilkSingle.scala
// Since: 2013/11/06 17:47
//
//--------------------------------------

package xerial.silk

import scala.language.experimental.macros
import xerial.silk.core.SilkMacros


/**
 * Silk data class for a single element
 * @tparam A element type
 */
abstract class SilkSingle[+A] extends Silk[A] {

  import SilkMacros._

  def isSingle = true
  def size : Int = 1

  /**
   * Get the materialized result
   */
  def get(implicit weaver:Weaver) : A = {
    weaver.get(this)
  }

  def get(target:String)(implicit weaver:Weaver) : Any = {
    weaver.get(this, target)
  }

  def eval(implicit weaver:Weaver): this.type = {
    weaver.weave(this)
    this
  }


  def map[B](f: A => B): SilkSingle[B] = macro mapSingleImpl[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro flatMapSingleImpl[A, B]
  def filter(cond: A => Boolean): SilkSingle[A] = macro mFilterSingle[A]
  def withFilter(cond: A => Boolean): SilkSingle[A] = macro mFilterSingle[A]


  // For differential computing
  def subscribe[A] : SilkSingle[A] = macro mSubscribeSingle[A]

}







