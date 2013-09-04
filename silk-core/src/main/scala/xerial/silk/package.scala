package xerial

import scala.language.experimental.macros
import scala.language.implicitConversions
import xerial.silk.framework.ops._
import scala.reflect.ClassTag
import xerial.silk.framework.WorkflowMacros

/**
 * Helper methods for using Silk. Import this package as follows:
 *
 * {{{
 *   import xerial.silk._
 * }}}
 *
 * @author Taro L. Saito
 */
package object silk {


  implicit class SilkSeqWrap[A](val a:Seq[A]) {
    def toSilk(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.mRawSmallSeq[A]
  }

  implicit class SilkArrayWrap[A:ClassTag](a:Array[A]) {
    def toSilk : SilkSeq[A] = SilkException.NA
  }

  implicit class SilkWrap[A:ClassTag](a:A) {
    def toSilkSingle : SilkSingle[A] = SilkException.NA
  }

  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) : CommandOp = macro SilkMacros.mCommand
  }

  /**
   * Import another workflow trait as a mixin to this class. The imported workflow shares the same session
   * @param ev
   * @tparam A
   * @return
   */
  def mixin[A](implicit ev:ClassTag[A]) : A = macro WorkflowMacros.mixinImpl[A]



}
