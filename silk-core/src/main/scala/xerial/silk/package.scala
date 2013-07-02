package xerial

import scala.language.experimental.macros
import scala.language.implicitConversions
import xerial.silk.framework.ops._
import xerial.silk.framework.ops.PreSilkCommand
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

  def loadFile(file:String) : LoadFile = macro SilkMacros.loadImpl

  implicit class SilkSeqWrap[A:ClassTag](a:Seq[A]) {
    def toSilk : SilkSeq[A] = Silk.newEnv.newSilk(a)
  }

  implicit class SilkArrayWrap[A:ClassTag](a:Array[A]) {
    def toSilk : SilkSeq[A] = Silk.newEnv.newSilk(a)
  }

  implicit class SilkWrap[A:ClassTag](a:A) {
    def toSilkSingle : SilkSingle[A] = SilkException.NA
  }

  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) = PreSilkCommand(sc, args)
  }

  /**
   * Import another workflow trait as a mixin to this class. The imported workflow shares the same session
   * @param ev
   * @tparam A
   * @return
   */
  def mixin[A](implicit ev:ClassTag[A]) : A = macro WorkflowMacros.mixinImpl[A]



}
