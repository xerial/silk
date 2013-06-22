package xerial

import scala.language.experimental.macros
import scala.language.implicitConversions
import xerial.silk.framework.ops._
import xerial.silk.framework.ops.PreSilkCommand

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

  implicit class SilkSeqWrap[A](a:Seq[A]) {
    def toSilk : SilkSeq[A] = null // TODO RawInput(a)
  }

  implicit class SilkArrayWrap[A](a:Array[A]) {
    def toSilk : SilkSeq[A] = null // TODO RawInput(a)
  }

  implicit class SilkWrap[A](a:A) {
    def toSilkSingle : SilkSingle[A] = null // TODO RawInputSingle(a)
  }

  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) = PreSilkCommand(sc, args)
  }

}
