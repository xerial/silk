package xerial

import java.io.File
import scala.language.experimental.macros
import scala.language.implicitConversions
import xerial.silk.framework.ops._
import xerial.silk.framework.ops.PreSilkCommand
import xerial.silk.core.file

/**
 * Helper methods for import
 *
 * {{{
 *   import xerial.silk._
 * }}}
 *
 * @author Taro L. Saitoc
 */
package object silk {

  def loadFile(file:String) : LoadFile = macro SilkMacros.loadImpl
//
//  implicit def convertToSilk[A](s:Seq[A]) : Silk[A] = RawInput(s)
//  implicit def convertToSilk[A](s:Array[A]) : Silk[A] = RawInput(s)
//
//
  implicit class SilkSeqWrap[A](a:Seq[A]) {
    def toSilk : SilkSeq[A] = null // TODO RawInput(a)
  }

  implicit class SilkArrayWrap[A](a:Array[A]) {
    def toSilk : SilkSeq[A] = null // TODO RawInput(a)
  }

  implicit class SilkWrap[A](a:A) {
    def toSilk : SilkSingle[A] = null // TODO RawInputSingle(a)
  }
//
  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) = PreSilkCommand(sc, args)
  }

}
