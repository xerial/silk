package xerial

import silk.core.{ShellCommand, Silk}
import silk.core.SilkFlow._
import java.io.File
import scala.language.experimental.macros

/**
 * Helper methods for import
 *
 * {{{
 *   import xerial.silk._
 * }}}
 *
 * @author Taro L. Saito
 */
package object silk {

  def fromFile(file:String) = FileInput(new File(file))


  implicit class SilkSeqWrap[A](a:Seq[A]) {
    def toSilk : Silk[A] = RawInput(a)
  }

  implicit class SilkArrayWrap[A](a:Array[A]) {
    def toSilk : Silk[A] = RawInput(a)
  }

  implicit class SilkWrap[A](a:A) {
    def toSilk : Silk[A] = RawInputSingle(a)
  }

  implicit class CmdBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) : Silk[ShellCommand] = macro mArgExpr
  }


}
