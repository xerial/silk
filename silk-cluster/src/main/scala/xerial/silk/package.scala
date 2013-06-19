package xerial

import xerial.silk.core.{SilkFlow, ShellCommand, Silk}
import silk.core.SilkFlow._
import java.io.File
import scala.language.experimental.macros
import scala.language.implicitConversions
import xerial.silk.framework.ops.SilkOps

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

  implicit def convertToSilk[A](s:Seq[A]) : Silk[A] = RawInput(s)
  implicit def convertToSilk[A](s:Array[A]) : Silk[A] = RawInput(s)


  implicit class SilkSeqWrap[A](a:Seq[A]) {
    def toSilk : Silk[A] = RawInput(a)
  }

  implicit class SilkArrayWrap[A](a:Array[A]) {
    def toSilk : Silk[A] = RawInput(a)
  }

  implicit class SilkWrap[A](a:A) {
    def toSilk : Silk[A] = RawInputSingle(a)
  }

  implicit class CmdBuilder(val sc:StringContext) {
//    def c(args:Any*) : ShellCommand = { val cmd = new SilkOps.CmdBuilder(sc).cimpl(args)
//      cmd
//    }
    def c(args:Any*) : ShellCommand = macro SilkFlow.mArgExpr
  }

  //implicit def asCmd(a:Any) : ShellCommand = a.asInstanceOf[ShellCommand]

  implicit class CmdAdapter(val any:Any) extends AnyVal {
    def asCmd : ShellCommand = {
      any.asInstanceOf[ShellCommand]
    }
    def file = asCmd.file
    def lines = asCmd.lines
  }

}
