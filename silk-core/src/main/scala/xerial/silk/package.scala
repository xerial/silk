package xerial

import java.io.File

import xerial.silk.core.{SilkMacros, FileInput, InputFrame, RawSQL}


/**
 *
 */
package object silk {
  import SilkMacros._

  implicit class SqlContext(val sc:StringContext) extends AnyVal {
    def sql(args:Any*) : RawSQL = macro mSQL
  }

  implicit class ShellContext(val sc:StringContext) extends AnyVal {
    def c(args: Any*) : ShellCommand = macro mShellCommand
  }


  private[chroniker] val UNDEFINED : Nothing = throw new UnsupportedOperationException("undefined")

  implicit class Duration(n:Int) {
    def month : Duration = UNDEFINED
    def seconds : Duration = UNDEFINED
  }

  def from[A](in:Seq[A]) : InputFrame[A] = macro mNewFrame[A]
  def fromFile[A](in:File) : FileInput[A] = macro mFileInput[A]

}
