package xerial

import java.io.File

import xerial.silk.core.ShellTask.ShellCommand
import xerial.silk.core._
import xerial.silk.core.SilkException._
import scala.language.experimental.macros

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

  def NA = {
    val t = new Throwable
    val caller = t.getStackTrace()(1)
    throw NotAvailable(s"${caller.getMethodName} (${caller.getFileName}:${caller.getLineNumber})")
  }

  implicit class Duration(n:Int) {
    def month : Duration = NA
    def seconds : Duration = NA
  }

  def from[A](in:Seq[A]) : InputFrame[A] = macro mNewFrame[A]
  def fromFile[A](in:File) : FileInput[A] = macro mFileInput[A]

}
