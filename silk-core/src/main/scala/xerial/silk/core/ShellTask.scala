package xerial.silk.core

import java.io.File

/**
 *
 */
object ShellTask {

  case class ShellCommand(context:FContext, sc:StringContext, args:Seq[Any]) extends Frame[Any] {
    def inputs = args.collect{case f:Frame[_] => f}
    def summary = templateString(sc)

    private def templateString(sc:StringContext) = {
      sc.parts.mkString("{}")
    }
  }

  case class ShellEnv(currentDir:File)

  def cd[U](path:String)(body: => U) = {
    body
  }


}
