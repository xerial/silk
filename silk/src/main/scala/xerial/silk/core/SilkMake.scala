//--------------------------------------
//
// SilkMake.scala
// Since: 2013/02/12 2:58 PM
//
//--------------------------------------

package xerial.silk.core
import xerial.macros.Macros._
import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
object SilkMake extends Logger {

  trait Task {

  }

  case object EmptyTask extends Task {

  }


  case class RootTask(name:String) extends Task{
    private var lst : Seq[Task] = Seq.empty[Task]
    def run {
      debug(s"running $name: ${lst.mkString(", ")}")
    }

    def ~(cmd:String) : RootTask = this.~(CmdTask(Some(this), cmd))
    def |(cmd:String) : RootTask = |(CmdTask(Some(this), cmd))

    def ~(t:Task) : RootTask = {
      lst = lst :+ t
      this
    }

    def |(t:Task) : RootTask = {
      lst = lst :+ t
      this
    }
  }

  case class CmdTask(private[SilkMake] var parent:Option[Task] = None, cmd:String) extends Task {
    
  }

  trait Runner



}

import SilkMake._

/**
 *
 */
trait SilkMake {
  def task : RootTask = {


    val m = enclosingMethodName; new RootTask(m)
  }




}
