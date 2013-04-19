//--------------------------------------
//
// CmdString.scala
// Since: 2013/04/16 19:08
//
//--------------------------------------

package xerial.silk.core

import xerial.core.log.Logger

/**
 * String representation of UNIX Commands
 * @author Taro L. Saito
 */
case class CmdString(sc:StringContext, args:Any*) extends Logger {
  override def toString = {
    trace(s"parts length: ${sc.parts.length}, argc: ${args.length}")
    val b = new StringBuilder
    val zip = sc.parts.zipAll(args, "", null)
    for((f, v) <- zip) {
      b.append(f)
      if(v != null)
        b.append(v)
    }
    trace(s"zipped ${zip.mkString(", ")}")
    b.result()
  }
  def argSize = args.size
  def arg(i:Int) : Any = args(i)

  def templateString = {
    val b = new StringBuilder
    val zip = sc.parts.zipAll(args, "", null)
    for((f, v) <- zip) {
      b.append(f)
      if(v != null)
        b.append("${}")
    }
    trace(s"zipped ${zip.mkString(", ")}")
    b.result()
  }
}