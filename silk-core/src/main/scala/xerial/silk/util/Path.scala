//--------------------------------------
//
// Path.scala
// Since: 2013/10/16 10:42
//
//--------------------------------------

package xerial.silk.util


import java.io.{File => JFile}
import scala.language.implicitConversions

/**
 * Utilities for managing files and paths
 * @author Taro L. Saito
 */
object Path {
  implicit def wrap(s:String) = new Path(new JFile(s))
  implicit def wrap(f:JFile) = new Path(f)
}

class Path(val f:JFile) extends AnyVal {
  def / (s:String) : JFile = new JFile(f, s)

  def ls : Seq[Path] = {
    if(f.isDirectory)
      f.listFiles.map{ c => new Path(c) }
    else
      Seq.empty
  }

  def rmdirs {
    if(f.isDirectory) {
      ls foreach { _.rmdirs }
    }
    f.delete()
  }

  def relativeTo(base:JFile) : JFile = {
    new JFile(f.getPath.replaceFirst(s"${base.getPath}\\/", ""))
  }

  def removeExt(dotExt:String) : String = {
    val path = f.getPath
    val pos = path.lastIndexOf(s"$dotExt")
    if(pos == -1)
      path
    else
      path.substring(0, pos)
  }

}

