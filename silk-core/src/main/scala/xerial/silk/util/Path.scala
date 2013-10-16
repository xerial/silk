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
  implicit def wrap(f:java.io.File) = new Path(f)
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
}

