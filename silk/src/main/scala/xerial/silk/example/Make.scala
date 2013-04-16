//--------------------------------------
//
// Make.scala
// Since: 2013/04/16 11:07 AM
//
//--------------------------------------

package xerial.silk.example

import java.io.File
import xerial.silk.core.{SilkSingle, Silk}
import java.nio.file.Files

/**
 * Make example
 * @author Taro L. Saito
 */
object Make {

  import xerial.silk._

  def inputFiles = """find src -name "*.scala""".!!

  def wc(file:String) = s"wc -l $file | cut -f 1 -d ' '".!!.head.map(_.trim.toInt)

  def md5sum(file:String) = s"md5sum $file".!!.head.map{ line =>
    val c = line.split("""\w+""")
    (c(0), c(1)) // md5sum, file name
  }

  def wordCount = for(f <- inputFiles) yield wc(f)

  def md5sumAll = for(f <- inputFiles) yield md5sum(f)

}