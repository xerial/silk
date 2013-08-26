//--------------------------------------
//
// Make.scala
// Since: 2013/04/16 11:07 AM
//
//--------------------------------------

package xerial.silk.example


import xerial.silk._
import xerial.core.util.DataUnit


/**
 * Make example
 * @author Taro L. Saito
 */
class MakeExample {

  def inputFiles = c"find . -maxdepth 1 -type f".lines

  def lc(file: String) = {
    val lcOut= c"wc -l $file | awk '{ print $$1; }'".lines.get.head
    println(s"lc result (file:$file):$lcOut")
    (file, lcOut.trim.toInt)
  }

  def md5sum(file: String) = c"md5sum $file".lines.head.map {
    line =>
      val c = line.split( """\w+""")
      (c(0), c(1)) // md5sum, file name
  }

  def lineCount = for (f <- inputFiles) yield lc(f)
  def md5sumAll = for (f <- inputFiles) yield md5sum(f)
}











































