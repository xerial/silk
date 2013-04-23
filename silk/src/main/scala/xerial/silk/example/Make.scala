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

import xerial.silk._

/**
 * Make example
 * @author Taro L. Saito
 */
object Make {

  def inputFiles = c"""find src -name "*.scala" """.!!

  def wc(file:String) = c"wc -l $file | cut -f 1 -d ' '".head.map(_.trim.toInt)

  def md5sum(file:String) = c"md5sum $file".head.map{ line =>
    val c = line.split("""\w+""")
    (c(0), c(1)) // md5sum, file name
  }

  def wordCount = for(f <- inputFiles) yield wc(f)

  def md5sumAll = for(f <- inputFiles) yield md5sum(f)

}


object BWAAlignment {

  val BWA = "/bio/bin/bwa"

  def hg19 = c"curl -O http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/chromFa.tar.gz | tar xvz -O".toFile("hg19.fa")

  case class BWAIndex(name:File, files:Seq[File])

  def index = {
    val outputFiles = c"$BWA index -a ${hg19}" ==> Seq("bwt", "rbwt", "pac", "rpac", "sa", "rsa", "amb", "ann").map(suffix => s"$hg19.$suffix")
    hg19.map(BWAIndex(_, outputFiles))
  }




}

