//--------------------------------------
//
// ParallelParsing.scala
// Since: 2012/12/07 2:39 PM
//
//--------------------------------------

package xerial.silk.example

import xerial.core.io.text.UString
import xerial.compress.QuantizedFloatCompress
import java.io.File
import xerial.silk._
import xerial.silk.framework.ops.LoadFile

/**
 * Parallel parsing example
 * @author Taro L. Saito
 */
object ParallelParsing {



  sealed trait ParseResult {
    def isDataLine = false
  }
  case class Header(chr:String, start:Int, step:Int, pos:Long) extends ParseResult {
    def newHeader(offset:Long) = Header(chr, start, step, offset+pos)
  }
  case class DataLine(v:Float) extends ParseResult {
    override def isDataLine = true
  }
  case object BlankLine extends ParseResult

  case class MyDB(header:SilkSeq[Header], value:SilkSeq[Array[Byte]])

  def compress(arr:Array[Float]) = QuantizedFloatCompress.compress(arr)

  def parseLine(count:Int, line:UString) : (Int, ParseResult) = {
    if(line.charAt(0) == '>') {
      (0, Header("", 0, 1, count))
    }
    else {
      val s = line.toString.trim
      if(s.length == 0)
        (count, BlankLine)
      else
        (count+1, DataLine(s.toFloat))
    }
  }

  def main(args:Array[String]) {
    // read files
    val f = Silk.loadFile("sample.txt")

    //  Header or DataLine
    val lineBlocks = f.rawLines.split
    val parsedBlock =
      for(s <- lineBlocks) yield
        s.scanLeftWith(0) { case (count, line) => parseLine(count, line) }


    // Collect context headers
//    val parsed = parsedBlock.concat
//    val header = parsed collect { case h:Header => h }
//
//    // Fix relative offsets to global offsets
//    val correctedHeader = header.scanLeftWith(0L){ case (offset, h) =>
//      (offset + h.pos, h.newHeader(offset))
//    }
//    // Create header table
//    val headerTable = correctedHeader sortBy { h => (h.chr, h.start) }
//    val dataLineBlock = parsed.collect{ case DataLine(v) => v }.split
//
//    val binary = for(s <- dataLineBlock; a = s.toArray[Float]) yield compress(a)
//
//    // Create a DB
//    val savedRef = MyDB(headerTable, binary).toSilkSingle.save

  }

}