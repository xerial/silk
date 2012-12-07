//--------------------------------------
//
// ParallelParsing.scala
// Since: 2012/12/07 2:39 PM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.collection.Silk

/**
 * @author Taro L. Saito
 */
object ParallelParsing {

  trait ParseResult
  case class Header(chr:String, start:Int, step:Int, pos:Int) extends ParseResult {
    def newHeader(offset:Int) = Header(chr, start, step, offset+pos)
  }
  case class DataLine(v:Float) extends ParseResult


  def main(args:Array[String]) {

    def parseLine(count:Int, line:String) = {
      if(line.startsWith("...")) {
        Header("", 0, 1, count)
      }
      else {
        DataLine(line.trim.toFloat)
      }
    }

    // read files
    val f = Silk.fromFile("...")

    //  Header or DataLine
    val parsed = f.lineBlocks.map { case (pos, lines) =>
      var count = 0
      for(line <- lines) yield {
        count += 1
        parseLine(count, line)
      }
    }

    // Collect context headers
    val header = parsed collect { case h:Header => h }
    // Fix offset frm the top
    val correctedHeader = header.scan(List.empty[Header]){ case (lst, h) =>
      val offset = lst.headOption map { _.pos } getOrElse(0)
      h.newHeader(offset) :: lst
    } reverse
    // Create header table
    val headerTable = correctedHeader sortBy { h => (h.chr, h.start) }
    val binary = parsed collect { case DataLine(v) => v } toArray map { a => compress(a) }

    // Create DB
    Silk.create(Map("index" -> headerTable, "value" -> binary))
  }

}