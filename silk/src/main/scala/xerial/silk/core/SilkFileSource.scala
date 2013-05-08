//--------------------------------------
//
// SilkFileSource.scala
// Since: 2012/12/10 11:48 AM
//
//--------------------------------------

package xerial.silk.core
import xerial.core.io.text.{UString, LineReader}
import java.io.{FileInputStream, ByteArrayInputStream}
import xerial.silk.flow.{SilkInMemory, Silk}

case class LineBlock(lines:Array[UString]) extends Silk[UString] with SilkStandardImpl[UString] {
  def iterator = lines.iterator
  def newBuilder[T] = null
  def eval = null
}

/**
 * @author Taro L. Saito
 */
class SilkFileSource(path:String) {

  def lines : Silk[UString] = {
    val reader = LineReader(new FileInputStream(path))
    SilkInMemory(reader.toSeq.asInstanceOf[Seq[UString]])
  }

  def lineBlocks : Silk[LineBlock] = {
    // TODO faster impl
    val reader = LineReader(new FileInputStream(path))
    val linesInBlock = 1000
    val seq = for(block <- reader.sliding(linesInBlock, linesInBlock).toSeq) yield {
      LineBlock(block.seq.asInstanceOf[Seq[UString]].toArray)
    }
    SilkInMemory(seq)
  }

}