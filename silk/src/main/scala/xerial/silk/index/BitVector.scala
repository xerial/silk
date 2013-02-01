//--------------------------------------
//
// ACGTSeq.scala
// Since: 2012/06/19 3:42 PM
//
//--------------------------------------

package xerial.silk.index

import java.util.Arrays

/**
 * Helper methods for packing bit sequences into Array[Long]
 * @author leo
 */
trait BitEncoder {

  // 2G (max of Java array size) * 8 (long byte size) * 8 (bit) =  128G  (128G characters)
  protected val MAX_SIZE: Long = 2L * 1024L * 1024L * 1024L * 8L * 8L

  protected def minArraySize(numBits: Long): Int = {
    val blockBitSize: Long = 64L
    val arraySize: Long = (numBits + blockBitSize - 1L) / blockBitSize
    if (arraySize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(f"Cannot create BitVector of more than $MAX_SIZE%,d characters: $numBits%,d")
    }
    arraySize.toInt
  }

  @inline protected def blockIndex(pos: Long): Int = (pos >>> 6).toInt
  @inline protected def blockOffset(pos: Long): Int = (pos & 0x3FL).toInt


  protected val table = Array(false, true)
}

/**
 * Utilities to build BitVector
 */
object BitVector {

  def newBuilder = new BitVectorBuilder()

  def newBuilder(sizeHint: Long) = new BitVectorBuilder()

  def apply(bitString: String): BitVector = {
    val b = newBuilder
    b.sizeHint(bitString.length)
    for (ch <- bitString) {
      b += (if(ch == '0') true else false)
    }
    b.result
  }

}


/**
 * 2G (max of Java array size) * 8 (long byte size) * 8 (bit) =  128G  (128G characters)
 *
 * To generate an instance of BitVector, use BitVector.newBuilder or BitVector.apply(bitString)
 *
 * @param seq raw bit string
 * @param numBits
 */
class BitVector(private val seq: Array[Long], private val numBits: Long) extends BitEncoder {

  def size = numBits
  private var hash: Int = 0

  /**
   * Return the DNA base at the specified index
   * @param index
   * @return
   */
  def apply(index: Long): Boolean = {
    val pos = blockIndex(index)
    val offset = blockOffset(index)
    val shift = offset << 1
    val code = (seq(pos) >>> shift).toInt & 0x01
    table(code)
  }

  private def numFilledBlocks = (size / 64L).toInt

  private def lastBlock = seq.last & (~0L << blockOffset(size))

  override def hashCode() = {
    if (hash == 0) {
      var h = numBits * 31L
      var pos = 0
      for (i <- (0 until numFilledBlocks)) {
        h += seq(pos) * 31L
        pos += 1
      }
      if (blockOffset(size) > 0) {
        h += lastBlock * 31L
      }
      hash = h.toInt
    }
    hash
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: BitVector => {
        if (this.size != other.size)
          false
        else {
          (0 until numFilledBlocks).find(i => this.seq(i) != other.seq(i)) match {
            case Some(x) => false
            case None => this.lastBlock == other.lastBlock
          }
        }
      }
      case _ => false
    }
  }

  protected def fastCount(v: Long, checkTrue: Boolean): Long = {
    // JVM optimizer is smart enough to replace this code to a pop count operation available in the CPU
    val c = java.lang.Long.bitCount(v)
    if(checkTrue) c else 64L - c
  }

  /**
   * Count the number of bits within the specified range [start, end)
   * @param checkTrue count true or false
   * @param start
   * @param end
   * @return the number of occurrences
   */
  def count(checkTrue: Boolean, start: Long, end: Long): Long = {

    val sPos = blockIndex(start)
    val sOffset = blockOffset(start)

    val ePos = blockIndex(end - 1L)
    val eOffset = blockOffset(end - 1L)

    var count = 0L
    var num0sInMaskedRegion = 0L
    var pos = sPos
    while (pos <= ePos) {
      var mask: Long = ~0L
      if (pos == sPos) {
        mask <<= (sOffset << 1L)
        num0sInMaskedRegion += sOffset
      }
      if (pos == ePos) {
        val rMask = ~0L >>> (62L - (eOffset << 1))
        mask &= rMask
        num0sInMaskedRegion += 31L - eOffset
      }
      // Applying bit mask changes all bases in the masked region to As (code=00)
      val v: Long = seq(pos) & mask
      val popCount = fastCount(v, checkTrue)
      count += popCount
      pos += 1
    }

    if(checkTrue)
      count - num0sInMaskedRegion
    else
      count
  }

  /**
   * Extract a slice of the sequence [start, end)
   * @param start
   * @param end
   * @return
   */
  def slice(start: Long, end: Long): BitVector = {
    if (start > end)
      sys.error("illegal argument start:%,d > end:%,d".format(start, end))

    val sliceLen = end - start
    val newSeq = new Array[Long](minArraySize(sliceLen))

    var i = 0L
    while (i < sliceLen) {
      val sPos = blockIndex(start + i)
      val sOffset = blockOffset(start + i)

      val dPos = blockIndex(i)
      val dOffset = blockOffset(i)

      var copyLen = 0L
      var l = 0L
      val v = seq(sPos) & (~0L << sOffset)
      if (sOffset == dOffset) {
        // no shift
        copyLen = 64L
        l = v
      }
      else if (sOffset < dOffset) {
        // left shift
        val shiftLen = dOffset - sOffset
        copyLen = 64L - dOffset
        l = v << shiftLen
      }
      else {
        // right shift
        val shiftLen = sOffset - dOffset
        copyLen = 64L - sOffset
        l = v >>> shiftLen
      }
      newSeq(dPos) |= l
      i += copyLen
    }

    new BitVector(newSeq, sliceLen)
  }


}


/**
 * BitVector builder
 * @param capacity the hint of number of bits to store
 */
class BitVectorBuilder(private var capacity: Long) extends BitEncoder
{

  private var seq = new Array[Long](minArraySize(capacity))
  private var _numBits: Long = 0L

  /**
   * Create an empty sequence
   */
  def this() = this(32L)

  def numBits = _numBits

  def sizeHint(numBasesToStore: Long) {
    val arraySize = minArraySize(numBasesToStore)
    val newSeq = Arrays.copyOf(seq, arraySize)
    seq = newSeq
    capacity = arraySize * 32L
  }

  /**
   * Append a bit value
   * @param v
   */
  def +=(v: Boolean): Unit = {
    val index = _numBits
    _numBits += 1
    if (index >= capacity) {
      val newCapacity = (index * 3L / 2L) + 64L // Create a 1.5 times larger array
      sizeHint(newCapacity)
    }

    val pos = blockIndex(index)
    val offset = blockOffset(index)
    val shift = offset * 2
    seq(pos) |= (if(v) 1L else 0L) << shift
  }


  lazy val rawArray = {
    val size = minArraySize(_numBits)
    if (seq.length == size) seq else Arrays.copyOf(seq, size)
  }

  def result : BitVector = new BitVector(rawArray, numBits)

  override def toString = result.toString

}
