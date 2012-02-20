package xerial.silk.io

/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayDeque
import collection.mutable.{ArrayBuilder, IndexedSeqLike}
import scala.actors.Actor._
import java.io._
import actors.Actor
import collection.IterableLike
import xerial.silk.util.Logging
import java.util.concurrent.{LinkedBlockingDeque, ConcurrentLinkedDeque}

//--------------------------------------
//
// BlockReader.scala
// Since: 2012/02/20 11:37
//
//--------------------------------------



class Block(val blockIndex: Long, source: DataSource, offset: Long, size: Int) {
  //def read: Array[Byte] = source.read(offset, size)
}


abstract class DataSource(blockSize: Int) {
  self =>

  val length: Long

  protected def blockStart(index: Long): Long = blockSize * index

  protected def blockEnd(index: Long): Long = scala.math.min(length, blockSize * (index + 1))

  protected def lastBlockIndex: Long = (length + blockSize - 1) / blockSize

  protected def readBlock(blockIndex: Long): Array[Byte] = {
    val s = blockStart(blockIndex)
    val e = blockEnd(blockIndex)
    read(s, (e - s).toInt)
  }

  protected def read(offset: Long, length: Int): Array[Byte]

  def stream: Stream[Array[Byte]] = {
    def block(index: Long): Array[Byte] = {
      //val start = blockStart(index)
      //val end = blockEnd(index)
      //new Block(index, self, start, (end - start).toInt)
      readBlock(index)
    }

    def nextStream(index: Long): Stream[Array[Byte]] = {
      if (index >= lastBlockIndex)
        Stream.empty
      else
        Stream.cons(block(index), nextStream(index + 1))
    }

    nextStream(0)
  }
}

class ByteArraySource(val data: Array[Byte], blockSize: Int) extends DataSource(blockSize) {
  val length = data.length.toLong

  def read(offset: Long, length: Int) = {
    data.slice(offset.toInt, length.toInt)
  }
}

class FileSource(file: File, blockSize: Int) extends DataSource(blockSize) {
  private val raf = new RandomAccessFile(file, "r")
  val length = raf.length()

  def read(offset: Long, length: Int) = {
    val rawData = new Array[Byte](length)
    val current = raf.getFilePointer
    if (current != offset)
      raf.seek(offset)
    raf.readFully(rawData)
    rawData
  }
}


object BlockReader {

//  def open(f: File)() = {
//    val in = new InputStreamWithPrefetch(new FileInputStream(f), 4 * 1024 * 1024)
//    try {
//
//    }
//    finally {
//      in.close
//    }
//  }

}


class InputStreamWithPrefetch(in: InputStream, blockSize: Int = 4 * 1024 * 1024, prefetchBlocks: Int = 3) extends Iterator[Array[Byte]] with Logging {

  private var noMoreData = false
  private val queue = new LinkedBlockingDeque[Array[Byte]](prefetchBlocks)

  private val prefetcher = new Actor {
    private def readFully(b: Array[Byte], off: Int, len: Int): Int = {
      var n: Int = 0
      do {
        var count: Int = in.read(b, off + n, len - n)
        if (count < 0) {
          return n
        }
        n += count
      } while (n < len)
      n
    }

    private def readFully(b: Array[Byte]): Int = {
      readFully(b, 0, b.length)
    }

    def readNextBlock = {
      val rawData = new Array[Byte](blockSize)
      val readLen = readFully(rawData)
      val block = if (readLen < blockSize) { rawData.slice(0, readLen)} else rawData
      queue.putLast(block)
      if(readLen < blockSize)
        noMoreData = true
    }

    def act() = {
      while(!noMoreData) {
        readNextBlock
      }
    }
  }

  prefetcher.start()

  def hasNext: Boolean = {
    if (!queue.isEmpty)
      true
    else {
      !noMoreData
    }
  }

  def next: Array[Byte] = {
    if (hasNext) {
      return queue.take()
    }
    throw new NoSuchElementException("next")
  }

}


class DataChunk(val data: Array[Byte])


//class WrappedArray(data:Array[Array[Byte]], offset:Int, offsetInLastBlock:Int) extends IndexedSeqLike[Byte, WrappedArray] {
//  
//  
//  def toSingleArray: Array[Byte] = {
//    val total = 
//  }
//}

abstract class DataStreamSource(blockSize: Int, blockDelimiterFinder: BlockDelimiterFinder) {

  private val cache: Option[ArrayBuilder[Byte]] = None

  def hasNextBlock: Boolean

  def nextBlock: Array[Byte]

  private def nextContinuousBlock: Option[Array[Byte]] = {


    if (!hasNextBlock) {
      return cache match {
        case Some(x) => new Some(x.result())
        case None => None
      }
    }

    val b = Array.newBuilder[Byte]
    val first = nextBlock
    b ++= first
    //
    //    next match {
    //      case Some(block) => blockDelimiterFinder.find(block) match {
    //        case Some(pos) => b +=
    //      }
    //    }
    //
    //
    //  }
    //
    //
    //  private def fill(numBlocksToRead: Int): Boolean {
    //    while (blockQueue.size () < numBlocksToRead) {
    //    nextBlock match {
    //    case Some (x) => blockQueue.add (x)
    //    case None => false
    //  }
    //  }
    //    true
    //  }
    //
    //
    None
  }
}


class TextBlock

abstract class BlockDelimiterFinder {
  def find(data: Array[Byte]): Option[Int] = find(data, 0, data.length)

  def find(data: Array[Byte], offset: Int, limit: Int): Option[Int]

  val delimiterLength: Int
}

trait NewLineFinder extends BlockDelimiterFinder {
  val delimiterLength = 2

  def find(data: Array[Byte], offset: Int, limit: Int): Option[Int] = {
    val len = limit
    var i = offset
    while (i < len) {
      if (data(i) == '\n')
        return Some(i)
      i += 1
    }
    None
  }
}


class TextBlockReader(source: DataSource, blockSize: Int) {
}




