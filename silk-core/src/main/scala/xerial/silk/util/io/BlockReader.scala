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


import java.io._
import xerial.silk.util.Logging
import xerial.silk.util.io.{RichInputStream}

//--------------------------------------
//
// BlockReader.scala
// Since: 2012/02/20 11:37
//
//--------------------------------------


case class Block(val blockIndex: Long, val offset: Long, val size: Int, val source: DataSource) {
  //def read: Array[Byte] = source.read(offset, size)
}

trait DataSource

abstract class DataSourceBase(blockSize: Int) extends DataSource with Closeable {
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

  def close: Unit

  def stream: Stream[Array[Byte]] = {
    def block(index: Long): Array[Byte] = {
      readBlock(index)
    }

    def nextStream(index: Long): Stream[Array[Byte]] = {
      if (index >= lastBlockIndex) {
        close
        Stream.empty
      }
      else
        Stream.cons(block(index), nextStream(index + 1))
    }

    nextStream(0)
  }
}

class ByteArraySource(val data: Array[Byte], blockSize: Int = 8 * 1024) extends DataSourceBase(blockSize) {
  val length = data.length.toLong

  def close = {}

  def read(offset: Long, length: Int) = {
    val start = offset.toInt
    val end = (offset + length).toInt
    data.slice(start, end)
  }
}

class FileSource(file: File, blockSize: Int = 8 * 1024) extends DataSourceBase(blockSize) {
  private val raf = new RandomAccessFile(file, "r")
  val length = raf.length()

  def close = raf.close

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

}



trait BlockDataStream[A] extends DataChannel[A] with RichInputStream {
  val blockSize: Int
  protected val in: InputStream

  override def close = {
    super.close
    in.close
  }
}

class InputStreamWithPrefetch(protected val in: InputStream, val blockSize: Int = 4 * 1024 * 1024, override protected val queueSize: Int = 5)
  extends BlockDataStream[Array[Byte]] with Logging {

  def this(data:Array[Byte]) = this(new ByteArrayInputStream(data))

  private def readInput = {
    def readBlock: Unit = {
      val rawData = new Array[Byte](blockSize)
      val readLen = readFully(rawData)
      if (readLen > 0) {
        val block =
          if (readLen < blockSize) rawData.slice(0, readLen) else rawData

        // Put the read entry to the queue. This method will block if the channel is full.
        write(block)
      }

      if (readLen == blockSize)
        readBlock // Continue reading
      else
        close // Finished reading. Now close the channel
    }
    readBlock
  }

  // Background thread to read data from the input stream
  private val reader = new Thread(new Runnable{
    def run() = readInput
  })

  if (queueSize <= 0)
    throw new IllegalArgumentException("prefetch size must be larger than 0")
  reader.start()
}



/**
 *
 * @param data
 */
case class DataSplit(data: Array[Byte])


//class SplitReader(protected val inputStream: InputStream, val blockSize: Int = 4 * 1024 * 1024, protected val prefetchBlocks: Int = 5)
//  extends BlockDataStream[DataSplit] with QueuedReader[DataSplit] {
//
//  private val reader = new InputStreamWithPrefetch(inputStream, blockSize, prefetchBlocks)
//
//
//  private val splitter = new Actor {
//    def readNextSplit = {
//
//      val readLen: Int = {
//        if (!reader.hasNext)
//          0
//        else {
//          val head: Array[Byte] = reader.peek
//
//
//          0
//        }
//      }
//      if (readLen < blockSize)
//        finishedReading = true
//    }
//
//    def act() = {
//      while (!finishedReading)
//        readNextSplit
//    }
//  }
//
//  splitter.start()
//}

trait DataOutput[T] {
  def emit(data: T): Unit
}

abstract class BlockDelimiter {
  def split(data: BlockDataStream[Array[Byte]], out: DataOutput[Array[Byte]]): Unit
}

class NewLine extends BlockDelimiter {
  val maxDelimiterLength = 2

  def split(data: BlockDataStream[Array[Byte]], out: DataOutput[Array[Byte]]): Unit = {
    // ('\r' | '\r'? '\n')
    def isNewLineChar(ch: Byte) = {
      ch == '\n' || ch == '\r'
    }
    def findNewLineChar(d: Array[Byte], pos: Int): Option[(Int, Byte)] = {
      var c = pos
      val ch = d(c)
      while (c < d.length && !isNewLineChar(ch)) {
        c += 1
      }
      if (c >= d.length)
        None
      else
        Some(c, ch)
    }
    def hasNewLineAtBoundary(d: Array[Byte]): Boolean = d(d.length - 1) == '\n'

    type T = Array[Byte]
    //    def find(offset:Int, builder:ArrayBuilder[T])  = {
    //      if(data.hasNext) {
    //        val next = data.next
    //        if(hasNewLineAtBoundary(next)) {
    //          // |b0    | b1   | b2
    //          // |  \r\n|      |
    //          // |    \n|      |
    //          out.emit(next)
    //        }
    //        else if(data.hasNext) {
    //          val next2 = data.next
    //          findNewLineChar(next, 0) match {
    //            case Some(pos, ch) =>
    //            case None =>
    //          }
    //          // |b0    | b1   | b2
    //          // |    \r|\n    |
    //          // |      | \r\n |
    //          // |      | \r   |
    //          // |      | \n   |
    //        }
    //        else {
    //          // |      |      | \r
    //        }
    //
    //
    //      }
    //    }

  }
}

//
//
//class TextBlockReader(source: BlockDataStream, blockSize: Int) {
//
//
//}
//
//
//
//
