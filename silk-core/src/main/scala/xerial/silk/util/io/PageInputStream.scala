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

package xerial.silk.util.io


//--------------------------------------
//
// PageInputStream.scala
// Since: 2012/03/14 10:33
//
//--------------------------------------

object PageInputStream {
  val DefaultPageSize : Int = 8192
}

import PageInputStream._
import java.io.{Reader, File, FileInputStream, InputStream}

/**
 * Page-wise input stream reader
 *
 * @author leo
 */
class PageInputStream private (in:InputStream, pageSize:Int) extends Iterable[Array[Byte]] {
  def this(in:InputStream) = this(in, DefaultPageSize)
  def this(reader:Reader, pageSize:Int=DefaultPageSize) = this(new ReaderInputStream(reader), pageSize)
  def this(file:File) = this(new FileInputStream(file))

  class PageIterator extends Iterator[Array[Byte]] {
    var reachedEOF = false
    var current: Option[Array[Byte]] = None

    private def readFully(b: Array[Byte], off: Int, len: Int): Int = {
      def read(count: Int): Int = {
        if (count >= len)
          count
        else {
          val readLen = in.read(b, off + count, len - count)
          if (readLen == -1)
            count
          else
            read(count + readLen)
        }
      }

      read(0)
    }

    private def readFully(b: Array[Byte]): Int = {
      readFully(b, 0, b.length)
    }

    private def readNextPage : Option[Array[Byte]] = {
      val page = new Array[Byte](pageSize)
      val readLen = readFully(page)
      if(readLen < pageSize)
        reachedEOF = true

      if(readLen <= 0) None else Some(page)
    }

    def hasNext = {
      current match {
        case Some(e) => true
        case None =>
          if(reachedEOF)
            false
          else {
            current = readNextPage
            current.isDefined
          }
      }
    }

    def next() = {
      if(hasNext) {
        val e = current.get
        current = None
        e
      }
      else
        throw new NoSuchElementException("next")
    }
  }

  def iterator = new PageIterator

}