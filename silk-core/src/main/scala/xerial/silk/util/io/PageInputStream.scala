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
  val DefaultPageSize: Int = 8192

}

import PageInputStream._
import java.io.{File, FileInputStream, FileReader, Reader, InputStream}
import xerial.silk.util.Logging
import collection.IterableLike
import collection.GenTraversableOnce

/**
 * Base implementation of page iterator
 * @param pageSize
 * @tparam T
 */
private[io] abstract class PageIterator[T : ClassManifest](pageSize: Int) extends Iterator[Array[T]] with Logging {
  private var reachedEOF = false
  private var current: Array[T] = null

  def newArray(size: Int): Array[T] = new Array[T](size)

  def read(b: Array[T], off: Int, len: Int): Int

  def readFully(b: Array[T], off: Int, len: Int): Int = {
    def loop(count: Int): Int = {
      if (count >= len)
        count
      else {
        val readLen = read(b, off + count, len - count)
        if (readLen == -1) {
          reachedEOF = true
          count
        }
        else
          loop(count + readLen)
      }
    }

    loop(0)
  }

  def readFully(b: Array[T]): Int = {
    readFully(b, 0, b.length)
  }

  private def readNextPage: Array[T] = {
    val page = newArray(pageSize)
    val readLen = readFully(page)
    if (readLen <= 0)
      null
    else if(readLen < pageSize)
      page.slice(0, readLen)
    else
      page
  }

  def hasNext = {
    if (current != null)
      true
    else if (reachedEOF)
      false
    else {
      current = readNextPage
      current != null
    }
  }

  def next: Array[T] = {
    if (hasNext) {
      val e = current
      current = null
      e
    }
    else
      Iterator.empty.next
  }

}

/**
 * Page-wise input stream reader
 *
 * @author leo
 */
class PageInputStream(in: InputStream, pageSize: Int) extends Iterable[Array[Byte]] {
  def this(in: InputStream) = this(in, pageSize = DefaultPageSize)

  def this(file: File, pageSize: Int = DefaultPageSize) = this(new FileInputStream(file))

  def iterator : Iterator[Array[Byte]]  = new PageIterator[Byte](pageSize) {
    def read(b: Array[Byte], off: Int, len: Int) = in.read(b, off, len)
  }
}

/**
 * Page-wise text reader
 * @param in
 * @param pageSize
 */
class PageReader(in: Reader, pageSize: Int) extends Iterable[Array[Char]] {
  def this(in: Reader) = this(in, pageSize = DefaultPageSize)

  def this(file: File, pageSize: Int = DefaultPageSize) = this(new FileReader(file))

  def iterator : Iterator[Array[Char]] = new PageIterator[Char](pageSize) {
    def read(b: Array[Char], off: Int, len: Int) = in.read(b, off, len)
  }


}