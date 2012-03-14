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


import java.io._
import xerial.silk.util.Logging


//--------------------------------------
//
// DataProducer.scala
// Since: 2012/03/14 11:01
//
//--------------------------------------

/**
 * Shared codes of producers
 */
trait DataProducerBase extends Closeable {
  self =>

  val pipeIn = new PipedInputStream
  val pipeOut = new PipedOutputStream(pipeIn)

  private val worker = new Thread(new Runnable {
    def run() {
      try self.start
      catch {
        case e: InterruptedException => // terminated by close
      }
    }
  })
  worker.setDaemon(true) // enable JVM terminate without stopping the worker
  worker.start()

  protected def start = {
    try {
      produce(pipeOut)
    }
    finally {
      pipeOut.flush
      pipeOut.close
    }
  }

  def produce(out: OutputStream)

  override def close: Unit = {
    worker.interrupt()
    pipeOut.close()
    pipeIn.close()
  }
}

/**
 * Data producer produces data using a thread, and provides input stream to fetch the data.
 * Implement the produce method to use this trait.
 *
 * @author leo
 */
trait DataProducer extends InputStream with DataProducerBase {

  override def read(): Int = pipeIn.read

  override def read(b: Array[Byte]): Int = pipeIn.read(b)

  override def read(b: Array[Byte], off: Int, len: Int) = pipeIn.read(b, off, len)

  override def skip(n: Long) = pipeIn.skip(n)

  override def available() = pipeIn.available()

  override def mark(readlimit: Int) {
    pipeIn.mark(readlimit)
  }

  override def reset() {
    pipeIn.reset
  }

  override def markSupported() = pipeIn.markSupported
}

/**
 * Producer of text data.
 */
trait TextDataProducer extends DataProducerBase {

  override def produce(out: OutputStream) = {
    val p = new PrintWriter(out)
    try
      produce(p)
    finally
      p.flush()
  }

  def produce(out: PrintWriter)

  class LineIterator extends Iterator[String] {
    val bufferedReader = new BufferedReader(new InputStreamReader(pipeIn))
    var nextLine: String = null

    def hasNext = {
      if (nextLine == null) {
        nextLine = bufferedReader.readLine()
      }
      nextLine != null
    }

    def next(): String = {
      if (hasNext) {
        val line = nextLine
        nextLine = null
        line
      }
      else
        Iterator.empty.next
    }
  }

  def lines: Iterator[String] = new LineIterator

}