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
import java.nio.CharBuffer
import xerial.core.log.Logging

//--------------------------------------
//
// DataProducer.scala
// Since: 2012/03/14 11:01
//
//--------------------------------------

/**
 * Implementer must invoke startWorker.
 * @tparam PipeIn
 * @tparam PipeOut
 */
trait DataProducerBase[PipeIn <: Closeable, PipeOut <: Closeable] extends Closeable with Logging {

  protected val pipeIn: PipeIn
  protected val pipeOut: PipeOut

  protected val worker = new Thread(new Runnable {
    def run() {
      try {
        produceStart
      }
      catch {
        case e: InterruptedException => {
          // terminated by close
          warn("Interrupted while producing data")
        }
      }
    }
  })

  /**
   * Start data production
   */
  protected def produceStart: Unit

  lazy val started: Boolean = {
    worker.setDaemon(true)
    worker.start()
    try {
      // Wait until the first data is produced. That also means initialization of classes extending this trait is finished.
      // The current thread is awaken by (PipedWriter.write -> PipedReader.recieve ->  notifyAll)
      Thread.sleep(0)
    }
    catch {
      case e: InterruptedException =>
        trace("sleep interrupted")
    }
    true
  }

  /**
   * Use this method to wrap any read method
   * @param f
   * @tparam A
   * @return
   */
  protected def wrap[A](f: => A): A = {
    if(started != true)
      sys.error("Failed to start the producer")
    f
  }

  override def close {
    // No need exists to close the reader explicitly since PipeReader.close simply reset the buffer
    pipeIn.close
    pipeOut.close
    if (started && worker.isAlive) {
      worker.interrupt
    }
  }
}

/**
 * Data producer produces data using a thread, and provides input stream to fetch the data.
 * To use this trait, implement the [[xerial.silk.util.io.DataProducer.produce]] method, in which
 * binary data produced
 *
 * @author leo
 */
trait DataProducer extends InputStream with DataProducerBase[InputStream, OutputStream] {

  protected val pipeIn = new PipedInputStream(8192)
  // 8K
  protected val pipeOut = new PipedOutputStream(pipeIn)

  protected def produceStart: Unit = {
    try
      produce(pipeOut)
    finally {
      pipeOut.flush
      pipeOut.close
    }
  }

  def produce(out: OutputStream): Unit

  def toReader = new InputStreamReader(this)

  override def read(): Int = wrap(pipeIn.read)

  override def read(b: Array[Byte]): Int = wrap(pipeIn.read(b))

  override def read(b: Array[Byte], off: Int, len: Int) = wrap(pipeIn.read(b, off, len))

  override def skip(n: Long) = wrap(pipeIn.skip(n))

  override def available() = wrap(pipeIn.available())

  override def mark(readlimit: Int) = wrap(pipeIn.mark(readlimit))

  override def reset() = wrap(pipeIn.reset)

  override def markSupported() = wrap(pipeIn.markSupported)
}

/**
 * Producer of text data.
 */
trait TextDataProducer extends Reader with DataProducerBase[Reader, Writer] {

  protected val pipeIn = new PipedReader
  protected val pipeOut = new PrintWriter(new PipedWriter(pipeIn))

  protected def produceStart: Unit = {
    try {
      produce(pipeOut)
    }
    finally {
      pipeOut.flush
      pipeOut.close
    }
  }

  def produce(out: PrintWriter): Unit

  class LineIterator extends Iterator[String] {
    val bufferedReader = new BufferedReader(pipeIn)
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

  /**
   * @deprecated This method has no support for closing the input stream appropriately. Use eachLine or lineIterator instead
   * @return
   */
  def lines: Iterator[String] = wrap(new LineIterator)

  /**
   * @deprecated use [[]]readStream instead
   * @return
   */
  def toInputStream = wrap(new ReaderInputStream(this))

  def readStream[A](f: InputStream => A): A = {
    val s = wrap(new ReaderInputStream(this))
    try
      f(s)
    finally
      close
  }


  def lineIterator[A](f: Iterator[String] => A): A = {
    val lineIt = wrap(new LineIterator)
    try
      f(lineIt)
    finally
      close
  }

  def eachLine[A](f: String => A): Unit = lineIterator(it => it.foreach(line => f(line)))

  def readAsString: String = {
    try {
      val b = new StringBuilder
      val buf = new Array[Char](8192)

      def loop: Unit = {
        val readLen = read(buf)
        if (readLen != -1) {
          b.appendAll(buf, 0, readLen)
          loop
        }
      }
      loop
      b.result()
    }
    finally
      close
  }


  override def read(target: CharBuffer) = wrap(pipeIn.read(target))

  override def read() = wrap(pipeIn.read())

  override def read(cbuf: Array[Char]) = wrap(pipeIn.read(cbuf))

  override def read(cbuf: Array[Char], offset: Int, len: Int) = wrap(pipeIn.read(cbuf, offset, len))

  override def skip(n: Long) = wrap(pipeIn.skip(n))

  override def ready() = wrap(pipeIn.ready)

  override def markSupported() = wrap(pipeIn.markSupported())

  override def mark(readAheadLimit: Int) = wrap(pipeIn.mark(readAheadLimit))

  override def reset() = wrap(pipeIn.reset)

}