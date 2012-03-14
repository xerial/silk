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
import xerial.silk.util.Logging


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
trait DataProducerBase[PipeIn <: Closeable, PipeOut <: Closeable] extends Closeable {

  protected val pipeIn : PipeIn
  protected val pipeOut : PipeOut

  protected val worker = new Thread(new Runnable {
    def run() {
      try {
        produceStart
      }
      catch {
        case e: InterruptedException => // terminated by close
      }
    }
  })

  protected def startWorker : Unit = {
    worker.setDaemon(true) // enable JVM terminate without stopping the worker
    worker.start()
  }

  /**
   * Start data production
   */
  protected def produceStart : Unit

  override def close {
    // No need exists to close the reader explicitely since PipeReader.close simply reset the buffer
    //pipeIn.close
    pipeOut.close
    if(worker.isAlive)
      worker.interrupt
  }
}


/**
 * Data producer produces data using a thread, and provides input stream to fetch the data.
 * Implement the produce method to use this trait.
 *
 * @author leo
 */
trait DataProducer extends InputStream with DataProducerBase[InputStream, OutputStream] {

  protected val pipeIn = new PipedInputStream
  protected val pipeOut = new PipedOutputStream(pipeIn)

  startWorker

  protected def produceStart : Unit = {
    try
      produce(pipeOut)
    finally {
      pipeOut.flush
      pipeOut.close
    }
  }
  
  def produce(out:OutputStream) : Unit
  
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
trait TextDataProducer extends Reader with DataProducerBase[Reader, Writer] with Logging {

  override protected val pipeIn = new PipedReader
  override protected val pipeOut = new PrintWriter(new PipedWriter(pipeIn))

  startWorker

  protected def produceStart : Unit = {
    try {
      produce(pipeOut)
    }
    finally {
      pipeOut.flush
      pipeOut.close
    }
  }

  def produce(out: PrintWriter) : Unit

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

  def lines: Iterator[String] = new LineIterator
  def toInputStream = new ReaderInputStream(this)
  
  private def wrapRead(f: => Int) : Int = {
    try {
      f
    }
    catch {
      case e:InterruptedIOException => -1
    }

  }
  
  
  override def read(target: CharBuffer) = wrapRead(pipeIn.read(target))

  override def read() = wrapRead(pipeIn.read())

  override def read(cbuf: Array[Char]) = wrapRead(pipeIn.read(cbuf))
  
  override def read(cbuf: Array[Char], offset:Int, len:Int) = wrapRead(pipeIn.read(cbuf, offset, len))

  override def skip(n: Long) = pipeIn.skip(n)

  override def ready() = pipeIn.ready

  override def markSupported() = pipeIn.markSupported()

  override def mark(readAheadLimit: Int) { pipeIn.mark(readAheadLimit) }

  override def reset() { pipeIn.reset }

}