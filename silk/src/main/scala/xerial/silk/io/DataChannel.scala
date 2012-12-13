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

package xerial.silk.io

import java.util.concurrent.ArrayBlockingQueue
import actors.Actor
import java.io.{OutputStream, Closeable}

//--------------------------------------
//
// DataChannel.scala
// Since: 2012/02/29 22:45
//
//--------------------------------------


trait Peekable[T] {
  def peek: T
}

trait DataOutputChannel[A] extends Closeable {
  def write(elem: A): Unit
}

trait DataInputChannel[A] extends Iterator[A] with Peekable[A] {

  def hasNext: Boolean

  def peek: A

  def next: A
}

/**
 * Channel supporting concurrent read/write of typed data streams
 * @tparam A data type to transfer
 */
trait DataChannel[A] extends DataInputChannel[A] with DataOutputChannel[A] {

  protected val queueSize: Int = 5

  @volatile private var closed: Boolean = false
  private var current: Option[A] = None
  private val queue = new ArrayBlockingQueue[A](queueSize)
  private var waitingThreads: List[Thread] = List.empty

  def isClosed: Boolean = closed

  def isOpen = !closed

  /**
   * Close the channel. Even after closing the channel, it is possible to continue reading data buffered in the channel
   */
  def close(): Unit = {
    closed = true
    // Awake pending threads
    waitingThreads.foreach(t => t.interrupt())
  }

  def write(elem: A): Unit = {
    queue.put(elem)
  }

  protected def readNext: Option[A] = {
    try {
      if (!queue.isEmpty) {
        Some(queue.take)
      }
      else if (isClosed)
        None
      else {
        // Wait until new entry is coming
        try {
          val c = Thread.currentThread
          waitingThreads = c :: waitingThreads
          Some(queue.take)
        }
        finally {
          waitingThreads = waitingThreads.tail
        }
      }
    }
    catch {
      case e: InterruptedException => readNext
    }
  }

  def hasNext: Boolean = {
    // Ensure that if this method returns true, the option, current, is set to the next value
    if (!current.isDefined) {
      current = readNext
    }
    current.isDefined
  }

  def peek: A = {
    if (hasNext)
      current.get
    else
      throw new NoSuchElementException("peek(next)")
  }

  def next: A = {
    if (hasNext) {
      val e = current.get
      current = None
      e
    }
    else
      throw new NoSuchElementException("next")
  }

}

class BlockingDataChannel[A](override protected val queueSize: Int = 5) extends DataChannel[A] {

}

trait Producer[A] {
  def produce(channel: DataOutputChannel[A]): Unit
}

trait Consumer[A] {
  def consume(channel: DataInputChannel[A]): Unit
}

class DataChannelReader[A](producer: Producer[A]) extends DataInputChannel[A] {
  private val channel = new BlockingDataChannel[A]()

  private val reader = new Thread(
    new Runnable {
      def run() = producer.produce(channel)
    }
  )

  reader.setDaemon(true)
  reader.start

  def hasNext = channel.hasNext

  def peek = channel.peek

  def next = channel.next
}



