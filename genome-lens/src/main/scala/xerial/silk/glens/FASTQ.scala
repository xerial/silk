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

package xerial.silk.glens

import java.io.{BufferedReader, Reader}
import xerial.silk.core.{InvalidFormat, ParseError}


//--------------------------------------
//
// FASTQ.scala
// Since: 2012/02/20 10:58
//
//--------------------------------------



/**
 * @author leo
 */


object FASTQRead {

  def parse(in:BufferedReader) : Option[FASTQRead] = {
    val name = in.readLine
    val seq = in.readLine
    in.readLine
    val qual = in.readLine

    if(name == null || seq == null || qual == null)
      None

    if(name.length < 2)
      throw new InvalidFormat("insufficient read name length: " + name)

    Some(FASTQRead(name.substring(1), seq, qual))
  }
}

case class FASTQRead(name:String, seq:String, qual:String) {
  def toFASTQEntry : String = {
    val newLine = "\n"
    val buf = StringBuilder.newBuilder
    buf.append("@")
    buf.append(name)
    buf.append(newLine)

    buf.append(seq)
    buf.append(newLine)

    buf.append("+")
    buf.append(newLine)

    buf.append(qual)
    buf.append(newLine)
    buf.result
  }
}

object ReadIterator {
  def using[A, B  <: { def close : Unit }](resource:B)(f: B => A):A = {
    try {
      f(resource)
    }
    finally {
      resource.close
    }
  }

}
//
//class FASTQReader(in: Reader) extends ReadIterator {
//
//  import Read.stringToDNASequence
//
//  val reader = new FastqReader(in)
//
//  def this(file: File) = {
//    this (new FileReader(file))
//  }
//
//  override def next : SingleEnd = super[ReadIterator].next.asInstanceOf[SingleEnd]
//
//  protected def consume: Option[Read] = {
//    if (!current.isDefined && !finishedReading) {
//      current = reader.next match {
//        case null => finishedReading = true; reader.close; None
//        case e => Some(FASTQRead(e.seqname, e.seq, e.qual))
//      }
//    }
//    current
//  }
//
//  def close :Unit =  reader.close
//}
//
//class FASTQPairedEndReader(in1: Reader, in2: Reader) extends ReadIterator {
//  val reader1 = new FASTQFileReader(in1)
//  val reader2 = new FASTQFileReader(in2)
//
//  def this(file1: File, file2: File) = {
//    this (new FileReader(file1), new FileReader(file2))
//  }
//
//  protected def consume: Option[Read] = {
//    if (!current.isDefined && !finishedReading) {
//      val r1 = reader1.next
//      val r2 = reader2.next
//      current = (r1, r2) match {
//        case (a: SingleEnd, b: SingleEnd) => Some(PairedEndRead(a, b))
//        case (a: SingleEnd, null) => Some(r1)
//        case (null, b: SingleEnd) => Some(r2)
//        case _ => finishedReading; None
//      }
//    }
//    current
//  }
//
//  def close : Unit = {
//    reader1.close
//    reader2.close
//  }
//
//}
//
//
//class ParallelReadReader {
//
//  import scala.actors.Actor
//  import scala.actors.Actor._
//
//  class Master extends Actor {
//    private val master = self
//
//    def map(in:ReadIterator) = {
//      master.trapExit = true
//
//      def spawnMapper[A](input:A) = {
//        val mapper = link _{
//          master ! block
//        }
//        mapper
//      }
//
//      for(block <- in.blockIterator) {
//        spawnMapper(block)
//      }
//
//
//
//    }
//
//
//  }
//
//}
//
//
