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

import xerial.silk.util.SilkSpec
import java.io._
import java.util.Random
import xerial.core.io.TextDataProducer


//--------------------------------------
//
// BlockReaderTest.scala
// Since: 2012/02/20 14:10
//
//--------------------------------------



class RandomFASTQGenerator(n:Int = 100, seed:Long=0L) extends TextDataProducer {
  val rand = new Random(seed) // Use fixed seed for generating the same data
  // qvAlphabet = (33.toChar until 126.toChar).mkString
  val qvAlphabet = """!"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}"""

  def produce(out: PrintWriter) {
    for(i<- 0 until n) {
      out.println(randomFASTQ(i))
    }
  }

  def randomRead(size: Int): String = {
    randomString(size, "ACGT")
  }

  def randomQV(size: Int): String = randomString(size, qvAlphabet)

  def randomString(size: Int, alphabet: String): String = {
    val b = new StringBuilder
    val len = alphabet.length
    for (i <- (0 until size)) {
      val ch = rand.nextInt(len)
      b += alphabet(ch)
    }
    b.result
  }

  def randomFASTQ(readID: Int): String = {
    Seq("@%s%d".format("read",readID), randomRead(100), "+", randomQV(100)).mkString("\n")
  }

}

trait RandomFASTQ {
  val n = 100

  def inputStream = {
    //debug("preparing %,d fastq reads ...", n)
    new RandomFASTQGenerator(n).toInputStream
  }
}


/**
 * @author leo
 */
class BlockReaderTest extends SilkSpec {

  import xerial.silk.io.InputStreamWithPrefetch

  "BlockReader" should {
    "read data correctly" in {
      new RandomFASTQ {
        val md5_a = Digest.md5sum(new InputStreamWithPrefetch(inputStream))
        val md5_b = Digest.md5sum(inputStream)

        md5_b must be(md5_a)
      }
    }

    "separate data reading and parsing" in {
      new RandomFASTQ {
        override val n = 5000

        val bufferSize = 8 * 1024
        val repeat = 3

        val md5_ans = Digest.md5sum(inputStream)
        trace { "md5sum: " + md5_ans }

        time("block read", repeat = repeat) {
          for (prefetchSize <- (1 to 100 by 20)) {
            block("prefetch=" + prefetchSize) {
              val s = new InputStreamWithPrefetch(inputStream, bufferSize, prefetchSize)
              val md5 = Digest.md5sum(s)
              md5 must be (md5_ans)
            }
          }
          block("PageInputStream") {
            val md5 = Digest.md5sum(inputStream)
            md5 must be (md5_ans)
          }
        }
      }

    }
  }

}