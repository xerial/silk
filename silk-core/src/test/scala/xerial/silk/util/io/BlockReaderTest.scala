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
import util.Random
import java.io._
import java.security.MessageDigest


//--------------------------------------
//
// BlockReaderTest.scala
// Since: 2012/02/20 14:10
//
//--------------------------------------

/**
 * @author leo
 */
class BlockReaderTest extends SilkSpec {

  import xerial.silk.io.InputStreamWithPrefetch
  import xerial.silk.util.TimeMeasure._

  def randomRead(size: Int): String = {
    randomString(size, "ACGT")
  }

  val qvAlphabet = (33.toChar until 126.toChar).mkString

  def randomQV(size: Int): String = randomString(size, qvAlphabet)

  def randomString(size: Int, alphabet: String): String = {
    val b = new StringBuilder
    for (i <- (0 until size)) {
      b += alphabet(Random.nextInt(alphabet.length()))
    }
    b.result
  }

  def randomFASTQ(readID: Int): String = {
    """@%s%d
%s
+
%s
""".format("read", readID, randomRead(100), randomQV(100))
  }



  trait RandomFASTQ {
    val n = 100

    def createData = {
      debug {
        "prepareing fastq data ..."
      }
      val buf = new ByteArrayOutputStream()
      val out = new BufferedOutputStream(buf)
      for (i <- (0 until n)) {
        out.write(randomFASTQ(i).getBytes("UTF-8"))
      }
      out.close()
      buf.toByteArray
    }
  }

  "BlockReader" should {
    "read data correctly" in {
      new RandomFASTQ {
        val data = createData
        val md5_a = Digest.md5sum(new InputStreamWithPrefetch(data))
        val md5_b = Digest.md5sum(new ByteArraySource(data).stream)

        md5_b must be(md5_a)
      }

    }

    "separate data reading and parsing" in {
      new RandomFASTQ {
        override val n = 10000

        val bufferSize = 1024 * 1024
        val repeat = 3
        val data = createData

        debug {
          "start reading"
        }

        time("block read", repeat = repeat) {
          for (prefetchSize <- (1 to 100 by 20)) {
            block("prefetch=" + prefetchSize) {
              val in = new ByteArrayInputStream(data)
              val s = new InputStreamWithPrefetch(in, bufferSize, prefetchSize)
              val md5 = Digest.md5sum(s)
              trace {
                "md5sum: " + md5
              }
            }
          }
          block("buffered input stream") {
            val in = new BufferedInputStream(new ByteArrayInputStream(data))
            val md5 = Digest.md5sum(in)
            trace {
              "md5sum: " + md5
            }
          }
          block("stream") {
            val in = new ByteArraySource(data, bufferSize)
            in.stream.foreach(_ => {})
            val md5 = Digest.md5sum(in.stream)
            trace {
              "md5sum: " + md5
            }
          }

        }
      }

    }
  }

}