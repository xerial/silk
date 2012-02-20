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

  val qvAlphabet = (33 until 126).map(_.toChar).mkString

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

  "BlockReader" should {
    "separate data reading and parsing" in {

      val n = 1000000
      val f = File.createTempFile("sample", ".fastq", new File("target"))
      try {
        val out = new BufferedOutputStream(new FileOutputStream(f))
        for (i <- (0 until n)) {
          out.write(randomFASTQ(i).getBytes("UTF-8"))
        }
        out.close()

        debug {
          "start reading"
        }

        //val in = new ByteArrayInputStream(sampleData)

        def md5sum(data: TraversableOnce[Array[Byte]]) = {
          val digest = data.foldLeft(MessageDigest.getInstance("md5")) {
            (digest, b) => digest.update(b); digest
          }
          digest.digest().map((n: Byte) => "%02x".format(n & 0xff)).mkString
        }

        val bufferSize = 1024 * 1024
        val repeat = 3

        time("block read", repeat = repeat) {
          for (prefetchSize <- (1 until 50).filter {
            i => (i == 1 || i % 5 == 0)
          }) {
            block("prefetch=" + prefetchSize) {
              val in = new FileInputStream(f)
              val s = new InputStreamWithPrefetch(in, bufferSize, prefetchSize)
              val md5 = md5sum(s)
              debug {
                "prefetch md5:\t" + md5.toString
              }
            }
          }
          block("stream") {
            val in = new FileSource(f, bufferSize)
            val md5 = md5sum(in.stream)
            debug {
              "stream md5:\t" + md5.toString
            }
          }
        }
      }
      finally {
        f.delete()
      }
    }
  }

}