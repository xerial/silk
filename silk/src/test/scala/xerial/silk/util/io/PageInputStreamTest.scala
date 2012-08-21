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

import xerial.silk.util.SilkSpec
import java.io.PrintWriter
import xerial.core.io.PageReader

//--------------------------------------
//
// PageInputStreamTest.scala
// Since: 2012/03/14 10:53
//
//--------------------------------------

object PageInputStreamTest {
  class FASTAProducer extends TextDataProducer {
    def produce(out: PrintWriter) = {
      out.println(">seq1")
      out.println("ACGGCAGAGGCGCGCGAG")
      out.println("ACGGAAGGGAGGATTTAT")
      out.println(">seq1")
      out.println("CGAGAGCGCC")
    }
  }
}

/**
 * @author leo
 */
class PageInputStreamTest extends SilkSpec {
  import PageInputStreamTest._

  "PageInputStream" should {
    "read input stream in pages" in {
      val pageSize = 10
      val r = new PageReader(new FASTAProducer, pageSize)
      val pages = r.toArray
      pages.size must be > 0
      for(p <- pages) {
        p.size must be >= 0
        p.size must be <= pageSize
      }

    }
  }
}