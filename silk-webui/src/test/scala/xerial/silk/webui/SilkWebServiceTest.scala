//--------------------------------------
//
// SilkWebServiceTest.scala
// Since: 2013/07/17 3:21 PM
//
//--------------------------------------

package xerial.silk.webui

import xerial.silk.util.SilkSpec
import xerial.core.io.IOUtil
import java.net.URL

/**
 * @author Taro L. Saito
 */
class SilkWebServiceTest extends SilkSpec {
  "SilkWebService" should {

    "start jetty" in {
      for(si <- SilkWebService(IOUtil.randomPort)) {
        val addr = new URL(s"http://localhost:${si.port}/hello.txt")
        IOUtil.readFully(addr.openStream) { b =>
          val contents = new String(b)
          info(s"contents: $contents")
          contents should (include ("Hello Silk"))
        }

        val addr2 = new URL(s"http://localhost:${si.port}/task/list")
        IOUtil.readFully(addr2.openStream) { b =>
          val contents = new String(b)
          contents should (include ("<body"))
        }


        val addr3 = new URL(s"http://localhost:${si.port}/")
        IOUtil.readFully(addr3.openStream) { b =>
          val contents = new String(b)
          contents should (include ("<body"))
        }

      }
    }

  }

}