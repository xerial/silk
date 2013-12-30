//--------------------------------------
//
// SilkWebServiceTest.scala
// Since: 2013/07/17 3:21 PM
//
//--------------------------------------

package xerial.silk.webui

import xerial.silk.util.SilkSpec
import xerial.core.io.IOUtil
import java.net.{HttpURLConnection, URL}
import org.scalatest.BeforeAndAfterAll

/**
 * @author Taro L. Saito
 */
class SilkWebServiceTest extends SilkSpec with BeforeAndAfterAll {

  var si : SilkWebService = null

  def get(path:String) : String = {
    val url = new URL(s"http://localhost:${si.port}/$path")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    val ret = conn.getResponseCode
    ret match {
      case HttpURLConnection.HTTP_OK =>
        IOUtil.readFully(conn.getInputStream) { b =>
          val s = new String(b)
          debug(s"contents:\n$s")
          s
        }
      case _ =>
        fail(s"Web server returned code: $ret")
    }
  }

  before {
    si = new SilkWebService(IOUtil.randomPort)
  }

  after {
    si.close
  }


  "SilkWebService" should {

    "start jetty" in {
      get("hello.txt") should (include ("Hello Silk"))
      get("task/list") should (include ("<body"))
      get("") should (include ("<body"))
    }

    "display node list" taggedAs("nodelist") in {
      get("node/list")
    }



  }

}