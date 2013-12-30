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
import xerial.silk.weaver.StandaloneCluster

/**
 * @author Taro L. Saito
 */
class SilkWebServiceTest extends SilkSpec {

  var si : SilkWebService = null

  /**
   * Get the contents of the specified path from SilkWebService
   * @param path
   * @return raw HTML string
   */
  def get(path:String, port: => Int = si.port) : String = {
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

  def inCluster(body: =>Unit) {
    StandaloneCluster.withClusterAndClient { f =>
      SilkWebService.service = f
      for(web <- SilkWebService(IOUtil.randomPort)) {
        si = web
        body
      }
    }
  }


  "SilkWebService" should {

    "start jetty and serve web pages" in {
      inCluster {
        get("hello.txt") should (include ("Hello Silk"))
        get("task/list") should (include ("<body"))
        get("") should (include ("<body"))
      }
    }

    "display node list" taggedAs("nodelist") in {
      inCluster {
        get("node/list")
      }
    }



  }

}