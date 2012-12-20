//--------------------------------------
//
// ExampleMain.scala
// Since: 2012/12/20 5:32 PM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.DefaultMessage
import xerial.lens.cui.{command, option}
import xerial.core.log.Logger
import java.net.InetAddress
import xerial.silk.cluster.Host

/**
 * @author Taro L. Saito
 */
class ExampleMain extends DefaultMessage with Logger {

  import xerial.silk._

  @command(description = "Execute a command in remote machine")
  def remoteFunction(@option(prefix="--host", description="hostname")
                    hostName:Option[String] = None) {

    if(hostName.isEmpty) {
      warn("No hostname is given")
      return
    }

    hostName map { hn =>
      val addr = InetAddress.getByName(hn)
      val host =  Host(hn, addr.getHostAddress)
      info("Sending a command to %s", host)
      at(host) { () =>
        info("here")
        println("Hello %s".format(cluster.localhost))
      }
    }

  }


}