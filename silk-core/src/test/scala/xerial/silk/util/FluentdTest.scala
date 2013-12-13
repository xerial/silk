//--------------------------------------
//
// FluentdTest.scala
// Since: 2013/12/12 3:17 PM
//
//--------------------------------------

package xerial.silk.util

import org.fluentd.logger.scala.FluentLoggerFactory
import org.msgpack.MessagePack
import java.net.Socket


object FluentdTest {
  case class MyMessage(num:Int, message:String, seq:Seq[Int])
}

/**
 * @author Taro L. Saito
 */
class FluentdTest extends SilkSpec {

  import FluentdTest._

  "fluentd-logger" should {

    "output logs" in {

      val logger = FluentLoggerFactory.getLogger("log")


      logger.log("debug", Map("message" -> "hello fluentd"))

      for(i <- 0 until 5) {
        logger.log("trace", "loop", i)
      }

      for(i <- 0 until 5) {
        logger.log("debug.FluentdTest", "message", MyMessage(i, "Hi", Seq(0, 3, 4)))
      }

    } 

    "output msgpack" in {
      val m = new MessagePack
      val packer = m.createPacker(new Socket("localhost", 24224).getOutputStream)

      for(i <- 0 until 5) {
        packer.writeArrayBegin(3)
        packer.write("log.silk")
        packer.write(System.currentTimeMillis() / 1000)
        packer.write(s"$i:hello fluentd")
//        packer.writeArrayBegin(4)
//        packer.write("message")
//        packer.write("hello")
//        packer.write("count")
//        packer.write(i)
//        packer.writeArrayEnd(true)
        packer.writeArrayEnd
      }

    }


  }

}