//--------------------------------------
//
// SilkMacroSpec.scalaa
// Since: 2013/05/01 10:21 AM
//
//--------------------------------------

package xerial.silk.flow

import org.scalatest._
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import xerial.core.io.Resource
import xerial.core.util.Timer
import xerial.core.log.Logger
import java.io.ByteArrayOutputStream
import scala.language.implicitConversions

trait SilkMacroSpec extends WordSpec with ShouldMatchers with MustMatchers with GivenWhenThen with OptionValues with Resource with Timer with Logger with BeforeAndAfter {


  implicit def toTag(t:String) = Tag(t)

  /**
   * Captures the output stream and returns the printed messages as a String
   * @param body
   * @tparam U
   * @return
   */
  def captureOut[U](body: => U) : String = {
    val out = new ByteArrayOutputStream
    Console.withOut(out) {
      body
    }
    new String(out.toByteArray)
  }

  def captureErr[U](body: => U) : String = {
    val out = new ByteArrayOutputStream
    Console.withErr(out) {
      body
    }
    new String(out.toByteArray)
  }

}
