//--------------------------------------
//
// ClassBoxTest.scala
// Since: 2012/12/20 10:34 AM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.{ThreadUtil, SilkSpec}
import xerial.lens.TypeUtil

object ClassBoxTest {

  def thisClass = this.getClass

  def hello : String = { "hello" }
}

/**
 * @author Taro L. Saito
 */
class ClassBoxTest extends SilkSpec {

  "ClassBox" should {
    "enumerate entries in classpath" in {
      val cb = ClassBox.current
      debug("md5sum of classbox: %s", cb.md5sum)
    }

    "create a classloder" in {
      val cb = ClassBox.current
      val loader = cb.classLoader

      val h1 = ClassBoxTest.thisClass
      var h2 : Class[_] = null
      var mesg : String = null
      val t = ThreadUtil.newManager(1)
      t.submit {
        val prevCl = Thread.currentThread.getContextClassLoader
        try {
          Thread.currentThread.setContextClassLoader(loader)
          h2 = loader.loadClass("xerial.silk.cluster.ClassBoxTest$")
          val m = h2.getMethod("hello")
          mesg = TypeUtil.companionObject(h2) map { co =>  m.invoke(co).toString } getOrElse (null)
        }
        catch {
          case e => warn(e)
        }
        finally {
          Thread.currentThread.setContextClassLoader(prevCl)
        }
      }
      t.awaitTermination()

      // Class loaded by different classloaders should have different IDs
      h1 should not be (h2)
      mesg should be ("hello")
    }
  }
}