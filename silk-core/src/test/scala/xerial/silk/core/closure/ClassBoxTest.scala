/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// ClassBoxTest.scala
// Since: 2012/12/20 10:34 AM
//
//--------------------------------------

package xerial.silk.core.closure

import java.io.File

import xerial.lens.TypeUtil
import xerial.silk.core.SilkSpec
import xerial.silk.core.util.ThreadUtil

object ClassBoxTest {

  def thisClass = this.getClass

  def hello: String = {
    "hello"
  }
}

/**
 * @author Taro L. Saito
 */
class ClassBoxTest extends SilkSpec {

  import ClassBox._

  val current = ClassBox.getCurrent(new File("target/classbox"), -1)

  "ClassBox" should {
    "enumerate entries in classpath" in {
      val cb = current
      debug(s"sha1sum of classbox: ${cb.sha1sum}")
    }

    "create a classloder" in {
      val cb = current
      val loader = cb.isolatedClassLoader

      val h1 = ClassBoxTest.thisClass
      var h2: Class[_] = null
      @volatile var mesg: String = null
      val t = ThreadUtil.newManager(1)
      t.submit {
        withClassLoader(loader) {
          try {
            h2 = loader.loadClass("xerial.silk.core.closure.ClassBoxTest")
            val m = h2.getMethod("hello")
            mesg = TypeUtil.companionObject(h2) map { co => m.invoke(co).toString } getOrElse {
              warn(s"no companion object for $h2 is found")
              null
            }
          }
          catch {
            case e: Exception => warn(e)
          }
        }
      }
      t.join

      // Class loaded by different class loaders should have different IDs
      h1 should not be (h2)
      mesg should be("hello")
    }

    "create local only ClassBox" in {
      val cb = ClassBox.localOnlyClassBox(-1)

      var mesg: String = null
      val loader = cb.isolatedClassLoader
      trace(s"${loader.getURLs.mkString(", ")}")
      withClassLoader(loader) {
        val h2 = loader.loadClass("xerial.silk.core.closure.ClassBoxTest")
        val m = h2.getMethod("hello")
        mesg = m.invoke(null).asInstanceOf[String]
      }

      mesg shouldBe "hello"
    }
  }
}