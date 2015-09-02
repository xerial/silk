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
// SilkMainTest.scala
// Since: 2012/11/15 5:56 PM
//
//--------------------------------------

package xerial.silk.cui

import xerial.lens.cui.ClassOptionSchema
import xerial.lens.ObjectSchema
import xerial.silk.core.SilkSpec


import xerial.silk.framework.Host
import xerial.silk.core.util.SilkUtil


/**
 * @author leo
 */
class SilkMainTest extends SilkSpec {
  "SilkMain" should {
    "have version command" in {
      val cl = ClassOptionSchema(classOf[SilkMain])
      trace(s"SilkMain options: ${cl.options.mkString(", ")}")

      val schema = ObjectSchema(classOf[SilkMain])
      trace(s"SilkMain constructor: ${schema.constructor}")

      val out = captureOut {
        SilkMain.main("version")
      }
      debug(out)

      out should (include(SilkUtil.getVersion))
    }

    "display short message" in {
      val out = captureOut {
        SilkMain.main("")
      }

      out should (include("silk"))
      out should (include(SilkMain.DEFAULT_MESSAGE))

    }

    "check the installation of Silk" in {
      pending
      // Pending becasue we cannot use ssh in Travis CI
      val installed = SilkMain.isSilkInstalled(Host("localhost", "127.0.0.1"))
      debug(s"silk installation: $installed")
    }


    "pass command line args to eval" taggedAs("eval") in {

      SilkMain.main("eval SilkSample:in -n 10")

      SilkMain.main("eval xerial.silk.cui.SilkSample:in -n 10")

    }



  }
}