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

//--------------------------------------
//
// DataServerTest.scala
// Since: 2012/12/20 18:53
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.{ThreadUtil, SilkSpec}
import xerial.core.io.IOUtil
import java.net.URL
import xerial.silk.io.Digest
import java.util.concurrent.TimeUnit

/**
 * @author Taro L. Saito
 */
class DataServerTest extends SilkSpec {

  val port =  IOUtil.randomPort
  var t : ThreadUtil.ThreadManager = null
  @volatile var ds : DataServer = null

  before {
    val b = new Barrier(2)
    t = ThreadUtil.newManager(1)
    t.submit {
      ds = new DataServer(port)
      ds.start
      b.enter("ready")
    }
    b.enter("ready")
    debug("DataServer is ready")
  }

  after {
    if(ds != null)
      ds.stop
    if(t != null)
      t.join
  }

  "DataServer" should {

    "provide ClassBox entries" in {
      val cb = ClassBox.current
      ds.register(cb)

      for(e <- cb.entries.par) {
        val sha1 = e.sha1sum
        val url = new URL("http://127.0.0.1:%d/jars/%s".format(port, sha1))
        debug("Retrieving %s", e)
        IOUtil.readFully(url.openStream()) { data =>
          val sha1sum = Digest.sha1sum(data)
          sha1 should be (sha1sum)
        }
      }
    }
  }

}