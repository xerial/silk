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
// StandaloneClusterTest.scala
// Since: 2012/12/29 22:33
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.silk.core.Silk



/**
 * @author Taro L. Saito
 */
class StandaloneClusterTest extends SilkSpec {

  import xerial.silk._ 
  import StandaloneCluster._

  "should startup a local cluster" in {

    withCluster {
      val hosts = Silk.hosts
      debug(hosts)
      for(h <- hosts) {
        at(h) {
          val s = "hello world"
          println(s)
        }
      }
    }

  }


}