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
// Remote.scala
// Since: 2012/12/20 2:22 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.cluster.SilkClient.{Register, Run}
import xerial.core.log.Logger

/**
 * Remote command launcher
 * @author Taro L. Saito
 */
object Remote extends Logger {

  /**
   * Run the given function at the specified host
   * @param host
   * @param f
   * @tparam U
   * @return
   */
  def at[U](host:Host)(f: () => U) : U = {
    // TODO copy closure environment
    val closureCls = f.getClass
    val classBox = ClassBox.current

    val localClient = SilkClient.getClientAt(localhost.address)
    localClient ! Register(classBox)

    // Get remote client
    info("getting remote client at %s", host.address)
    val client = SilkClient.getClientAt(host.address)
    // TODO Support functions with arguments
    // Send a remote command request
    client ! Run(classBox, closureCls.getName)


    // TODO retrieve result
    null.asInstanceOf[U]
  }

  private[cluster] def run(cb:ClassBox, className:String) {
    info("Running command at %s", localhost)

    val myClassBox = cb.resolve

    val cl = myClassBox.classLoader
    ClassBox.withClassLoader(cl) {
      val mainClass = cl.loadClass(className)
      val m = mainClass.getMethod("apply")
      info("invoke method: %s, className:%s", m.getName, className)
      // TODO instantiate the closure instance
      m.invoke(null)
    }
  }

}