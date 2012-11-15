/*
 * Copyright 2012 Taro L. Saito
 *
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
// ClioMainTest.scala
// Since: 2012/10/24 4:03 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.core.XerialSpec
import java.io.{PrintWriter, BufferedWriter, FileWriter, File}
import xerial.core.io.IOUtil
import org.apache.log4j.BasicConfigurator

/**
 * @author leo
 */
class ClioMainTest extends XerialSpec {

  BasicConfigurator.configure()

  "ClioMain" should {
    "read zookeeper-ensemble file" in {
      val t = File.createTempFile("tmp-zookeeper-ensemble", "")
      t.deleteOnExit()
      val w = new PrintWriter(new BufferedWriter(new FileWriter(t)))
      val servers = for(i <- 0 until 3) yield
        "localhost:%d:%d".format(IOUtil.randomPort, IOUtil.randomPort)
      servers.foreach(w.println(_))
      w.flush
      w.close

      val serversInFile = ClioMain.readHostsFile(t.getPath).getOrElse(Seq.empty)
      serversInFile map (_.name) should be (servers)

      val isStarted = ClioMain.checkZooKeeperServers(serversInFile)
      isStarted should be (false)
    }
  }
}