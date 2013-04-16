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
// ZooKeeperTest.scala
// Since: 2012/10/24 10:17 AM
//
//--------------------------------------

package xerial.silk.cluster

import collection.JavaConversions._
import com.netflix.curator.test.{TestingCluster, TestingServer}
import com.netflix.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.google.common.io.Closeables
import org.scalatest.BeforeAndAfter
import org.apache.log4j.{Level, ConsoleAppender, BasicConfigurator}
import java.util.concurrent.{Executors, TimeUnit, Callable}
import com.netflix.curator.framework.recipes.leader.{LeaderSelectorListener, LeaderSelector}
import java.io._
import com.netflix.curator.framework.state.ConnectionState
import util.Random
import com.netflix.curator.utils.{ZKPaths, EnsurePath}
import xerial.core.io.IOUtil
import xerial.silk.util.{ThreadUtil, SilkSpec}
import xerial.silk.SilkMain


/**
 * @author leo
 */
class ZooKeeperTest extends SilkSpec with BeforeAndAfter {

  xerial.silk.suppressLog4jwarning

  var server: TestingServer = null
  var client: CuratorFramework = null


  before {
    debug("starting zookeeper server")
    server = new TestingServer
    info(s"zookeeper server string: ${server.getConnectString}")
    client = CuratorFrameworkFactory.newClient(server.getConnectString, new ExponentialBackoffRetry(1000, 3))
    client.start
  }

  after {
    Closeables.closeQuietly(client)
    Closeables.closeQuietly(server)
  }

  class LeaderSelectorExample(val client:CuratorFramework, path:String, name:String) extends Closeable with LeaderSelectorListener {

    private var ourThread : Thread = null

    private val leaderSelector = new LeaderSelector(client, path, this)
    leaderSelector.autoRequeue

    def start {
      debug(s"starting $name")
      leaderSelector.start
    }

    def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      if(newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
        if(ourThread != null)
          ourThread.interrupt
      }
    }

    def takeLeadership(client: CuratorFramework) {
      val path = new EnsurePath("/xerial-clio/leader")
      path.ensure(client.getZookeeperClient)

      val prevLeader = new String(client.getData().forPath("/xerial-clio/leader"))

      debug(s"$name takes the leadership (previous leader was $prevLeader)")

      debug(s"leader selector has ${leaderSelector.getParticipants.size} participants")
      ourThread = Thread.currentThread
      try {
        client.setData().forPath("/xerial-clio/leader", name.getBytes)

        Thread.sleep(TimeUnit.SECONDS.toMillis(1))
      }
      catch {
        case e:InterruptedException => {
          debug(s"$name was interrupted")
          Thread.currentThread.interrupt
        }
      }
      finally {
        ourThread = null
        debug(s"$name relinquished the leadership")
      }

    }

    def close() {
      debug(s"closing $name")
      leaderSelector.close
    }
  }


  "ZooKeeper" should {

    "start a server" in {

      val started = client.isStarted
      started should be (true)

      debug("create znode")
      client.create().forPath("/xerial-clio")
      client.create().forPath("/xerial-clio/data")

      debug("write data")
      client.setData.forPath("/xerial-clio/data", "Hello ZooKeeper!".getBytes)

      debug("read data")
      val s = client.getData().forPath("/xerial-clio/data")
      new String(s) should be("Hello ZooKeeper!")
    }




    "elect a leader" in {
      val clients = for(i <- 0 until 5) yield {
        val c = CuratorFrameworkFactory.newClient(server.getConnectString, new ExponentialBackoffRetry(1000, 3))
        val s = new LeaderSelectorExample(c, "/xerial-clio/test/leader", "client%d".format(i))
        c.start
        s.start
        (c, s)
      }

      Thread.sleep(TimeUnit.SECONDS.toMillis(5))

      for((c, s) <- clients) {
        Closeables.closeQuietly(s)
      }

      for((c, s) <- clients) {
        Closeables.closeQuietly(c)
      }
    }
  }


}

class ZkPathTest extends SilkSpec {

  import ZkPath._

  "ZkPath" should {
    "have string constructor" in {
      val p = ZkPath("/silk/cluster")
      p.path should be("/silk/cluster")
      p.parent.map { parent =>
        parent.path should be ("/silk")
      } getOrElse {
        fail("parent should be exist")
      }
    }
  }

}


class ZooKeeperEnsembleTest extends SilkSpec {

  xerial.silk.suppressLog4jwarning

  var server: TestingCluster = null

  before {
    xerial.silk.configureLog4jWithLogLevel(Level.FATAL)

    server = new TestingCluster(5)
    server.start

    info(s"started zookeeper ensemble: ${server.getConnectString}")
  }

  after {
    Closeables.closeQuietly(server)
    xerial.silk.configureLog4j
  }

  def withClient[U](f: CuratorFramework => U) : U = {
    val c = CuratorFrameworkFactory.newClient(server.getConnectString, new ExponentialBackoffRetry(1000, 3))
    try {
      c.start
      f(c)
    }
    finally {
      Closeables.closeQuietly(c)
    }
  }


  "ZooKeeperEnsemble" should {

    "run safely even if one of the nodes is down" in {
      val m = "Hello Zookeeper Quorum"

      withClient { client =>
        client.create.forPath("/xerial-clio")
        client.create.forPath("/xerial-clio/demo")
        client.setData.forPath("/xerial-clio/demo", m.getBytes)
        val servers = server.getInstances.toSeq
        val victim = servers(Random.nextInt(servers.size))
        debug(s"kill a zookeeper server: $victim")
        server.killServer(victim)

        TimeUnit.SECONDS.sleep(1)

        val b = client.getData.forPath("/xerial-clio/demo")
        new String(b) should be (m)

        info(s"restart a zookeeper server: $victim")
        server.restartServer(victim)

        TimeUnit.SECONDS.sleep(1)

        val b2 = client.getData.forPath("/xerial-clio/demo")
        new String(b2) should be (m)
      }

      withClient { client =>
        val b = client.getData.forPath("/xerial-clio/demo")
        new String(b) should be (m)
      }

    }

  }

}
