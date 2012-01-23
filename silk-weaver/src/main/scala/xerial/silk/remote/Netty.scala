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

package xerial.silk.remote

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import org.jboss.netty.channel.group.{DefaultChannelGroup, ChannelGroup}
import java.net.{UnknownHostException, InetAddress, InetSocketAddress}
import xerial.silk.util.Logging
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder


//--------------------------------------
//
// Netty.scala
// Since: 2012/01/23 10:29
//
//--------------------------------------


class NettyRemoteAddress(val host: String, val ip: Option[InetAddress], val port: Int) {
  override def toString = "%s@%s:%d".format(host, ip, port)
}

object NettyRemoteAddress {
  def apply(host: String, port: Int) = {
    val ip = try Some(InetAddress.getByName(host)) catch {
      case _: UnknownHostException => None
    }
    new NettyRemoteAddress(host, ip, port)
  }
}


/**
 * @author leo
 */
class NettyRemoteServer(val address: NettyRemoteAddress) extends Logging {

  if (address.ip.isEmpty) throw new java.net.UnknownHostException(address.host)

  private val factory = new NioServerSocketChannelFactory(
    Executors.newCachedThreadPool(),
    Executors.newCachedThreadPool()
  )
  private val bootstrap: ServerBootstrap = {
    val b = new ServerBootstrap()
    val config = Map(
      // "backlog" -> Backlog,
      "child.tcpNoDelay" -> true,
      "child.keepAlive" -> true,
      "child.reuseAddress" -> true
      //"child.connectTimeoutMillis" -> ConnectionTimeout.toMillis
    )
    config.foreach(x => b.setOption(x._1, x._2))
    b
  }

  private val name = "NettyRemote@" + address
  private val openChannels: ChannelGroup = new DefaultChannelGroup(name)

  openChannels.add(bootstrap.bind(new InetSocketAddress(address.ip.get, address.port)))

  def shutdown() {
    try {
      // send shutdown message
      openChannels.write("shutdown")
      openChannels.disconnect
      openChannels.close.awaitUninterruptibly
      bootstrap.releaseExternalResources
    }
    catch {
      case e: Exception => error {
        e
      }
    }
  }

}

class NettyRemoteHandler extends SimpleChannelUpstreamHandler with Logging {

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {

    val ch = e.getChannel
    val writeFuture = ch.write("hello")



  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    val cause = e.getCause
    error {
      cause
    }
  }
}

class NettyRemoteServerPipelineFactory extends ChannelPipelineFactory {
  def getPipeline : ChannelPipeline = {

    // Strip the first length field (4 bytes)
    val lengthDecorder = new LengthFieldBasedFrameDecoder(20000000, 0, 4, 0, 4)
    val remoteHandler = new NettyRemoteHandler
    val pipeline = Array[ChannelHandler](remoteHandler)
    new StaticChannelPipeline(pipeline:_*)
  }
}


