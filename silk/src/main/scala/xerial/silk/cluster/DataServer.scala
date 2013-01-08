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
// DataServer.scala
// Since: 2012/12/20 2:54 PM
//
//--------------------------------------

package xerial.silk.cluster

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import java.net.{URLDecoder, InetSocketAddress}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.stream.ChunkedWriteHandler
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil
import java.io.{FileNotFoundException, RandomAccessFile, File, UnsupportedEncodingException}
import xerial.core.log.Logger
import javax.activation.MimetypesFileTypeMap
import java.text.SimpleDateFormat
import java.util._
import scala.Some


object DataServer {
  val HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz"
  val HTTP_CACHE_SECONDS = 60

}

/**
 * DataServer is a HTTP server that provides jar files and serialized data.
 * For the implementation of the HTTP server, Netty, a fast network library, is used.
 *
 * jar file address:
 * http://(host address):(config.dataServerPort)/jars/UUID
 *
 * data address:
 * http://(host address):(config.dataServerPort)/data/UUID
 *
 * @author Taro L. Saito
 */
class DataServer(port:Int) extends SimpleChannelUpstreamHandler with Logger {  self =>

  private var channel : Option[Channel] = None
  private val classBoxEntry = collection.mutable.Map[UUID, ClassBox]()
  private val jarEntry = collection.mutable.Map[String, ClassBox.JarEntry]()

  def register(cb:ClassBox) {
    for(e <- cb.entries) {
      addJar(e)
    }
    classBoxEntry += cb.uuid -> cb
  }

  def containsClassBox(uuid: UUID) :Boolean = {
    classBoxEntry.contains(uuid)
  }

  private def addJar(jar:ClassBox.JarEntry) {
    jarEntry += jar.sha1sum -> jar
  }

  def start {
    val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool,
      Executors.newCachedThreadPool
    ))

    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pl = Channels.pipeline()
        pl.addLast("decoder", new HttpRequestDecoder())
        pl.addLast("aggregator", new HttpChunkAggregator(65536))
        pl.addLast("encoder", new HttpResponseEncoder)
        pl.addLast("chunkedWriter", new ChunkedWriteHandler())
        pl.addLast("handler", self)
        pl
      }
    })

    channel = Some(bootstrap.bind(new InetSocketAddress(port)))
  }

  def stop {
    channel map{ c =>
      info("Closing the DataServer")
      c.close
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val request = e.getMessage().asInstanceOf[HttpRequest]
    var writeFuture : Option[ChannelFuture] = None
    request.getMethod match {
      case GET => {
        val path = sanitizeUri(request.getUri)
        info("request path: %s", path)
        path match {
          case p if path.startsWith("/jars/") => {
            val uuid = path.replaceFirst("^/jars/", "")
            debug("uuid %s", uuid)
            if(!jarEntry.contains(uuid)) {
              sendError(ctx, NOT_FOUND, uuid)
              return
            }
            val jar = jarEntry(uuid)
            // open in read-only mode
            val f = new File(jar.path.getPath)
            val file = try
              new RandomAccessFile(f, "r")
              catch {
                case e:FileNotFoundException => sendError(ctx, NOT_FOUND, f.getPath); return;
              }


            val response = new DefaultHttpResponse(HTTP_1_1, OK)
            val fileLength = file.length
            setContentLength(response, fileLength)
            response.setHeader(CONTENT_TYPE, new MimetypesFileTypeMap().getContentType(path))

            val dateFormat = new SimpleDateFormat(DataServer.HTTP_DATE_FORMAT, Locale.US)
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

            val cal = new GregorianCalendar()
            response.setHeader(DATE, dateFormat.format(cal.getTime))
            cal.add(Calendar.SECOND, DataServer.HTTP_CACHE_SECONDS)
            response.setHeader(EXPIRES, dateFormat.format(cal.getTime))
            response.setHeader(CACHE_CONTROL, "private, max-age=%d".format(DataServer.HTTP_CACHE_SECONDS))
            response.setHeader(LAST_MODIFIED, dateFormat.format(new Date(jar.lastModified)))

            val ch = e.getChannel
            // Write the header
            ch.write(response)
            // Write the jar content using zero-copy
            val region = new DefaultFileRegion(file.getChannel, 0, fileLength)
            writeFuture = Some(ch.write(region))
            writeFuture.map{ _.addListener(new ChannelFutureProgressListener {
              def operationProgressed(future: ChannelFuture, amount: Long, current: Long, total: Long) {}
              def operationComplete(future: ChannelFuture) {}
            })}

          }
          case p if path.startsWith("data/") =>
          case _ => {
            sendError(ctx, NOT_FOUND)
            return
          }
        }
      }
      case _ =>
        sendError(ctx, METHOD_NOT_ALLOWED)
        return
    }

    if(!isKeepAlive(request))
      writeFuture map (_.addListener(ChannelFutureListener.CLOSE))
  }

  private def sanitizeUri(uri:String) : String = {
    val decoded = try
      URLDecoder.decode(uri, "UTF-8")
    catch {
      case e:UnsupportedEncodingException => try
        URLDecoder.decode(uri, "ISO-8859-1")
      catch {
        case e1:UnsupportedClassVersionError => {
          error(e1)
          throw e1
        }
      }
    }
    decoded
  }

  private def sendError(ctx:ChannelHandlerContext, status:HttpResponseStatus, message:String="") {
    val response = new DefaultHttpResponse(HTTP_1_1, status)
    response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8")
    response.setContent(ChannelBuffers.copiedBuffer("Failure: %s\n%s".format(status, message), CharsetUtil.UTF_8))
    ctx.getChannel.write(response).addListener(ChannelFutureListener.CLOSE)
  }

}

