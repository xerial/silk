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
import org.jboss.netty.handler.stream.{ChunkedStream, ChunkedWriteHandler}
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil
import java.io._
import xerial.core.log.Logger
import javax.activation.MimetypesFileTypeMap
import java.text.SimpleDateFormat
import java.util._
import scala.Some
import xerial.larray.{MMapMode, LArray}
import scala.Some


object DataServer {
  val HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz"
  val HTTP_CACHE_SECONDS = 60

  abstract class Data(createdAt:Long)
  case class MmapData(mmapFile:File, createdAt:Long) extends Data(createdAt)
  case class ByteData(ba: Array[Byte], createdAt: Long) extends Data(createdAt)
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
class DataServer(val port:Int) extends SimpleChannelUpstreamHandler with Logger {  self =>

  import DataServer._

  private var channel : Option[Channel] = None
  private val classBoxEntry = collection.mutable.Map[String, ClassBox]()
  private val jarEntry = collection.mutable.Map[String, ClassBox.JarEntry]()
  private val dataTable = collection.mutable.Map[String, Data]()


  def registerData(id:String, mmapFile:File) {
    warn(s"register data: $id, $mmapFile")
    dataTable += id -> MmapData(mmapFile, System.currentTimeMillis)
  }

  def register(cb:ClassBox) {
    if(!classBoxEntry.contains(cb.id)) {
      for(e <- cb.entries) {
        addJar(e)
      }
      classBoxEntry += cb.id -> cb
    }
  }

  def register(id: String, ba: Array[Byte])
  {
    warn(s"register data: $id, ${ba.take(10)}")
    dataTable += id -> ByteData(ba, System.currentTimeMillis)
  }

  def containsClassBox(id:String) :Boolean = {
    classBoxEntry.contains(id)
  }
  def getClassBox(id:String) : ClassBox = classBoxEntry(id)

  private def addJar(jar:ClassBox.JarEntry) {
    jarEntry += jar.sha1sum -> jar
  }


  private var bootstrap : ServerBootstrap = null

  def start {
    bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
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
    val c = bootstrap.bind(new InetSocketAddress(port))
    channel = Some(c)
  }

  def stop {
    channel map{ c =>
      info("Closing the DataServer")
      c.close
      bootstrap.releaseExternalResources
      channel = None
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val request = e.getMessage().asInstanceOf[HttpRequest]
    var writeFuture : Option[ChannelFuture] = None
    request.getMethod match {
      case GET => {
        val path = sanitizeUri(request.getUri)
        debug(s"request path: $path")
        path match {
          case p if path.startsWith("/jars/") => {
            val uuid = path.replaceFirst("^/jars/", "")
            trace(s"uuid $uuid")
            if(!jarEntry.contains(uuid)) {
              sendError(ctx, NOT_FOUND, uuid)
              return
            }
            val jar = jarEntry(uuid)

            val f : File = {
              val p = new File(jar.path.getPath)
              if(p.exists)
                p
              else {
                val localFile = ClassBox.localJarPath(uuid)
                if(localFile.exists())
                  localFile
                else
                  null
              }
            }

            if(f == null) {
              sendError(ctx, NOT_FOUND, uuid)
              return
            }

            // open in read-only mode
            val file = new RandomAccessFile(f, "r")

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
          case p if path.startsWith("/data/") =>
            // /data/(data ID)
            val (dataID, offset, size) = {
              val c = path.replaceFirst("^/data/", "").split(":")
              // TODO error handling
              (c(0), c(1).toLong, c(2).toLong)
            }
            trace(s"dataID:$dataID")
            if(!dataTable.contains(dataID)) {
              sendError(ctx, NOT_FOUND, dataID)
              return
            }

            // Send data
            val dataEntry = dataTable(dataID)
            val response = new DefaultHttpResponse(HTTP_1_1, OK)

            dataEntry match
            {
              case MmapData(file, createdAt) =>
              {
                val m = LArray.mmap(file, 0, file.length(), MMapMode.READ_ONLY)
                setContentLength(response, size)
                response.setHeader(CONTENT_TYPE, new MimetypesFileTypeMap().getContentType(path))

                val dateFormat = new SimpleDateFormat(DataServer.HTTP_DATE_FORMAT, Locale.US)
                dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

                val cal = new GregorianCalendar()
                response.setHeader(DATE, dateFormat.format(cal.getTime))
                cal.add(Calendar.SECOND, DataServer.HTTP_CACHE_SECONDS)
                response.setHeader(EXPIRES, dateFormat.format(cal.getTime))
                response.setHeader(CACHE_CONTROL, "private, max-age=%d".format(DataServer.HTTP_CACHE_SECONDS))
                response.setHeader(LAST_MODIFIED, dateFormat.format(new Date(createdAt)))


                val ch = ctx.getChannel
                // Write the header
                ch.write(response)

                trace("after sending response header")
                // TODO avoid memory copy
                val b = new Array[Byte](size.toInt)
                m.writeToArray(offset, b, 0, size.toInt)
                val buf = ChannelBuffers.wrappedBuffer(b)
                ch.write(buf)
              }
              case ByteData(ba, createdAt) =>
              {
                setContentLength(response, size)
                response.setHeader(CONTENT_TYPE, new MimetypesFileTypeMap().getContentType(path))

                val dateFormat = new SimpleDateFormat(DataServer.HTTP_DATE_FORMAT, Locale.US)
                dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

                val cal = new GregorianCalendar()
                response.setHeader(DATE, dateFormat.format(cal.getTime))
                cal.add(Calendar.SECOND, DataServer.HTTP_CACHE_SECONDS)
                response.setHeader(EXPIRES, dateFormat.format(cal.getTime))
                response.setHeader(CACHE_CONTROL, "private, max-age=%d".format(DataServer.HTTP_CACHE_SECONDS))
                response.setHeader(LAST_MODIFIED, dateFormat.format(new Date(createdAt)))


                val ch = ctx.getChannel
                // Write the header
                ch.write(response)

                trace("after sending response header")
                val buf = ChannelBuffers.wrappedBuffer(ba)
                ch.write(buf)
              }
            }
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
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    error(e.getCause)
  }
}

