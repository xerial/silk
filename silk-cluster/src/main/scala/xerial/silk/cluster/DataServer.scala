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
import org.jboss.netty.util.{HashedWheelTimer, CharsetUtil}
import java.io._
import xerial.core.log.Logger
import javax.activation.MimetypesFileTypeMap
import java.text.SimpleDateFormat
import java.util._
import scala.Some
import xerial.larray.{MMapMode, LArray}
import scala.Some
import java.nio.ByteBuffer
import xerial.silk.core.SilkSerializer
import xerial.silk.io.ServiceGuard
import xerial.silk.util.ThreadUtil.ThreadManager
import org.jboss.netty.handler.codec.http.multipart.{Attribute, DefaultHttpDataFactory, HttpPostRequestDecoder}
import org.jboss.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType
import xerial.silk.cluster.ClassBox.JarEntry
import xerial.silk.framework.IDUtil
import org.jboss.netty.handler.timeout.{IdleState, IdleStateHandler}


object DataServer extends Logger {
  val HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz"
  val HTTP_CACHE_SECONDS = 60

  abstract class Data(createdAt:Long)
  case class MmapData(mmapFile:File, createdAt:Long) extends Data(createdAt)
  case class ByteData(ba: Array[Byte], createdAt: Long) extends Data(createdAt)
  case class RawData[A](data:Seq[A], createdAt:Long) extends Data(createdAt)

  def apply(port:Int, keepAlive:Boolean=true) : ServiceGuard[DataServer] = new ServiceGuard[DataServer] {
    protected[silk] val service = new DataServer(port, keepAlive)

    // Start a data server in a new daemon thread
    val tm = new ThreadManager(1, useDaemonThread = true)
    tm.submit {
      info(s"Start a new DataServer(port:${port})")
      service.start  // This is a blocking operation
    }

    def close {
      service.stop
      tm.join
      info(s"Closed DataServer")
    }
  }
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
class DataServer(val port:Int, keepAlive:Boolean=true) extends SimpleChannelUpstreamHandler with IDUtil with Logger {  self =>

  import DataServer._

  private var channel : Option[Channel] = None
  private val classBoxEntry = collection.mutable.Map[String, ClassBox]()
  private val jarEntry = collection.mutable.Map[String, ClassBox.JarEntry]()
  // TODO add deserialized entry
  private val dataTable = collection.mutable.Map[String, Data]()


  def registerData(id:String, mmapFile:File) {
    trace(s"registerByteData data: $id, $mmapFile")
    dataTable += id -> MmapData(mmapFile, System.currentTimeMillis)
  }

  def registerData[A](id:String, data:Seq[A]) {
    dataTable += id -> RawData(data, System.currentTimeMillis())
  }

  def registerByteData(id: String, ba: Array[Byte])
  {
    trace(s"registerByteData data: $id, ${ba.take(10)}")
    dataTable += id -> ByteData(ba, System.currentTimeMillis)
  }

  def getData(id:String) : Option[Data] = {
    dataTable.get(id)
  }


  def register(cb:ClassBox) {
    debug(s"register classbox ${cb.id.prefix} at ${cb.urlPrefix}")
    if(!classBoxEntry.contains(cb.id.prefix)) {
      for(e @ JarEntry(_, _, _) <- cb.entries) {
        addJar(e)
      }
      classBoxEntry += cb.id.prefix -> cb
    }
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

    val timer = new HashedWheelTimer()

    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pl = Channels.pipeline()
        if(keepAlive) {
          pl.addLast("idle", new IdleStateHandler(timer, 15, 15, 15) {
            override def channelIdle(ctx:ChannelHandlerContext, state:IdleState, lastActivityTimeMillis:Long) {
              state match {
                case IdleState.READER_IDLE => ctx.getChannel.close()
                case IdleState.WRITER_IDLE => ctx.getChannel.close()
                case IdleState.ALL_IDLE => ctx.getChannel.close()
              }
            }
          })
        }
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
      trace("Closing the DataServer")
      c.close
      bootstrap.releaseExternalResources
      channel = None
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val request = e.getMessage().asInstanceOf[HttpRequest]
    var writeFuture : Option[ChannelFuture] = None


    try {
      request.getMethod match {
        case GET => {
          val path = sanitizeUri(request.getUri)
          debug(s"request path: $path")

          def prepareHeader(response:HttpResponse, size:Long, createdAt:Long) {
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
          }


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
              val fileLength = file.length
              val response = new DefaultHttpResponse(HTTP_1_1, OK)
              prepareHeader(response, fileLength, jar.lastModified)

              val ch = e.getChannel
              // Write the header
              ch.write(response)
              // Write the jar content using zero-copy
              val region = new DefaultFileRegion(file.getChannel, 0, fileLength)
              writeFuture = Some(ch.write(region))
              writeFuture.map{ _.addListener(new ChannelFutureProgressListener {
                def operationProgressed(future: ChannelFuture, amount: Long, current: Long, total: Long) {}
                def operationComplete(future: ChannelFuture) {
                  region.releaseExternalResources
                }
              })}

            }
            case p if path.startsWith("/data/") =>
              // /data/(data ID)
              val dataID = path.replaceFirst("^/data/", "")

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
                  val size = file.length()
                  prepareHeader(response, size, createdAt)

                  // Write the header
                  val ch = ctx.getChannel


                  // Set data
                  val m = LArray.mmap(file, 0, size, MMapMode.READ_ONLY)
                  val buf = ChannelBuffers.wrappedBuffer(m.toDirectByteBuffer:_*)
                  ch.write(response)
                  writeFuture = Some(ch.write(buf))
                  writeFuture.map(_.addListener(new ChannelFutureListener {
                    def operationComplete(future: ChannelFuture) { m.close() }
                  }))
                }
                case ByteData(ba, createdAt) =>
                {
                  prepareHeader(response, ba.length, createdAt)
                  val ch = ctx.getChannel
                  // Write the header
                  val buf = ChannelBuffers.wrappedBuffer(ba)
                  ch.write(response)
                  writeFuture = Some(ch.write(buf))
                }
                case RawData(data, createdAt) => {
                  val ba = SilkSerializer.serializeObj(data)
                  prepareHeader(response, ba.length, createdAt)

                  val ch = ctx.getChannel
                  // Write the header
                  val buf = ChannelBuffers.wrappedBuffer(ba)
                  ch.write(response)
                  writeFuture = Some(ch.write(buf))
                }
              }


//            case POST if(path.startsWith("/data/")) => {
//              // registerByteData large data through put
//              val sliceInfo = path.replaceFirst("^/data/", "")
//              val decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(false), request)
//              val data = decoder.getBodyHttpData("data")
//              if(data.getHttpDataType == HttpDataType.Attribute) {
//                val buf = data.asInstanceOf[Attribute].getChannelBuffer
//                val bb = buf.toByteBuffer()
//              }
//
//
//
//            }
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
    }
    catch {
      case e:Exception =>
        error(e)
        sendError(ctx, INTERNAL_SERVER_ERROR)
        return
    }

    if(!keepAlive || !isKeepAlive(request)) {
      writeFuture map (_.addListener(ChannelFutureListener.CLOSE))
      writeFuture map (_.addListener(ChannelFutureListener.CLOSE_ON_FAILURE))
    }
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

