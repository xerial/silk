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
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil
import java.io.UnsupportedEncodingException
import xerial.core.log.Logger

/**
 * DataServer is a content provider of jar files and serialized data.
 *
 * jar file address:
 * http://(host address):(config.dataServerPort)/jars/UUID.jar
 *
 * data address:
 * http://(host address):(config.dataServerPort)/data/UUID
 *
 *
 *
 * @author Taro L. Saito
 */
object DataServer extends Logger {



  def run(port:Int) = {
    val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool,
      Executors.newCachedThreadPool
    ))

    bootstrap.setPipelineFactory(new DataServerFactory())
    val channel = bootstrap.bind(new InetSocketAddress(port))
    channel
  }

  private class DataServerFactory extends ChannelPipelineFactory {
    def getPipeline = {
      val pl = Channels.pipeline()
      pl.addLast("decoder", new HttpRequestDecoder())
      pl.addLast("aggregator", new HttpChunkAggregator(65536))
      pl.addLast("encoder", new HttpResponseEncoder)
      pl.addLast("chunkedWriter", new ChunkedWriteHandler())
      pl.addLast("handler", new DataServerHandler)
      pl
    }
  }


  private class DataServerHandler extends SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val request = e.getMessage().asInstanceOf[HttpRequest]

      request.getMethod match {
        case GET => {
          val path = sanitizeUri(request.getUri)

          path match {
            case p if path.startsWith("jars/") =>
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

    private def sendError(ctx:ChannelHandlerContext, status:HttpResponseStatus) {
      val response = new DefaultHttpResponse(HTTP_1_1, status)
      response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8")
      response.setContent(ChannelBuffers.copiedBuffer("Failure: %s\n".format(status), CharsetUtil.UTF_8))
      ctx.getChannel.write(response).addListener(ChannelFutureListener.CLOSE)
    }
  }
}


