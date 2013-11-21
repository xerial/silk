//--------------------------------------
//
// SilkFramework.scala
// Since: 2013/06/09 11:44
//
//--------------------------------------

package xerial.silk.framework

import scala.language.higherKinds
import xerial.silk._
import xerial.core.log.Logger
import java.net.InetAddress
import xerial.core.util.DataUnit
import java.io.{File, ObjectOutputStream}
import scala.collection.GenSeq
import xerial.silk.core.CallGraph
import xerial.silk.util.Path

import Path._
import scala.io.Source
import xerial.silk.framework.NodeRef
import xerial.silk.framework.NodeResource
import xerial.silk.framework.ResourceRequest
import scala.Some
import xerial.silk.framework.Node
import xerial.silk.framework.Slice

/**
 * SilkFramework contains the abstraction of input and result data types of Silk operations.
 *
 * @author Taro L. Saito
 */
trait SilkFramework {

  // Abstraction of configuration type. This type varies according to runtime-framework to use.
  // For example, if one needs to use local framework, only the LocalConfig type is set
  type Config
  def config : Config

  def run[A](silk:SilkSeq[A]) : Seq[A] = eval(silk).get
  def run[A](silk:SilkSingle[A]) : A = eval(silk).get
  def eval[A](silk:SilkSeq[A]) : SilkFuture[Seq[A]]
  def eval[A](silk:SilkSingle[A]) : SilkFuture[A]

}

object HomeConfig {

  def defaultSilkHome : File = {
    sys.props.get("silk.home") map { new File(_) } getOrElse {
      val homeDir = sys.props.get("user.home") getOrElse ("")
      new File(homeDir, ".silk")
    }
  }
}

case class HomeConfig(silkHome : File = HomeConfig.defaultSilkHome) {
  val silkHosts : File = silkHome / "hosts"


  val silkConfig : File = silkHome / "config.silk"
  val silkLocalDir : File = silkHome / "local"
  val silkSharedDir : File = silkHome / "shared"
  val silkTmpDir : File = silkLocalDir / "tmp"
  val silkLogDir : File = silkLocalDir / "log"


  // Preparing the local directories
  for(d <- Seq(silkLocalDir, silkSharedDir, silkTmpDir, silkLogDir) if !d.exists) d.mkdirs
}

trait HomeConfigComponent {

  val home = new HomeConfig

}




trait ExecutorAPI {
  def getSlices[A](op: Silk[A]) : GenSeq[SilkFuture[Slice]]
}




trait SerializationService {

  implicit class Serializer(a:Any) {
    def serialize : Array[Byte] = SilkSerializer.serializeObj(a)
    def serializeTo(oos:ObjectOutputStream) = {
      oos.writeObject(a)
    }
  }


  implicit class Deserializer(b:Array[Byte]) {
    def deserialize[A] : A = SilkSerializer.deserializeObj[A](b)
  }
}



/**
 * Components that need some initialization and termination steps should override this trait.
 * startup and teardown methods will be invoked from SilkClientService or SilkMasterService.
 */
trait LifeCycle {

  def startup {}
  def teardown {}
}





/**
 * Representing worker node
 * @param name
 * @param address
 * @param clientPort
 * @param dataServerPort
 */
case class Node(name:String,
                address:String,
                pid:Int,
                clientPort:Int,
                dataServerPort:Int,
                webuiPort:Int,
                resource:NodeResource) {
  def host = Host(name, address)
  def toRef = NodeRef(name, address, clientPort)
}

case class NodeRef(name:String, address:String, clientPort:Int) {
  def host = Host(name, address)
}



object Host extends Logger {
  def apply(s:String) : Host = {
    val lh = InetAddress.getByName(s)
    Host(s, lh.getHostAddress)
  }

  def readHostsFile(f:File) : Seq[Host] = {
    val hosts = for(line <- Source.fromFile(f).getLines; h <- parseHostsLine(line)) yield h
    hosts.toSeq
  }

  def parseHostsLine(line:String) : Option[Host] = {
    try {
      // Strip by white spaces (hostname, ip address)
      val trimmed = line.trim
      val c = trimmed.split("""\s+""")
      if(trimmed.startsWith("#") || trimmed.isEmpty)
        None
      else if(c.length >= 1 && !c(0).isEmpty) {
        if(c.length > 1 && !c(1).isEmpty)
          Some(new Host(c(0), c(1)))
        else
          Some(apply(c(0)))
      }
      else
        None
    }
    catch {
      case e:Exception =>
        warn(s"invalid line: $line")
        None
    }
  }
}

case class Host(name: String, address: String) {
  def prefix = name.split("\\.")(0)
}



case class NodeResource(nodeName:String, numCPUs:Int, memorySize:Long) {

  def readableMemorySize = DataUnit.toHumanReadableFormat(memorySize)

  private def ensureSameNode(n:String) {
    require(nodeName == n, "must be the same node")
  }

  private def ensureNonNegative(v:Long) = if(v < 0) 0L else v

  def adjustFor(r:ResourceRequest) = {
    NodeResource(nodeName, r.cpu, r.memorySize.getOrElse(-1))
  }

  def -(r:NodeResource) = {
    ensureSameNode(r.nodeName)
    NodeResource(nodeName, numCPUs - r.numCPUs, memorySize - ensureNonNegative(r.memorySize))
  }

  def +(r:NodeResource) = {
    ensureSameNode(r.nodeName)
    NodeResource(nodeName, numCPUs + r.numCPUs, memorySize + ensureNonNegative(r.memorySize))
  }

  def isEnoughFor(r:ResourceRequest) : Boolean = {
    r.cpu <= numCPUs && r.memorySize.map(_ <= memorySize).getOrElse(true)
  }
}

case class ResourceRequest(nodeName:Option[String], cpu:Int, memorySize:Option[Long])


/**
 * Node manager component
 */
trait NodeManagerComponent {

  type NodeManager <: NodeManagerAPI
  val nodeManager : NodeManager


  trait NodeManagerAPI {
    def clientIsActive(nodeName:String) : Boolean
    def nodes : Seq[Node]
    def getNode(nodeName:String) : Option[Node]
    def addNode(n:Node)
    def removeNode(nodeName:String)
  }
}

/**
 * ResourceManager runs on a master node and manages
 * CPU/memory resources available in nodes
 */
trait ResourceManagerComponent {

  type ResourceManager <: ResourceManagerAPI

  val resourceManager : ResourceManager


}

trait ResourceManagerAPI {
  /**
   * Acquire the resource. This operation blocks until the resource becomes available
   */
  def acquireResource(r:ResourceRequest) : NodeResource
  def addResource(n:Node, r:NodeResource)
  def getNodeRef(nodeName:String) : Option[NodeRef]
  def releaseResource(r:NodeResource)
  def lostResourceOf(nodeName:String)
}
