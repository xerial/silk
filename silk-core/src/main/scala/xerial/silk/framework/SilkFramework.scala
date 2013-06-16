//--------------------------------------
//
// SilkFramework.scala
// Since: 2013/06/09 11:44
//
//--------------------------------------

package xerial.silk.framework

import scala.language.higherKinds
import scala.language.experimental.macros
import scala.reflect.ClassTag
import xerial.silk.mini._
import xerial.silk.{SilkException, SilkError}
import xerial.core.log.Logger
import java.util.UUID
import java.net.InetAddress


/**
 * SilkFramework contains the abstraction of input and result data types of Silk operations.
 *
 * @author Taro L. Saito
 */
trait SilkFramework extends LoggingComponent {

  /**
   * Silk is an abstraction of data processing operation. By calling run method, its result can be obtained
   * @tparam A
   */
  type Silk[A] = SilkMini[A]
  type Result[A] = Seq[A]
  type Future[A] = SilkFuture[A]

  /**
   * Future reference to a result
   * @tparam A
   */
  type ResultRef[A] = Future[Result[A]]




    /**
   * Helper functions
   */
  protected object helper {
    def fwrap[A,B](f:A=>B) = f.asInstanceOf[Any=>Any]
    def filterWrap[A](f:A=>Boolean) = f.asInstanceOf[Any=>Boolean]
    def rwrap[P, Q, R](f: (P, Q) => R) = f.asInstanceOf[(Any, Any) => Any]

  }
}



trait LifeCycle {

  def startup {}
  def teardown {}
}


/**
 * A component for manipulating program trees
 */
trait ProgramTreeComponent {
  self:SilkFramework =>

  /**
   * Enclose the tree traversal functions within the object since they should be accessible within SilkFramework only
   */
  protected object ProgramTree {

    def graphOf[A](op:Silk[A]) = SilkMini.createCallGraph(op)

    /**
     * Find a part of the silk tree
     * @param silk
     * @param targetName
     * @tparam A
     * @return
     */
    def collectTarget[A](silk:Silk[A], targetName:String) : Seq[Silk[_]] = {
      info(s"Find target $targetName from $silk")
      val g = graphOf(silk)
      debug(s"call graph: $g")

      g.nodes.collect{
        case op if op.fref.localValName.map(_ == targetName) getOrElse false =>
          op
      }
    }

    def findTarget[A](silk:Silk[A], targetName:String) : Option[Silk[_]] = {
      val matchingOps = collectTarget(silk, targetName)
      matchingOps.size match {
        case v if v > 1 => throw new IllegalArgumentException(s"more than one target is found for $targetName")
        case other => matchingOps.headOption
      }
    }

    def descendantsOf[A](silk:Silk[A]) : Set[Silk[_]] = {
      val g = graphOf(silk)
      g.descendantsOf(silk)
    }

    def descendantsOf[A](silk:Silk[A], targetName:String) : Set[Silk[_]] = {
      val g = graphOf(silk)
      findTarget(silk, targetName) match {
        case Some(x) => g.descendantsOf(x)
        case None => Set.empty
      }
    }
  }

}





trait SliceComponent {

  self:SilkFramework =>

  /**
   * Slice might be a future
   * @tparam A
   */
  type Slice[A] <: SliceAPI[A]

  trait SliceAPI[A] {
    def index: Int
    def data: Seq[A]
  }


}






/**
 * @author Taro L. Saito
 */
trait SliceStorageComponent extends SliceComponent {
  self: SilkFramework =>

  val sliceStorage: SliceStorageAPI

  trait SliceStorageAPI {
    def get(op: Silk[_], index: Int): Future[Slice[_]]
    def put(op: Silk[_], index: Int, slice: Slice[_]): Unit
    def contains(op: Silk[_], index: Int): Boolean
  }
}


/**
 * Managing running state of
 */
trait StageManagerComponent extends SilkFramework {

  type StageManager <: StageManagerAPI
  val stageManager: StageManagerAPI

  trait StageManagerAPI {
    /**
     * Call this method when an evaluation of the given Silk expression has started
     * @param op
     * @return Future of the all slices
     */
    def startStage[A](op:Silk[A])

    def finishStage[A](op:Silk[A])

    def abortStage[A](op:Silk[A])

    /**
     * Returns true if the evaluation of the Silk expression has finished
     * @param op
     * @return
     */
    def isFinished[A](op: Silk[A]): Boolean
  }

}




trait DistributedFramework
  extends SilkFramework {

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
                resource:NodeResource) {
  def host = Host(name, address)
  def toRef = NodeRef(name, address, clientPort)
}

case class NodeRef(name:String, address:String, clientPort:Int) {
  def host = Host(name, address)
}

object Host {
  def apply(s:String) : Host = {
    val lh = InetAddress.getByName(s)
    Host(s, lh.getHostAddress)
  }
}

case class Host(name: String, address: String) {
  def prefix = name.split("\\.")(0)
}



case class NodeResource(nodeName:String, numCPUs:Int, memorySize:Long) {

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
 * ClusterManager
 */
trait ClusterManagerComponent {

  type NodeManager <: NodeManagerAPI
  val nodeManager : NodeManager

  def clientIsActive(nodeName:String) : Boolean

  trait NodeManagerAPI {
    def nodes : Seq[Node]
    def addNode(n:Node)
    def removeNode(nodeName:String)
  }
}

/**
 * ResourceManager runs on a master node
 */
trait ResourceManagerComponent {

  type ResourceManager <: ResourceManagerAPI

  val resourceManager : ResourceManager

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

}
