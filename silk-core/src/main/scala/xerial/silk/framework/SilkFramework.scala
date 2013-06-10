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
import xerial.core.log.{LogLevel, LoggerFactory, Logger}
import scala.collection.GenTraversableOnce
import xerial.silk.mini.FlatMapOp
import xerial.silk.mini.MapOp
import xerial.silk.mini.RawSeq
import java.util.UUID
import scala.reflect.macros.Context

/**
 * SilkFramework is an abstraction of input and result data types of Silk operations.
 *
 * @author Taro L. Saito
 */
trait SilkFramework {

  /**
   * Silk is an abstraction of data processing operation. By calling run method, its result can be obtained
   * @tparam A
   */
  type Silk[A] = SilkMini[A]
  type Result[A] = Seq[A]

  def trace(s: => String)
  def debug(s: => String)
  def info(s: => String)
  def warn(s: => String)
  def error(s: => String)
  def fatal(s: => String)

  //type Config

  /**
   * Run the given Silk operation and return the result
   * @param silk
   * @return
   */
  def run[A](silk: Silk[A]): Result[A]
}


trait DefaultLogger extends SilkFramework {

  private[this] val logger = LoggerFactory("Silk")

  def trace(s: => String) { logger.trace(s) }
  def debug(s: => String) { logger.debug(s) }
  def info(s: => String) { logger.info(s) }
  def warn(s: => String) { logger.warn(s) }
  def error(s: => String) { logger.error(s) }
  def fatal(s: => String) { logger.fatal(s) }

}


/**
 *
 *
 * @author Taro L. Saito
 */
trait SliceEvaluator extends SilkFramework {
  type Slice[V] <: SliceAPI[V]

  trait SliceAPI[A] {
    def index: Int
    def data: Seq[A]
  }

  def getSlices(v: Silk[_]): Seq[Slice[_]]
}


/**
 * @author Taro L. Saito
 */
trait SliceStorageComponent extends SliceEvaluator {

  type Future[V]
  val sliceStorage: SliceStorage

  trait SliceStorage {
    def get(op: Silk[_], index: Int): Future[Slice[_]]
    def put(op: Silk[_], index: Int, slice: Slice[_]): Unit
    def contains(op: Silk[_], index: Int): Boolean
  }

}


/**
 * Managing running state of
 */
trait StageManagerComponent extends SliceStorageComponent with SliceEvaluator {

  val stageManager: StageManager

  trait StageManager {
    /**
     * Call this method when an evaluation of the given Silk expression has started
     * @param op
     * @return Future of the all slices
     */
    def startStage(op: Silk[_]): Future[Seq[Slice[_]]]

    /**
     * Returns true if the evaluation of the Silk expression has finished
     * @param op
     * @return
     */
    def isFinished(op: Silk[_]): Boolean
  }

}


//
//
//
//
//
//trait LocalFramework extends SilkFramework {
//
//  type Config <: LocalConfig
//  type Node <: LocalNode
//
//  trait LocalConfig {
//
//
//  }
//
//  case class LocalNode(name:String)
//
//}
//
//
//trait DistributedFramework extends SilkFramework {
//  type Config <: DistributedConfig
//  type Node <: RemoteNode
//  type NodeManager <: NodeManagerAPI
//
//
//
//  trait DistributedConfig {
//
//
//  }
//
//  trait NodeManagerAPI {
//    def addNode(node:Node)
//
//  }
//
//  case class RemoteNode(name:String, address:String)
//}
//
//trait LifeCycle {
//
//  def start : Unit
//  def stop : Unit
//}
//
//
//trait ZooKeeperService extends LifeCycle {
//
//  type Config <: ZooKeeperConfig
//  type ZookeeperClient
//
//  def client : ZookeeperClient
//
//  case class ZooKeeperConfig(
//    basePath : String = "/silk",
//    clientPort: Int = 8980,
//    quorumPort: Int = 8981,
//    leaderElectionPort: Int = 8982,
//    tickTime: Int = 2000,
//    initLimit: Int = 10,
//    syncLimit: Int = 5,
//    clientConnectionMaxRetry : Int = 5,
//    clientConnectionTickTime : Int = 500,
//    clientSessionTimeout : Int = 60 * 1000,
//    clientConnectionTimeout : Int = 3 * 1000)
//
//
//
//}
//
//
//
//
