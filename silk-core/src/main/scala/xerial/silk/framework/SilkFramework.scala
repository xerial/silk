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


/**
 * SilkFramework is an abstraction of input and result data types of Silk operations.
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


  /**
   * Run the given Silk operation and return the result
   * @param silk
   * @return
   */
  def run[A](silk: Silk[A]): Result[A]
}



/**
 * Configuration
 */
trait ConfigComponent {
  type Config

  /**
   * Get the current configuration
   * @return
   */
  def config: Config

}

trait EvaluatorComponent extends SilkFramework {
  type Slice[V] <: SliceAPI[V]
  type Evaluator <: EvaluatorAPI

  trait SliceAPI[A] {
    def index: Int
    def data: Seq[A]
  }

  trait EvaluatorAPI {
    def getSlices(op:Silk[_]) : Seq[Slice[_]]
    def eval[A](op: Silk[A]): Seq[Slice[_]]
  }

  def run[A](silk: Silk[A]): Result[A]

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
