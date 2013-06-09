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
import xerial.core.log.Logger
import scala.collection.GenTraversableOnce
import xerial.silk.mini.FlatMapOp
import xerial.silk.mini.MapOp
import xerial.silk.mini.RawSeq
import java.util.UUID

/**
 * @author Taro L. Saito
 */
trait SilkFramework {

  type Silk[V] = SilkMini[V]
  //type SilkSingle[V]
  type Result[V]
  //type Config

  /**
   * Run the given Silk operation and return the result
   * @param silk
   * @return
   */
  def run[A](silk:Silk[A]) : Result[A]

  //def run[A](silk:SilkSingle[A]) : A
}


trait SliceEvaluator extends SilkFramework {
  type Slice[V] <: SliceAPI[V]

  trait SliceAPI[A] {
    def index: Int
    def data: Seq[A]
  }

  def getSlices(v:Silk[_]) : Seq[Slice[_]]
}

trait SliceStorageComponent { self: SliceEvaluator =>

  type Future[V]
  val sliceStorage : SliceStorage

  trait SliceStorage {
    def get(op:Silk[_], index:Int) : Future[Slice[_]]
    def put(op:Silk[_], index:Int, slice:Slice[_]) : Unit
    def contains(op:Silk[_], index:Int) : Boolean
  }

}

trait InMemorySliceStorage extends SliceStorageComponent { self: SliceEvaluator =>

  type Future[V] = SilkFuture[V]

  val sliceStorage = new SliceStorage with Guard {
    private val table = collection.mutable.Map[(UUID, Int), Slice[_]]()
    private val futureToResolve = collection.mutable.Map[(UUID, Int), SilkFuture[Slice[_]]]()

    def get(op: Silk[_], index: Int) : Future[Slice[_]] = guard {
      val key = (op.uuid, index)
        if (futureToResolve.contains(key)) {
          futureToResolve(key)
        }
        else {
          val f = new SilkFutureMultiThread[Slice[_]]
          if (table.contains(key)) {
            f.set(table(key))
          }
          else
            futureToResolve += key -> f
          f
        }
    }

    def put(op: Silk[_], index: Int, slice: Slice[_]) {
      guard {
        val key = (op.uuid, index)
        if (!table.contains(key)) {
          table += key -> slice
        }
        if (futureToResolve.contains(key)) {
          futureToResolve(key).set(slice)
          futureToResolve -= key
        }
      }
    }

    def contains(op: Silk[_], index:Int) = guard {
      val key = (op.uuid, index)
      table.contains(key)
    }
  }


}


trait InMemorySliceEvaluator extends InMemoryRunner with SliceEvaluator with Logger {
  type Slice[V] = RawSlice[V]
  case class RawSlice[A](index:Int, data:Result[A]) extends SliceAPI[A]

  def evalRecursively(v:Any) : Seq[Slice[_]] = {
    v match {
      case silk:Silk[_] => getSlices(silk)
      case seq:Seq[_] => Seq(RawSlice(0, seq))
      case e => Seq(RawSlice(0, Seq(e)))
    }
  }

  private def flattenSlices(in: Seq[Seq[Slice[_]]]): Seq[Slice[_]] = {
    var counter = 0
    val result = for (ss <- in; s <- ss) yield {
      val r = RawSlice(counter, s.data)
      counter += 1
      r
    }
    result
  }

  def getSlices(v:Silk[_]) : Seq[Slice[_]] = {
    v match {
      case MapOp(fref, in, f, fe) =>
        val slices = for(slc <- getSlices(in)) yield
          RawSlice(slc.index, slc.data.map(e => fwrap(f)(e)))
        slices
      case FlatMapOp(fref, in, f, fe) =>
        val slices = for(slc <- getSlices(in)) yield
          RawSlice(slc.index, slc.data.flatMap(e => evalRecursively(fwrap(f)(e))))
        slices
      case FilterOp(fref, in, f, fe) =>
        val slices = for(slc <- getSlices(in)) yield
          RawSlice(slc.index, slc.data.filter(filterWrap(f)))
        slices
      case ReduceOp(fref, in, f, fe) =>
        val rf = rwrap(f)
        val reduced = for(slc <- getSlices(in)) yield
          slc.data.reduce(rf)
        Seq(RawSlice(0, Seq(reduced.reduce(rf))))
      case RawSeq(fc, in) => Seq(RawSlice(0, in))
      case other =>
        warn(s"unknown op: $other")
        Seq.empty
    }
  }

  override def run[A](silk:Silk[A]) : Result[A] = {
    getSlices(silk).flatMap(_.data).asInstanceOf[Result[A]]
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
