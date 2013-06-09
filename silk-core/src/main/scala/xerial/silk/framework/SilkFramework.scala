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

/**
 * @author Taro L. Saito
 */
trait SilkFramework {

  type Silk[V]
  type Result[V]
  //type Config

  /**
   * Run the given Silk operation and return the result
   * @param silk
   * @return
   */
  def run[A](silk:Silk[A]) : Result[A]
}


trait InMemoryRunner extends SilkFramework with Logger {
  type Silk[V] = SilkMini[V]
  type Result[V] = Seq[V]

  def newSilk[A](in: Result[A])(implicit ev: ClassTag[A]): Silk[A] = macro SilkMini.newSilkImpl[A]

  private def fwrap[A,B](f:A=>B) = f.asInstanceOf[Any=>Any]

  private def eval(v:Any) : Any = {
    v match {
      case s:Silk[_] => run(s)
      case other => other
    }
  }

  private def evalSeq(seq:Any) : Seq[Any] = {
    seq match {
      case s:Silk[_] => run(s)
      case other => other.asInstanceOf[Seq[Any]]
    }
  }

  def run[A](silk:Silk[A]) : Result[A] = {
    implicit class Cast(v:Any) {
      def cast : Result[A] = v.asInstanceOf[Result[A]]
    }

    silk match {
      case RawSeq(fref, in) => in.cast
      case MapOp(fref, in, f, fe) =>
        run(in).map(e => eval(fwrap(f)(e))).cast
      case FlatMapOp(fref, in, f, fe) =>
        run(in).flatMap(e => evalSeq(fwrap(f)(e))).cast
      case FilterOp(fref, in, f, fe) =>
        run(in).filter(f).cast
      case ReduceOp(fref, in, f, fe) =>
        Seq(run(in).reduce(f)).cast
      case other =>
        warn(s"unknown silk type: $silk")
        Seq.empty
    }

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
