//--------------------------------------
//
// SilkFlow.scala
// Since: 2013/05/08 2:18 PM
//
//--------------------------------------

package xerial.silk.flow

import java.io.File
import reflect.ClassTag
import xerial.core.log.Logger


/**
 * Base trait for representing an arrow from a data type to another data type
 * @tparam From
 * @tparam To
 */
trait SilkFlow[From, To] extends Silk[To]

/**
 * SilkFlow with its implementation
 * @tparam From
 * @tparam To
 */
trait SilkFlowBase[From, To] extends SilkFlowLike[From, To] with SilkFlow[From, To]


/**
 * Mapping to single data type
 * @tparam From
 * @tparam To
 */
trait SilkFlowSingle[From, To] extends SilkFlowLike[From, To] with SilkSingle[To] with SilkFlow[From, To] {
  import SilkFlow._
  // TODO impl
  def mapSingle[B](f: To => B): SilkSingle[B] = Silk.EmptySingle
  override def map[B](f: To => B): SilkSingle[B] = FlowMapSingle(this, f)

  def get: To = throw new UnsupportedOperationException("get")
}


trait SilkFilter[A] extends SilkMonadicFilter[A] {
  // TODO impl
  def iterator = null
  def newBuilder[T] = null
}


trait SilkFlowLike[P, A] { this : SilkFlow[P, A] =>

  import SilkFlow._

  def self = this

  def file = SaveToFile(this)

  // TODO impl
  def iterator = null
  // TODO impl
  def newBuilder[T] = null

  def head = Head(this)

  def foreach[U](f: (A) => U) = Foreach(self, f)
  def map[B](f: (A) => B): Silk[B] = FlowMap(self, f)
  //def flatMap[B](f: (A) => GenTraversableOnce[B]) = FlatMap(this, f)
  def flatMap[B](f: (A) => Silk[B]) = FlatMap(self, f)
  def filter(p: (A) => Boolean) = Filter(self, p)
  def collect[B](pf: PartialFunction[A, B]) = Collect(self, pf)
  def collectFirst[B](pf: PartialFunction[A, B]) = CollectFirst(self, pf)
  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B) = Aggregate(self, z, seqop, combop)
  def reduce[A1 >: A](op: (A1, A1) => A1) = Reduce(self, op)
  def reduceLeft[B >: A](op: (B, A) => B) = ReduceLeft(self, op)
  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1) = Fold(self, z, op)
  def foldLeft[B](z: B)(op: (B, A) => B) = FoldLeft(self, z, op)

  def scanLeftWith[B, C](z: B)(op: (B, A) => (B, C)): Silk[C] = ScanLeftWith(self, z, op)

  def size = 0
  def isSingle = false
  def isEmpty = false
  def sum[B >: A](implicit num: Numeric[B]) = Fold(self, num.zero, num.plus)
  def product[B >: A](implicit num: Numeric[B]) = Fold(self, num.one, num.times)
  def min[B >: A](implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.lteq(x, y)) x else y)
  def max[B >: A](implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.gteq(x, y)) x else y)
  def maxBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.gteq(f(x), f(y))) x else y)
  def minBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.lteq(f(x), f(y))) x else y)
  def mkString(start: String, sep: String, end: String) =
    FlowMap[A, String](self, _.toString).aggregate(new StringBuilder)(
    {
      (b, a) =>
        if (!b.isEmpty)
          b.append(sep)
        b.append(a)
        b
    }, {
      (b1, b2) =>
        if (!b1.isEmpty)
          b1.append(sep)
        b1.append(b2.result())
        b1
    }).mapSingle {
      s =>
        val b = new StringBuilder
        b.append(start)
        b.append(s)
        b.append(end)
        b.result
    }

  def groupBy[K](f: (A) => K) = GroupBy(self, f)
  def project[B](implicit mapping: ObjectMapping[A, B]) = Project(self, mapping)
  def join[K, B](other: Silk[B], k1: (A) => K, k2: (B) => K) = Join(self, other, k1, k2)
  def joinBy[B](other: Silk[B], cond: (A, B) => Boolean) = JoinBy(self, other, cond)
  def sortBy[K](f: (A) => K)(implicit ord: Ordering[K]) = SortBy(self, f, ord)
  def sorted[A1 >: A](implicit ord: Ordering[A1]) = Sort(self, ord)
  def takeSample(proportion: Double) = Sampling(self, proportion)
  def withFilter(p: (A) => Boolean) = WithFilter(self, p)
  def zip[B](other: Silk[B]) = Zip(self, other)
  def zipWithIndex = ZipWithIndex(self)

  def split: Silk[Silk[A]] = Split(self)

  def concat[B](implicit asSilk: A => Silk[B]): Silk[B] = Concat(self, asSilk)

  // Type conversion method
  def toArray[B >: A : ClassTag]: Array[B] = null

  def save[B >: A]: Silk[B] = Save(self)

}


/**
 * @author Taro L. Saito
 */
object SilkFlow {


  case class Root(name: String) extends SilkFlowBase[Nothing, Nothing]
  class RootWrap[A](val name: String, in: => Silk[A]) extends SilkFlowBase[Nothing, A] {
    val lazyF0 = LazyF0(in)
    //def eval = in.eval
  }


  case class WithFilter[A](prev: Silk[A], p: A => Boolean) extends SilkFilter[A] with SilkFlow[A, A] with SilkFlowLike[A, A] {
    override def self = Filter(prev, p)
  }


  case class Save[A](prev: Silk[A]) extends SilkFlowBase[A, A]
  case class ScanLeftWith[A, B, C](prev: Silk[A], z: B, op: (B, A) => (B, C)) extends SilkFlowBase[A, C]
  case class Concat[A, B](prev: Silk[A], asSilk: A => Silk[B]) extends SilkFlowBase[A, B]
  case class Split[A](prev: Silk[A]) extends SilkFlowBase[A, Silk[A]]
  case class Head[A](prev: Silk[A]) extends SilkFlowSingle[A, A]
  case class Foreach[A, U](prev: Silk[A], f: A => U) extends SilkFlowBase[A, U]
  case class FlowMap[A, B](prev: Silk[A], f: A => B) extends SilkFlowBase[A, B] {
    override def toString = s"Map($prev, f:${f.getClass.getName})"
  }
  case class FlowMapSingle[A, B](prev: Silk[A], f: A => B) extends SilkFlowSingle[A, B]
  case class FlatMap[A, B](prev: Silk[A], f: A => Silk[B]) extends SilkFlowBase[A, B]
  case class Filter[A](prev: Silk[A], f: A => Boolean) extends SilkFlowBase[A, A]
  case class Collect[A, B](prev: Silk[A], pf: PartialFunction[A, B]) extends SilkFlowBase[A, B]
  case class CollectFirst[A, B](prev: Silk[A], pf: PartialFunction[A, B]) extends SilkFlowSingle[A, Option[B]]
  case class Aggregate[A, B](prev: Silk[A], z: B, seqop: (B, A) => B, combop: (B, B) => B) extends SilkFlowSingle[A, B]
  case class Reduce[A, A1 >: A](prev: Silk[A], op: (A1, A1) => A1) extends SilkFlowSingle[A, A1]
  case class ReduceLeft[A, A1 >: A](prev: Silk[A], op: (A1, A) => A1) extends SilkFlowSingle[A, A1]
  case class Fold[A, A1 >: A](prev: Silk[A], z: A1, op: (A1, A1) => A1) extends SilkFlowSingle[A, A1]
  case class FoldLeft[A, B](prev: Silk[A], z: B, op: (B, A) => B) extends SilkFlowSingle[A, B]
  case class GroupBy[A, K](prev: Silk[A], f: A => K) extends SilkFlowBase[A, (K, Silk[A])]
  case class Project[A, B](prev: Silk[A], mapping: ObjectMapping[A, B]) extends SilkFlowBase[A, B]
  case class Join[A, B, K](left: Silk[A], right: Silk[B], k1: (A) => K, k2: (B) => K) extends SilkFlowBase[(A, B), (K, Silk[(A, B)])]
  case class JoinBy[A, B](left: Silk[A], right: Silk[B], cond: (A, B) => Boolean) extends SilkFlowBase[(A, B), (A, B)]
  case class SortBy[A, K](prev: Silk[A], f: A => K, ord: Ordering[K]) extends SilkFlowBase[A, A]
  case class Sort[A, A1 >: A, K](prev: Silk[A], ord: Ordering[A1]) extends SilkFlowBase[A, A1]
  case class Sampling[A](prev: Silk[A], proportion: Double) extends SilkFlowBase[A, A]

  case class Zip[A, B](prev: Silk[A], other: Silk[B]) extends SilkFlowBase[A, (A, B)]
  case class ZipWithIndex[A](prev: Silk[A]) extends SilkFlowBase[A, (A, Int)]

  case class CommandSeq[A, B](cmd: ShellCommand, next: SilkFlow[A, B]) extends SilkFlowBase[A, B]
  case class Run[A](prev: Silk[A]) extends SilkFlowBase[A, A]
  case class CommandOutputStream(cmd:ShellCommand) extends SilkFlowBase[Nothing, String]

  case class CommandResult(cmd:ShellCommand) {
    def file = SaveToFile(cmd)
  }

  case class SaveToFile[A](prev:Silk[A]) extends SilkFlowBase[A, File]


  case class ShellCommand(sc:StringContext, args:Any*) extends SilkFlowBase[Nothing, CommandResult] with Logger {
    override def toString = s"ShellCommand(${templateString})"
    //def |[A, B](next: A => B) = FlowMap(this, next)

    def cmdString = {
      trace(s"parts length: ${sc.parts.length}, argc: ${args.length}")
      val b = new StringBuilder
      val zip = sc.parts.zipAll(args, "", null)
      for((f, v) <- zip) {
        b.append(f)
        if(v != null)
          b.append(v)
      }
      trace(s"zipped ${zip.mkString(", ")}")
      b.result()
    }

    //def as(next: SilkFile[CommandResult]) = CommandSeq(this, next)
    def lines : CommandOutputStream =  CommandOutputStream(this)

    def argSize = args.size
    def arg(i:Int) : Any = args(i)
    def argSeq : Seq[Any] = args

    def templateString = {
      val b = new StringBuilder
      val zip = sc.parts.zipAll(args, "", null)
      for((f, v) <- zip) {
        b.append(f)
        if(v != null)
          b.append("${}")
      }
      trace(s"zipped ${zip.mkString(", ")}")
      b.result()
    }
  }


}

