//--------------------------------------
//
// SilkFlow.scala
// Since: 2013/05/08 2:18 PM
//
//--------------------------------------

package xerial.silk.core

import java.io.File
import reflect.ClassTag
import xerial.core.log.Logger
import xerial.core.io.text.UString
import reflect.macros.Context
import scala.language.experimental.macros

/**
 * Base trait for representing an arrow from a data type to another data type
 * @tparam P previous type
 * @tparam A current type
 */
abstract class SilkFlow[P, A] extends Silk[A] {

  import SilkFlow._

  def self = this

  protected def err = sys.error("N/A")

  //def foreach[U](f: A => U) : Silk[U] = macro mForeach[A, U]
  //def map[B](f: (A) => B) = macro mMap[A, B]
//  def flatMap[B](f: (A) => Silk[B]) = macro mFlatMap[A, B]
//  def filter(p: (A) => Boolean) = err
//  def collect[B](pf: PartialFunction[A, B]) = err
//  def collectFirst[B](pf: PartialFunction[A, B]) = err
//  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B) = err
//  def reduce[A1 >: A](op: (A1, A1) => A1) = err
//  def reduceLeft[B >: A](op: (B, A) => B) = err
//  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1) = err
//  def foldLeft[B](z: B)(op: (B, A) => B) = err
//
//  def scanLeftWith[B, C](z: B)(op: (B, A) => (B, C)): Silk[C] = err
//
//  def size = 0
//  def isSingle = false
//  def isEmpty = false
//  def sum[B >: A](implicit num: Numeric[B]) = Fold(self, num.zero, num.plus)
//  def product[B >: A](implicit num: Numeric[B]) = Fold(self, num.one, num.times)
//  def min[B >: A](implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.lteq(x, y)) x else y)
//  def max[B >: A](implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.gteq(x, y)) x else y)
//  def maxBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.gteq(f(x), f(y))) x else y)
//  def minBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = Reduce(self, (x: A, y: A) => if (cmp.lteq(f(x), f(y))) x else y)
//  //  def mkString(start: String, sep: String, end: String) =
//  //    FlowMap[A, String](self, _.toString).aggregate(new StringBuilder)(
//  //    {
//  //      (b, a) =>
//  //        if (!b.isEmpty)
//  //          b.append(sep)
//  //        b.append(a)
//  //        b
//  //    }, {
//  //      (b1, b2) =>
//  //        if (!b1.isEmpty)
//  //          b1.append(sep)
//  //        b1.append(b2.result())
//  //        b1
//  //    }).mapSingle {
//  //      s =>
//  //        val b = new StringBuilder
//  //        b.append(start)
//  //        b.append(s)
//  //        b.append(end)
//  //        b.result
//  //    }
//
//  def groupBy[K](f: (A) => K) = GroupBy(self, f)
//  def project[B](implicit mapping: ObjectMapping[A, B]) = Project(self, mapping)
//  def join[K, B](other: Silk[B], k1: (A) => K, k2: (B) => K) = Join(self, other, k1, k2)
//  def joinBy[B](other: Silk[B], cond: (A, B) => Boolean) = JoinBy(self, other, cond)
//  def sortBy[K](f: (A) => K)(implicit ord: Ordering[K]) = SortBy(self, f, ord)
//  def sorted[A1 >: A](implicit ord: Ordering[A1]) = Sort(self, ord)
//  def takeSample(proportion: Double) = Sampling(self, proportion)
//  def withFilter(p: (A) => Boolean) = WithFilter(self, p)
//  def zip[B](other: Silk[B]) = Zip(self, other)
//  def zipWithIndex = ZipWithIndex(self)
//
//  def split: Silk[Silk[A]] = Split(self)
//
//  // Type conversion method
//  def toArray[B >: A : ClassTag]: Array[B] = null
//
//  def save[B >: A]: Silk[B] = err // SaveToFile(self)

}



/**
 * Mapping to single data type
 * @tparam From
 * @tparam To
 */
trait SilkFlowSingle[From, To] extends SilkFlow[From, To] with SilkSingle[To] {
  def mapSingle[B](f: To => B): SilkSingle[B] = err // Silk.EmptySingle
  def get: To = throw new UnsupportedOperationException("get")
}


import scala.reflect.runtime.{universe=>ru}



/**
 * @author Taro L. Saito
 */
private[xerial] object SilkFlow {

  import scala.language.existentials

  private def helper[F, B](c:Context)(f:c.Expr[F], op:c.Tree) = {
    import c.universe._
    // TODO resolve local functions
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    val exprGen = c.Expr[Expr[ru.Expr[F]]](t)
    c.Expr[Silk[B]](Apply(Select(op, newTermName("apply")), List(c.prefix.tree, f.tree, exprGen.tree)))
  }


  def mMap[A, B](c:Context)(f:c.Expr[A=>B]) = {
    import c.universe._
    helper[A=>B, B](c)(f, reify{MapFun}.tree)
  }

  def mForeach[A, B](c:Context)(f:c.Expr[A=>B]) = {
    import c.universe._
    helper[A=>B, B](c)(f, reify{Foreach}.tree)
  }
  def mFlatMap[A, B](c:Context)(f:c.Expr[A=>Silk[B]]) =
    helper[A=>Silk[B], B](c)(f, c.universe.reify{FlatMap}.tree)

  def mFilter[A](c:Context)(p:c.Expr[A=>Boolean]) =
    helper[A=>Boolean, A](c)(p, c.universe.reify{Filter}.tree)

  def mFilterNot[A](c:Context)(p:c.Expr[A=>Boolean]) =
    helper[A=>Boolean, A](c)(p, c.universe.reify{FilterNot}.tree)

  // Root nodes
  case class Root(name: String) extends SilkFlow[Nothing, Nothing]
  case class SingleInput[A](e:A) extends SilkFlowSingle[Nothing, A]
  case class RawInput[A](in:Seq[A]) extends SilkFlow[Nothing, A]
  case class FileInput(in:File) extends SilkFlow[Nothing, File] {
    def lines : Silk[UString] = ParseLines(in)
  }


  // Mapping operation nodes
  case class MapFun[A, B](prev:Silk[A], f:A=>B, fExpr:ru.Expr[_]) extends SilkFlow[A, B] {
    override def toString = s"MapFun($prev, f:${f.getClass.getName})"
  }
  case class MapSingle[A, B](prev: Silk[A], f: A => B, fExpr:ru.Expr[_]) extends SilkFlowSingle[A, B]
  case class FlatMap[A, B](prev:Silk[A], f:A=>Silk[B], fExpr:ru.Expr[_]) extends SilkFlow[A, B]
  case class Foreach[A, U](prev: Silk[A], f: A => U, fExpr:ru.Expr[_]) extends SilkFlow[A, U]

  // Filtering operations
  case class Filter[A, B](prev:Silk[A], f:A=>Boolean, fExpr:ru.Expr[_]) extends SilkFlow[A, B]
  case class FilterNot[A, B](prev:Silk[A], f:A=>Boolean, fExpr:ru.Expr[_]) extends SilkFlow[A, B]
  case class Collect[A, B](prev: Silk[A], pf: PartialFunction[A, B], fExpr:ru.Expr[_]) extends SilkFlow[A, B]
  case class CollectFirst[A, B](prev: Silk[A], pf: PartialFunction[A, B], fExpr:ru.Expr[_]) extends SilkFlow[A, B]

  // scan
  case class ScanLeftWith[A, B, C](prev: Silk[A], z: B, op: (B, A) => (B, C)) extends SilkFlow[A, C]

  //

  // Aggregate functions


  // File I/O
  case class ParseLines(in:File) extends SilkFlow[File, UString]
  case class SaveToFile[A](prev: Silk[A]) extends SilkFlowSingle[A, File]




  case class Split[A](prev: Silk[A]) extends SilkFlow[A, Silk[A]]
  case class Head[A](prev: Silk[A]) extends SilkFlowSingle[A, A]



  case class Aggregate[A, B](prev: Silk[A], z: B, seqop: (B, A) => B, combop: (B, B) => B) extends SilkFlowSingle[A, B]
  case class Reduce[A, A1 >: A](prev: Silk[A], op: (A1, A1) => A1) extends SilkFlowSingle[A, A1]
  case class ReduceLeft[A, A1 >: A](prev: Silk[A], op: (A1, A) => A1) extends SilkFlowSingle[A, A1]
  case class Fold[A, A1 >: A](prev: Silk[A], z: A1, op: (A1, A1) => A1) extends SilkFlowSingle[A, A1]
  case class FoldLeft[A, B](prev: Silk[A], z: B, op: (B, A) => B) extends SilkFlowSingle[A, B]
  case class GroupBy[A, K](prev: Silk[A], f: A => K) extends SilkFlow[A, (K, Silk[A])]
  case class Project[A, B](prev: Silk[A], mapping: ObjectMapping[A, B]) extends SilkFlow[A, B]
  case class Join[A, B, K](left: Silk[A], right: Silk[B], k1: (A) => K, k2: (B) => K) extends SilkFlow[(A, B), (K, Silk[(A, B)])]
  case class JoinBy[A, B](left: Silk[A], right: Silk[B], cond: (A, B) => Boolean) extends SilkFlow[(A, B), (A, B)]
  case class SortBy[A, K](prev: Silk[A], f: A => K, ord: Ordering[K]) extends SilkFlow[A, A]
  case class Sort[A, A1 >: A, K](prev: Silk[A], ord: Ordering[A1]) extends SilkFlow[A, A1]
  case class Sampling[A](prev: Silk[A], proportion: Double) extends SilkFlow[A, A]

  case class Zip[A, B](prev: Silk[A], other: Silk[B]) extends SilkFlow[A, (A, B)]
  case class ZipWithIndex[A](prev: Silk[A]) extends SilkFlow[A, (A, Int)]

  case class CommandSeq[A](cmd: ShellCommand, next: Silk[A]) extends SilkFlow[Nothing, A]
  case class Run[A](prev: Silk[A]) extends SilkFlow[A, A]
  case class CommandOutputStream(cmd:ShellCommand) extends SilkFlow[Nothing, String]

  case class CommandResult(cmd:ShellCommand) {
    def file = SaveToFile(cmd)
  }




//  class RootWrap[A](val name: String, in: => Silk[A]) extends SilkFlow[Nothing, A] {
//    val lazyF0 = LazyF0(in)
//    //def eval = in.eval
//  }


  case class WithFilter[A](prev: Silk[A], p: A => Boolean) extends SilkFlow[A, A] { // with SilkMonadicFilter[A] {
    //override def self = Filter(prev, p)
  }



  case class ShellCommand(sc:StringContext, args:Any*) extends SilkFlow[Nothing, CommandResult] with Logger {
    override def toString = s"ShellCommand(${templateString})"
    //def |[A, B](next: A => B) = FlowMap(this, next)

    def &&[A](next: Silk[A]) = CommandSeq(this, next)

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

