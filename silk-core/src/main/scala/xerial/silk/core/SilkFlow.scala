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

  private def helperSingle[F, B](c:Context)(f:c.Expr[F], op:c.Tree) = {
    import c.universe._
    // TODO resolve local functions
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    val exprGen = c.Expr[Expr[ru.Expr[F]]](t)
    c.Expr[SilkSingle[B]](Apply(Select(op, newTermName("apply")), List(c.prefix.tree, f.tree, exprGen.tree)))
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

  def mWithFilter[A](c:Context)(p:c.Expr[A=>Boolean]) =
    helper[A=>Boolean, A](c)(p, c.universe.reify{WithFilter}.tree)

  def mReduce[A, A1](c:Context)(op:c.Expr[(A1,A1)=>A1]) =
    helperSingle[(A1,A1)=>A1, A1](c)(op, c.universe.reify{Reduce}.tree)

  def mReduceLeft[A,B](c:Context)(op:c.Expr[(B,A)=>B]) =
    helperSingle[(B,A)=>B, B](c)(op, c.universe.reify{ReduceLeft}.tree)

  private def helperFold[F, B](c:Context)(z:c.Expr[B], f:c.Expr[F], op:c.Tree) = {
    import c.universe._
    // TODO resolve local functions
    val zt = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(z.tree))
    val zexprGen = c.Expr[Expr[ru.Expr[B]]](zt)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    val exprGen = c.Expr[Expr[ru.Expr[F]]](t)
    c.Expr[SilkSingle[B]](Apply(Select(op, newTermName("apply")), List(c.prefix.tree, z.tree, zexprGen.tree, f.tree, exprGen.tree)))
  }


  def mFold[A, A1](c:Context)(z:c.Expr[A1])(op:c.Expr[(A1,A1)=>A1]) =
    helperFold[(A1,A1)=>A1, A1](c)(z, op, c.universe.reify{Fold}.tree)

  def mFoldLeft[A,B](c:Context)(z:c.Expr[B])(op:c.Expr[(B,A)=>B]) =
    helperFold[(B,A)=>B, B](c)(z, op, c.universe.reify{FoldLeft}.tree)

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

  case class WithFilter[A](prev: Silk[A], p: A => Boolean, fExpr:ru.Expr[_]) extends SilkFlow[A, A]


  // scan
  case class ScanLeftWith[A, B, C](prev: Silk[A], z: B, op: (B, A) => (B, C)) extends SilkFlow[A, C]

  // File I/O
  case class ParseLines(in:File) extends SilkFlow[File, UString]
  case class SaveToFile[A](prev: Silk[A]) extends SilkFlowSingle[A, File]

  // Split & Merge
  case class Split[A](prev: Silk[A]) extends SilkFlow[A, Silk[A]]
  case class Concat[A, B](prev:Silk[A], cv:A=>Silk[B]) extends SilkFlow[A, B]
  case class Head[A](prev: Silk[A]) extends SilkFlowSingle[A, A]

  // Aggregate functions
  case class NumericReduce[A, A1 >: A](prev: Silk[A], op: (A1, A1) => A1) extends SilkFlowSingle[A, A1]
  case class NumericFold[A, A1 >: A](prev: Silk[A], z: A1, op: (A1, A1) => A1) extends SilkFlowSingle[A, A1]


  case class MkString[A](in:Silk[A], start:String, sep:String, end:String) extends SilkFlowSingle[A, String]


  case class Aggregate[A, B](prev: Silk[A], z: B, seqop: (B, A) => B, combop: (B, B) => B) extends SilkFlowSingle[A, B]
  case class Reduce[A, A1 >: A](prev: Silk[A], op: (A1, A1) => A1, fExpr:ru.Expr[_]) extends SilkFlowSingle[A, A1]
  case class ReduceLeft[A, A1 >: A](prev: Silk[A], op: (A1, A) => A1, fExpr:ru.Expr[_]) extends SilkFlowSingle[A, A1]
  case class Fold[A, A1 >: A](prev: Silk[A], z: A1, zExpr:ru.Expr[_], op: (A1, A1) => A1, fExpr:ru.Expr[_]) extends SilkFlowSingle[A, A1]
  case class FoldLeft[A, B](prev: Silk[A], z: B, zExpr:ru.Expr[_], op: (B, A) => B, fExpr:ru.Expr[_]) extends SilkFlowSingle[A, B]


  def mGroupBy[A, K](c:Context)(f:c.Expr[A=>K]) = {
    helper[A=>K, (K, Silk[A])](c)(f, c.universe.reify{GroupBy}.tree)
  }
  case class GroupBy[A, K](prev: Silk[A], f: A => K, fExpr:ru.Expr[_]) extends SilkFlow[A, (K, Silk[A])]

  case class Project[A, B](prev: Silk[A], mapping: ObjectMapping[A, B]) extends SilkFlow[A, B]
  case class Join[A, B, K](left: Silk[A], right: Silk[B], k1: (A) => K, k2: (B) => K) extends SilkFlow[(A, B), (K, Silk[(A, B)])]
  case class JoinBy[A, B](left: Silk[A], right: Silk[B], cond: (A, B) => Boolean) extends SilkFlow[(A, B), (A, B)]


  // Sorting
  case class SortBy[A, K](prev: Silk[A], f: A => K, ord: Ordering[K]) extends SilkFlow[A, A]
  case class Sort[A, A1 >: A](prev: Silk[A], ord: Ordering[A1]) extends SilkFlow[A, A1]
  case class Sampling[A](prev: Silk[A], proportion: Double) extends SilkFlow[A, A]

  // Zip
  case class Zip[A, B](prev: Silk[A], other: Silk[B]) extends SilkFlow[A, (A, B)]
  case class ZipWithIndex[A](prev: Silk[A]) extends SilkFlow[A, (A, Int)]


  // Command execution
  case class CommandSeq[A](cmd: ShellCommand, next: Silk[A]) extends SilkFlow[Nothing, A]
  case class Run[A](prev: Silk[A]) extends SilkFlow[A, A]
  case class CommandOutputStream(cmd:ShellCommand) extends SilkFlow[Nothing, String]

  case class CommandResult(cmd:ShellCommand) {
    def file = SaveToFile(cmd)
  }

  case class ConvertToSeq[A, B >: A](prev:Silk[A]) extends SilkFlowSingle[A, Seq[B]]
  case class ConvertToArray[A, B >: A](prev:Silk[A]) extends SilkFlowSingle[A, Array[B]]


//  class RootWrap[A](val name: String, in: => Silk[A]) extends SilkFlow[Nothing, A] {
//    val lazyF0 = LazyF0(in)
//    //def eval = in.eval
//  }





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

