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
import xerial.silk.CmdBuilder
import javax.management.remote.rmi._RMIConnection_Stub
import xerial.lens.{Parameter, ObjectSchema}
import scala.collection.GenTraversableOnce


/**
 * Base trait for representing an arrow from a data type to another data type
 * @tparam P previous type
 * @tparam A current type
 */
trait SilkFlow[+P, +A] extends Silk[A] {

  import SilkFlow._
  import scala.reflect.runtime.{universe=>ru}

  def self = this

  protected def err = sys.error("N/A")

  def toSilkString : String = {
    val s = new StringBuilder
    mkSilkText(0, s)
    s.result.trim
  }

//  override def toString = {
//    def str(filter:Parameter => Boolean) : String = {
//      val sc = ObjectSchema(self.getClass)
//      val params = for(p <- sc.constructor.params if filter(p)) yield {
//        p.get(self).toString
//      }
//      s"${self.getClass.getSimpleName}(${params.mkString(", ")})"
//    }
//
//    self match {
//      case w:SilkFlow.WithInput[_] =>
//        str({_.name != "prev"})
//      case f:SilkFlow[_, _] =>
//        str({ p:Parameter => true})
//      case _ => super.toString
//    }
//
//  }

  protected def indent(n:Int) : String = {
    val b = new StringBuilder(n)
    for(i <- 0 until n) b.append(' ')
    b.result
  }

  def mkSilkText(level:Int, s:StringBuilder)  {
    val sc = ObjectSchema.apply(getClass)
    val idt = indent(level)
    s.append(s"${idt}-${sc.name}\n")
    for(p <- sc.constructor.params) {
      val v = p.get(this)
      val idt = indent(level+1)
      v match {
        case f:SilkFlow[_, _] =>
          s.append(s"$idt-${p.name}\n")
          f.mkSilkText(level+2, s)
        case e:ru.Expr[_] =>
          s.append(s"$idt-${p.name}: ${ru.show(e)}\n")
        case _ =>
          s.append(s"$idt-${p.name}: $v\n")
      }
    }
  }

}



/**
 * Mapping to single data type
 * @tparam From
 * @tparam To
 */
trait SilkFlowSingle[From, To] extends SilkFlow[From, To] with SilkSingle[To] {
  override def mapSingle[B](f: To => B): SilkSingle[B] = macro SilkFlow.mMapSingle[To, B]
  def get(implicit ex:SilkExecutor): To = ex.evalSingle(this)
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

  def mMapSingle[A, B](c:Context)(f:c.Expr[A=>B]) = {
    import c.universe._
    helperSingle[A=>B, B](c)(f, reify{MapSingle}.tree)
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
  case class RawInputSingle[A](e:A) extends SilkFlowSingle[Nothing, A] {
    override def isRaw = true
  }
  case class RawInput[A](in:Seq[A]) extends SilkFlow[Nothing, A] {
    override def isRaw = true
  }
  case class FileInput(in:File) extends SilkFlow[Nothing, File] {
    def lines : Silk[UString] = ParseLines(in)
  }


  // Mapping operation nodes
  //abstract class SilkFlowF1[A, B](val f:A=>B, val fExpr:ru.Expr[_]) extends SilkFlow[A, B]

  trait WithInput[A]{
    val prev:Silk[A]
    def copyWithoutInput : Silk[A] = {
      val sc = ObjectSchema(this.getClass)
      val params = for(p <- sc.constructor.params) yield {
        if(p.name == "prev")
          Silk.Empty
        else
          p.get(this)
      }
      sc.constructor.newInstance(params.map(_.asInstanceOf[AnyRef]).toArray).asInstanceOf[Silk[A]]
    }
  }

  // Evaluation
  case class GetSingle[A](prev:SilkSingle[A]) extends SilkFlowSingle[A, A]

  // Map
  case class MapFun[A, B](prev:Silk[A], f:A=>B, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, B]
  case class MapSingle[A, B](prev: Silk[A], f: A => B, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlowSingle[A, B]
  case class FlatMap[A, B](prev:Silk[A], f:A=>Silk[B], fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, B]
  case class Foreach[A, U](prev: Silk[A], f: A => U, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, U]

  // Filtering operations
  case class Filter[A, B](prev:Silk[A], f:A=>Boolean, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, B]
  case class FilterNot[A, B](prev:Silk[A], f:A=>Boolean, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, B]
  case class Collect[A, B](prev: Silk[A], pf: PartialFunction[A, B], fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, B]
  case class CollectFirst[A, B](prev: Silk[A], pf: PartialFunction[A, B], fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, B]
  case class WithFilter[A](prev: Silk[A], p: A => Boolean, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, A]


  // scan
  case class Count[A](prev:Silk[A]) extends WithInput[A] with SilkFlowSingle[A, Long]
  case class ScanLeftWith[A, B, C](prev: Silk[A], z: B, op: (B, A) => (B, C)) extends WithInput[A] with SilkFlow[A, C]

  // File I/O
  case class ParseLines(in:File) extends SilkFlow[File, UString]
  case class SaveToFile[A](prev: Silk[A]) extends WithInput[A] with SilkFlowSingle[A, File]
  case class SaveAs[A](prev:Silk[A], name:String) extends WithInput[A] with SilkFlowSingle[A, File]

  // Split & Merge
  case class Split[A](prev: Silk[A]) extends WithInput[A] with SilkFlow[A, Silk[A]]
  case class Concat[A, B](prev:Silk[A], cv:A=>Silk[B]) extends WithInput[A] with SilkFlow[A, B]
  case class Head[A](prev: Silk[A]) extends WithInput[A] with SilkFlowSingle[A, A]

  // Aggregate functions
  case class NumericReduce[A](prev: Silk[A], op: (A, A) => A) extends WithInput[A] with SilkFlowSingle[A, A]
  case class NumericFold[A](prev: Silk[A], z: A, op: (A, A) => A) extends WithInput[A] with SilkFlowSingle[A, A]


  case class MkString[A](prev:Silk[A], start:String, sep:String, end:String) extends WithInput[A] with SilkFlowSingle[A, String]


  case class Aggregate[A, B](prev: Silk[A], z: B, seqop: (B, A) => B, combop: (B, B) => B) extends WithInput[A] with SilkFlowSingle[A, B]
  case class Reduce[A, A1 >: A](prev: Silk[A], op: (A1, A1) => A1, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlowSingle[A, A1]
  case class ReduceLeft[A, A1 >: A](prev: Silk[A], op: (A1, A) => A1, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlowSingle[A, A1]
  case class Fold[A, A1 >: A](prev: Silk[A], z: A1, zExpr:ru.Expr[_], op: (A1, A1) => A1, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlowSingle[A, A1]
  case class FoldLeft[A, B](prev: Silk[A], z: B, zExpr:ru.Expr[_], op: (B, A) => B, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlowSingle[A, B]


  def mGroupBy[A, K](c:Context)(f:c.Expr[A=>K]) = {
    import c.universe._
    // TODO resolve local functions
    //println(show(f.tree))
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    //println(t)
    val exprGen = c.Expr[Expr[ru.Expr[(K, Silk[A])]]](t)
    c.Expr[Silk[(K, Silk[A])]](Apply(Select(reify{GroupBy}.tree, newTermName("apply")), List(c.prefix.tree, f.tree, exprGen.tree)))
  }

  case class GroupBy[A, K](prev: Silk[A], f: A => K, fExpr:ru.Expr[_]) extends WithInput[A] with SilkFlow[A, (K, Silk[A])]

  case class Join[A, B, K](left: Silk[A], right: Silk[B], k1: (A) => K, k2: (B) => K) extends SilkFlow[(A, B), (K, Silk[(A, B)])]
  case class JoinBy[A, B](left: Silk[A], right: Silk[B], cond: (A, B) => Boolean) extends SilkFlow[(A, B), (A, B)]


  // Sorting
  case class SortBy[A, K](prev: Silk[A], f: A => K, ord: Ordering[K]) extends WithInput[A] with SilkFlow[A, A]
  case class Sort[A, A1 >: A](prev: Silk[A], ord: Ordering[A1]) extends WithInput[A] with SilkFlow[A, A1]
  case class Sampling[A](prev: Silk[A], proportion: Double) extends WithInput[A] with SilkFlow[A, A]

  // Zip
  case class Zip[A, B](prev: Silk[A], other: Silk[B]) extends WithInput[A] with SilkFlow[A, (A, B)]
  case class ZipWithIndex[A](prev: Silk[A]) extends WithInput[A] with SilkFlow[A, (A, Int)]



  case class ConvertToSeq[A, B >: A](prev:Silk[A]) extends WithInput[A] with SilkFlowSingle[A, Seq[B]]
  case class ConvertToArray[A, B >: A](prev:Silk[A]) extends WithInput[A] with SilkFlowSingle[A, Array[B]]

  // Command execution
  case class CommandSeq[A, B](prev: Silk[A], next: Silk[B]) extends SilkFlow[A, A]
  case class Run[A](prev: Silk[A]) extends WithInput[A] with SilkFlow[A, A]
  case class LineInput[A](prev:Silk[A]) extends WithInput[A] with SilkFlow[A, String]


  //  class RootWrap[A](val name: String, in: => Silk[A]) extends SilkFlow[Nothing, A] {
//    val lazyF0 = LazyF0(in)
//    //def eval = in.eval
//  }


  def mArgExpr(c:Context)(args:c.Expr[Any]*) : c.Expr[ShellCommand] = {
    import c.universe._

    val argSeq = c.Expr[Seq[Any]](Apply(Select(reify{Seq}.tree, newTermName("apply")), args.map(_.tree).toList))
    val exprGenSeq = for(a <- args) yield {
      val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(a.tree))
      c.Expr[Expr[ru.Expr[_]]](t).tree
    }
    val argExprSeq = c.Expr[Seq[ru.Expr[_]]](Apply(Select(reify{Seq}.tree, newTermName("apply")), exprGenSeq.toList))
    reify{ ShellCommand(c.Expr[CmdBuilder](c.prefix.tree).splice.sc, argSeq.splice, argExprSeq.splice) }
  }


}



case class ShellCommand(sc:StringContext, args:Seq[Any], argsExpr:Seq[ru.Expr[_]]) extends SilkFlow[Nothing, ShellCommand] with Logger {
  override def toString = s"ShellCommand(${templateString}, ${argsExpr})"
  //def |[A, B](next: A => B) = FlowMap(this, next)

  //def file = SilkFlow.SaveToFile(this)
  def &&[A](next: Silk[A]) = SilkFlow.CommandSeq(this, next)
  def as(file:String) = SilkFlow.SaveAs(this, file)

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
  def lines =  SilkFlow.LineInput(this)

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
