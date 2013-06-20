//--------------------------------------
//
// SilkMacros.scala
// Since: 2013/06/19 5:28 PM
//
//--------------------------------------

package xerial.silk.framework.ops

import scala.reflect.macros.Context
import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import java.io.File

/**
 * Defines macros for generating Silk operation objects
 *
 * @author Taro L. Saito
 */
private[silk] object SilkMacros {

  class MacroHelper[C <: Context](val c: C) {

    import c.universe._

    /**
     * Removes nested reifyTree application to Silk operations.
     */
    object RemoveDoubleReify extends Transformer {
      override def transform(tree: c.Tree) = {
        tree match {
          case Apply(t@TypeApply(s@Select(idt@Ident(q), termname), sa), List(fc, in, f, reified))
            if termname.decoded == "apply" && q.decoded.endsWith("Op")
          =>
            Apply(TypeApply(s, sa), List(fc, in, f, c.unreifyTree(reified)))
          case Apply(s@Select(idt@Ident(q), termname), List(fc, in, f, reified))
            if termname.decoded == "apply" && q.decoded.endsWith("Op")
          =>
            Apply(s, List(fc, in, f, c.unreifyTree(reified)))
          case _ => super.transform(tree)
        }
      }
    }

    def removeDoubleReify(tree: c.Tree) = {
      RemoveDoubleReify.transform(tree)
    }

    /**
     * Find a function/variable/class context where the expression is used
     * @return
     */
    def createFContext: c.Expr[FContext] = {
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decoded
        case _ => "<init>"
      }

      val mne = c.literal(methodName)
      val selfCl = c.Expr[AnyRef](This(tpnme.EMPTY))
      val vd = findValDef
      val vdTree = vd.map {
        v =>
          val nme = c.literal(v.name.decoded)
          reify {
            Some(nme.splice)
          }
      } getOrElse {
        reify {
          None
        }
      }
      reify {
        FContext(selfCl.splice.getClass, mne.splice, vdTree.splice)
      }
    }

    // Find a target variable of the operation result by scanning closest ValDefs
    def findValDef: Option[ValOrDefDef] = {

      def print(p: c.Position) = s"${p.line}(${p.column})"

      val prefixPos = c.prefix.tree.pos

      class Finder extends Traverser {

        var enclosingDef: List[ValOrDefDef] = List.empty
        var cursor: c.Position = null

        private def contains(p: c.Position, start: c.Position, end: c.Position) =
          start.precedes(p) && p.precedes(end)

        override def traverse(tree: Tree) {
          if (tree.pos.isDefined)
            cursor = tree.pos
          tree match {
            // Check whether the rhs of variable definition contains the prefix expression
            case vd@ValDef(mod, varName, tpt, rhs) =>
              // Record the start and end positions of the variable definition block
              val startPos = vd.pos
              super.traverse(rhs)
              val endPos = cursor
              //println(s"val $varName range:${print(startPos)} - ${print(endPos)}: prefix: ${print(prefixPos)}")
              if (contains(prefixPos, startPos, endPos)) {
                enclosingDef = vd :: enclosingDef
              }
            case other =>
              super.traverse(other)
          }
        }

        def enclosingValDef = enclosingDef.reverse.headOption
      }

      val f = new Finder()
      val m = c.enclosingMethod
      if (m == null)
        f.traverse(c.enclosingClass)
      else
        f.traverse(m)
      f.enclosingValDef
    }

  }


  /**
   * Generating a new RawSeq instance of SilkMini[A] and register the input data to
   * the value holder of SilkSession. To avoid double registration, this method retrieves
   * enclosing method name and use it as a key for the cache table.
   * @return
   */
  def newSilkImpl[A](c: Context)(in: c.Expr[Seq[A]])(ev: c.Expr[ClassTag[A]]): c.Expr[SilkSeq[A]] = {
    import c.universe._

    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFContext
    reify {
      val input = in.splice
      val fref = frefExpr.splice
      //val id = ss.seen.getOrElseUpdate(fref, ss.newID)
      val r = RawSeq(fref, input)(ev.splice)
      //SilkOps.cache.putIfAbsent(r.uuid, Seq(RawSlice(Host("localhost", "127.0.0.1"), 0, input)))
      r
    }
  }

  def loadImpl(c: Context)(file: c.Expr[String]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFContext
    reify {
      val fref = frefExpr.splice
      val r = LoadFile(fref, new File(file.splice))
      r
    }
  }


  def newOp[F, Out](c: Context)(op: c.Tree, f: c.Expr[F]) = {
    import c.universe._
    val ft = f.tree
    val helper = new MacroHelper[c.type](c)
    val rmdup = helper.removeDoubleReify(ft)
    val checked = c.typeCheck(rmdup)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    val frefTree = helper.createFContext.tree.asInstanceOf[c.Tree]
    c.Expr[SilkSeq[Out]](Apply(Select(op, newTermName("apply")), List(frefTree, c.prefix.tree, f.tree, exprGen)))
  }

  def newSingleOp[F, Out](c: Context)(op: c.Tree, f: c.Expr[F]) = {
    import c.universe._
    // TODO share the same code with newOp
    val ft = f.tree
    val helper = new MacroHelper[c.type](c)
    val rmdup = helper.removeDoubleReify(ft)
    val checked = c.typeCheck(rmdup)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    val frefTree = helper.createFContext.tree.asInstanceOf[c.Tree]
    c.Expr[SilkSingle[Out]](Apply(Select(op, newTermName("apply")), List(frefTree, c.prefix.tree, f.tree, exprGen)))
  }


  def mForeach[A, B](c: Context)(f: c.Expr[A => B]) =
    newOp[A => B, B](c)(c.universe.reify { ForeachOp }.tree, f)

  def mMap[A, B](c: Context)(f: c.Expr[A => B]) = newOp[A => B, B](c)(c.universe.reify{ MapOp }.tree, f)
  def mFlatMap[A, B](c: Context)(f: c.Expr[A => SilkSeq[B]]) = newOp[A => SilkSeq[B], B](c)(c.universe.reify { FlatMapOp }.tree, f)


  def mapSingleImpl[A, B](c: Context)(f: c.Expr[A => B]) =
    newSingleOp[A => B, B](c)(c.universe.reify {MapSingleOp}.tree, f)

  def mGroupBy[A:c.WeakTypeTag, K:c.WeakTypeTag](c: Context)(f: c.Expr[A => K]) = {
    newOp[A => K, (K, SilkSeq[A])](c)(c.universe.reify{GroupByOp}.tree, f)
  }


  def flatMapSingleImpl[A, B](c: Context)(f: c.Expr[A => SilkSeq[B]]) = {
    newOp[A => SilkSeq[B], B](c)(c.universe.reify {
      FlatMapOp
    }.tree, f)
  }

  def mFilter[A](c: Context)(f: c.Expr[A => Boolean]) = {
    newOp[A => Boolean, A](c)(c.universe.reify {
      FilterOp
    }.tree, f)
  }

  def mFilterNot[A](c: Context)(f: c.Expr[A => Boolean]) = {
    newOp[A => Boolean, A](c)(c.universe.reify {
      FilterOp
    }.tree, c.universe.reify{ (x:A) => !f.splice(x) })
  }


  def filterSingleImpl[A](c: Context)(f: c.Expr[A => Boolean]) = {
    newSingleOp[A => Boolean, A](c)(c.universe.reify {
      FilterSingleOp
    }.tree, f)
  }


  def mSplit[A:c.WeakTypeTag](c:Context) : c.Expr[SilkSeq[SilkSeq[A]]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    c.Expr[SilkSeq[SilkSeq[A]]](Apply(Select(reify{SplitOp}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree)))
  }

  def mConcat[A, B:c.WeakTypeTag](c:Context)(asSilkSeq:c.Expr[A=>SilkSeq[B]]) : c.Expr[SilkSeq[B]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    c.Expr[SilkSeq[B]](Apply(Select(reify{ConcatOp}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, asSilkSeq.tree)))
  }


  def mNaturalJoin[A: c.WeakTypeTag, B](c: Context)(other: c.Expr[SilkSeq[B]])(ev1: c.Expr[scala.reflect.ClassTag[A]], ev2: c.Expr[scala.reflect.ClassTag[B]]): c.Expr[SilkSeq[(A, B)]] = {
    import c.universe._

    val helper = new MacroHelper(c)
    val fref = helper.createFContext
    reify {
      NaturalJoinOp(fref.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], other.splice)(ev1.splice, ev2.splice)
    }
  }

  def mJoin[A, K, B](c:Context)(other:c.Expr[SilkSeq[B]], k1:c.Expr[A=>K], k2:c.Expr[B=>K]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify { JoinOp(fc.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], other.splice, k1.splice, k2.splice) }
  }

  def mSampling[A:c.WeakTypeTag](c: Context)(proportion: c.Expr[Double]) : c.Expr[SilkSeq[A]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    //reify { SamplingOp(fc.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], proportion.splice) }
    c.Expr[SilkSeq[A]](Apply(Select(reify{SamplingOp}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, proportion.tree)))
  }

  def mSortBy[A:c.WeakTypeTag, K](c: Context)(keyExtractor: c.Expr[A=>K])(ord:c.Expr[Ordering[K]]) : c.Expr[SilkSeq[A]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext

    c.Expr[SilkSeq[A]](Apply(Select(reify{SortByOp}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, keyExtractor.tree, ord.tree)))
  }

  def mSorted[A:c.WeakTypeTag](c: Context)(ord:c.Expr[Ordering[A]]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify { NoOp[A](fc.splice) }
    //c.Expr[SilkSeq[A]](Apply(Select(reify{SortOp}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, ord.tree)))
  }


  def mZip[A:c.WeakTypeTag, B:c.WeakTypeTag](c: Context)(other: c.Expr[SilkSeq[B]]) : c.Expr[SilkSeq[(A, B)]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    c.Expr[SilkSeq[(A, B)]](Apply(Select(reify{ZipOp}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, other.tree)))
  }

  def mZipWithIndex[A:c.WeakTypeTag](c: Context) : c.Expr[SilkSeq[(A, Int)]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    c.Expr[SilkSeq[(A, Int)]](Apply(Select(reify{ZipWithIndexOp}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree)))
  }

  def mMkStringDefault[A:c.WeakTypeTag](c:Context) : c.Expr[SilkSingle[String]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify { MkStringOp(fc.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], "", "", "" ) }
  }

  def mMkStringSep[A:c.WeakTypeTag](c:Context)(sep:c.Expr[String]) : c.Expr[SilkSingle[String]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify { MkStringOp(fc.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], "", sep.splice, "" ) }
  }

  def mMkString[A:c.WeakTypeTag](c:Context)(start:c.Expr[String], sep:c.Expr[String], end:c.Expr[String]) : c.Expr[SilkSingle[String]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify { MkStringOp(fc.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], start.splice, sep.splice, end.splice ) }
  }


  //  def mJoinBy[A, B](c:Context)(other:c.Expr[SilkSeq[B]], cond:c.Expr[(A, B)=>Boolean]) = {
//    import c.universe._
//    val fc = new MacroHelper[c.type](c).createFContext
//    reify { JoinByOp(fc.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], other.splice, cond.splice) }
//  }

  def mReduce[A:c.WeakTypeTag](c: Context)(f: c.Expr[(A, A) => A]) = {
    newSingleOp[(A, A) => A, A](c)(c.universe.reify {
      ReduceOp
    }.tree, f)
  }

  def mSum[A:c.WeakTypeTag](c:Context)(num:c.Expr[Numeric[A]]) : c.Expr[SilkSingle[A]] = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify { NumericFold[A](fc.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], num.splice.zero, num.splice.plus) }
  }

  def mProduct[A:c.WeakTypeTag](c:Context)(num:c.Expr[Numeric[A]]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    val e = c.Expr[SilkSingle[A]](Apply(Select(reify{NumericFold}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, reify{num.splice.one}.tree, reify{num.splice.times(_, _)}.tree)))
    reify { e.splice }
  }

  def mMin[A:c.WeakTypeTag](c:Context)(cmp:c.Expr[Ordering[A]]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    val e = c.Expr[SilkSingle[A]](Apply(Select(
      reify{NumericReduce}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, reify{{(x:A, y:A) => if (cmp.splice.lteq(x, y)) x else y }}.tree)))
    reify { e.splice }
  }

  def mMax[A:c.WeakTypeTag](c:Context)(cmp:c.Expr[Ordering[A]]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    val e = c.Expr[SilkSingle[A]](Apply(Select(
      reify{NumericReduce}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, reify{{(x:A, y:A) => if (cmp.splice.gteq(x, y)) x else y }}.tree)))
    reify { e.splice }
  }

  def mMinBy[A, B](c:Context)(f: c.Expr[A=>B])(cmp:c.Expr[Ordering[B]]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    val e = c.Expr[SilkSingle[A]](Apply(Select(
      reify{NumericReduce}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, reify{{(x:A, y:A) => if (cmp.splice.lteq(f.splice(x), f.splice(y))) x else y }}.tree)))
    reify { e.splice }
  }

  def mMaxBy[A, B](c:Context)(f: c.Expr[A=>B])(cmp:c.Expr[Ordering[B]]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    val e = c.Expr[SilkSingle[A]](Apply(Select(
      reify{NumericReduce}.tree, newTermName("apply")), List(fc.tree, c.prefix.tree, reify{{(x:A, y:A) => if (cmp.splice.gteq(f.splice(x), f.splice(y))) x else y }}.tree)))
    reify { e.splice }
  }


  //  private def helperFold[F, B](c:Context)(z:c.Expr[B], f:c.Expr[F], op:c.Tree) = {
  //    import c.universe._
  //    // TODO resolve local functions
  //    val zt = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(z.tree))
  //    val zexprGen = c.Expr[Expr[ru.Expr[B]]](zt)
  //    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
  //    val exprGen = c.Expr[Expr[ru.Expr[F]]](t)
  //    c.Expr[SilkSingle[B]](Apply(Select(op, newTermName("apply")), List(c.prefix.tree, z.tree, zexprGen.tree, f.tree, exprGen.tree)))
  //  }
  //
  //
  //  def mFold[A, A1](c:Context)(z:c.Expr[A1])(op:c.Expr[(A1,A1)=>A1]) =
  //    helperFold[(A1,A1)=>A1, A1](c)(z, op, c.universe.reify{Fold}.tree)
  //
  //  def mFoldLeft[A,B](c:Context)(z:c.Expr[B])(op:c.Expr[(B,A)=>B]) =
  //    helperFold[(B,A)=>B, B](c)(z, op, c.universe.reify{FoldLeft}.tree)




}