//--------------------------------------
//
// SilkMacros.scala
// Since: 2013/06/19 5:28 PM
//
//--------------------------------------

package xerial.silk.core

import scala.reflect.macros.Context
import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.runtime.{universe => ru}
import java.io.File
import xerial.silk._
import scala.collection.GenTraversable
import xerial.silk.Silk.CommandBuilder
import xerial.silk.Silk.SilkArrayWrap
import xerial.silk.Silk.SilkSeqWrap
import xerial.silk.Silk.SilkWrap

/**
 * Defines macros for generating Silk operation objects
 *
 * @author Taro L. Saito
 */
private[silk] object SilkMacros {

  class MacroHelper[C <: Context](val c: C) {

    import c.universe._

    //def seq = reify { c.prefix.splice.asInstanceOf[SilkSeq[_]] }

    def removeDoubleReify(tree: c.Tree) = {
      /**
       * Removes nested reifyTree application to Silk operations.
       */
      object RemoveDoubleReify extends Transformer {

        val target = Set("MapOp", "SizeOp", "ConcatOp", "FlatMapOp", "ForeachOp", "GroupByOp", "MapSingleOp", "FilterSingleOp", "ReduceOp")
        def isTarget(className:String) = className.endsWith("Op")
        //def isTarget(className:String) = target.contains(className)

        override def transform(tree: c.Tree) = {
          tree match {
            case Apply(t@TypeApply(s@Select(idt@Ident(q), termname), sa), List(uuid, fc, in, f, reified))
              if termname.decoded == "apply" && isTarget(q.decoded)
            =>
              Apply(TypeApply(s, sa), List(uuid, fc, in, f, c.unreifyTree(reified)))
            case Apply(s@Select(idt@Ident(q), termname), List(uuid, fc, in, f, reified))
              if termname.decoded == "apply" && isTarget(q.decoded)
            =>
              Apply(s, List(uuid, fc, in, f, c.unreifyTree(reified)))
            case _ => super.transform(tree)
          }
        }
      }

      RemoveDoubleReify.transform(tree)
    }

    /**
     * Find a function/variable/class context where the expression is used
     * @return
     */
    def createFContext: c.Expr[FContext] = {
      // Find the enclosing method.
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decoded
        case other =>
          "<constructor>"
      }


      val selfCl = c.Expr[AnyRef](This(tpnme.EMPTY))
      val vd = findValDef
      var parent : c.Expr[Option[String]] = reify {None}
      val vdTree = if(vd.isEmpty)
        reify{None}
      else {
        if(!vd.tail.isEmpty) {
          parent = reify { Some(c.literal(vd.tail.head.name.decoded).splice) }
        }
        val nme = c.literal(vd.head.name.decoded)
        reify{Some(nme.splice)}
      }
//      val vdTree = vd match {
//        case Some(v) =>
//          val nme = c.literal(v.name.decoded)
//          reify { Some(nme.splice)}
//        case None =>
//          reify { None }
//      }

      val mne = c.literal(methodName)
      val pos = c.enclosingPosition

      val l_line = c.literal(pos.line)
      val l_pos = c.literal(pos.column)
      val l_source = c.literal(pos.source.path)
      reify {
        FContext(selfCl.splice.getClass, mne.splice, vdTree.splice, parent.splice, l_source.splice, l_line.splice, l_pos.splice)
      }
    }

    // Find a target variable of the operation result by scanning closest ValDefs
    def findValDef: List[ValOrDefDef] = {

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
      f.enclosingDef.reverse
    }


    def createExprTree[F](f:c.Expr[F]) : c.Expr[ru.Expr[F]] = {
      val rmdup = removeDoubleReify(f.tree)
      val checked = c.typeCheck(rmdup)
      val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
      val exprGen = c.Expr[ru.Expr[F]](t)
      exprGen
    }

    def createVDef[A:c.WeakTypeTag](op:c.Expr[_]) = {
      val fc = createFContext
      reify {
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _fc = fc.splice
        val _cl = op.splice.getClass
      }
    }

    def opGen[A:c.WeakTypeTag, Out](op:c.Expr[_], args:Seq[c.Expr[_]]) = {
      //val vdef = createVDef[A](op)
      val fc = createFContext

      // Create a new UUID via SilkUtil.newUUID(FContext, inputs...)
      // This macro creates NewOp(SilkUtil.newUUID(_fc, _prefix), _fc, _prefix, f)
      val e = c.Expr[SilkSeq[Out]](
        Apply(
          Select(op.tree, newTermName("apply")),
          List(
            Apply(Select(reify{SilkUtil}.tree, newTermName("newUUIDOf")),
              List(
                reify{op.splice.getClass}.tree,
                Ident(newTermName("_fc")),
                Ident(newTermName("_prefix")))),
            Ident(newTermName("_fc")),
            Ident(newTermName("_prefix"))
          ) ++ args.map(_.tree).toList
        )
      )
      // Wrap with a block to hide the above variable definitions from the outer context
      reify {
        {
          val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
          val _fc = fc.splice
          val _cl = op.splice.getClass
          e.splice
        }
      }
    }

    def opSingleGen[A:c.WeakTypeTag, Out](op:c.Expr[_], args:Seq[c.Expr[_]]) = {
      val fc = createFContext
      // Create a new UUID via SilkUtil.newUUID(FContext, inputs...)
      // This macro creates NewOp(SilkUtil.newUUID(_fc, _prefix), _fc, _prefix, f)
      val e = c.Expr[SilkSingle[Out]](
        Apply(
          Select(op.tree, newTermName("apply")),
          List(
            Apply(Select(reify{SilkUtil}.tree, newTermName("newUUIDOf")),
              List(
                Ident(newTermName("_cl")),
                Ident(newTermName("_fc")),
                Ident(newTermName("_prefix")))),
            Ident(newTermName("_fc")),
            Ident(newTermName("_prefix"))
          ) ++ args.map(_.tree).toList
        )
      )
      // Wrap with a block to hide the above variable definitions from the outer context
      reify {
        {
          val _prefix = c.prefix.splice.asInstanceOf[SilkSingle[A]]
          val _fc = fc.splice
          val _cl = op.splice.getClass
          e.splice
        }
      }
    }

    def opReduceGen[A:c.WeakTypeTag, Out](op:c.Expr[_], args:Seq[c.Expr[_]]) = {
      val fc = createFContext
      // Create a new UUID via SilkUtil.newUUID(FContext, inputs...)
      // This macro creates NewOp(SilkUtil.newUUID(_fc, _prefix), _fc, _prefix, f)
      val e = c.Expr[SilkSingle[Out]](
        Apply(
          Select(op.tree, newTermName("apply")),
          List(
            Apply(Select(reify{SilkUtil}.tree, newTermName("newUUIDOf")),
              List(
                Ident(newTermName("_cl")),
                Ident(newTermName("_fc")),
                Ident(newTermName("_prefix")))),
            Ident(newTermName("_fc")),
            Ident(newTermName("_prefix"))
          ) ++ args.map(_.tree).toList
        )
      )
      // Wrap with a block to hide the above variable definitions from the outer context
      reify {
        {
          val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
          val _fc = fc.splice
          val _cl = op.splice.getClass
          e.splice
        }
      }
    }

  }


  /**
   * Generating a new RawSeq instance of SilkMini[A]
   * @return
   */
  def mNewSilk[A:c.WeakTypeTag](c: Context)(in: c.Expr[Seq[A]]): c.Expr[SilkSeq[A]] = {
    import c.universe._

    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    //val selfCl = c.Expr[AnyRef](This(tpnme.EMPTY))
    reify {
      {
        val _fc = fc.splice
        val _in = in.splice
        RawSeq(SilkUtil.newUUIDOf(classOf[RawSeq[_]], _fc), _fc, _in)
      }
    }
  }

  def mRawSeq[A:c.WeakTypeTag](c: Context): c.Expr[SilkSeq[A]] = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        RawSeq(SilkUtil.newUUIDOf(classOf[RawSeq[_]], _fc), _fc, c.prefix.splice.asInstanceOf[SilkSeqWrap[A]].a)
      }
    }
  }


  def mArrayToSilk[A:c.WeakTypeTag](c: Context): c.Expr[SilkSeq[A]] = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        val _prefix = c.prefix.splice.asInstanceOf[SilkArrayWrap[A]]
        RawSeq(SilkUtil.newUUIDOf(classOf[RawSeq[_]], _fc), _fc, _prefix.a)
      }
    }
  }

  def mNewSilkSingle[A:c.WeakTypeTag](c:Context) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        RawSingle(SilkUtil.newUUIDOf(classOf[RawSingle[_]], _fc), _fc, c.prefix.splice.asInstanceOf[SilkWrap[A]].a)
      }
    }
  }


  def mScatter[A:c.WeakTypeTag](c: Context)(in: c.Expr[Seq[A]], numNodes:c.Expr[Int]): c.Expr[SilkSeq[A]] = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        ScatterSeq(SilkUtil.newUUIDOf(classOf[ScatterSeq[_]], _fc), _fc, in.splice, numNodes.splice)
      }
    }
  }

  def loadImpl(c: Context)(file: c.Expr[String]) : c.Expr[LoadFile] = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        val f = new File(file.splice)
        val r = LoadFile(SilkUtil.newUUIDOf(classOf[LoadFile], _fc, f.getPath), _fc, f)
        r
      }
    }
  }


  def open(c: Context)(file: c.Expr[File]) : c.Expr[LoadFile] = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        val f = file.splice
        val r = LoadFile(SilkUtil.newUUIDOf(classOf[LoadFile], _fc, f.getPath), _fc, f)
        r
      }
    }
  }


  /**
   * SilkSeq => SilkSeq
   * @param c
   * @param op
   * @param exprs
   * @tparam A
   * @tparam Out
   * @return
   */
  def newOp[A:c.WeakTypeTag, Out](c: Context)(op: c.Expr[_], exprs:c.Expr[_]*) = {
    val helper = new MacroHelper[c.type](c)
    helper.opGen[A, Out](op, exprs.toSeq)
  }

  /**
   * SilkSingle => SilkSingle
   * @param c
   * @param op
   * @param f
   * @tparam A
   * @tparam Out
   * @return
   */
  def newSingleOp[A:c.WeakTypeTag, Out](c: Context)(op: c.Expr[_], f: c.Expr[_]*) = {
    val helper = new MacroHelper[c.type](c)
    helper.opSingleGen[A, Out](op, f.toSeq)
  }

  /**
   * SilkSeq => SilkSingle
   * @param c
   * @param op
   * @param f
   * @tparam A
   * @tparam Out
   * @return
   */
  def newReduceOp[A:c.WeakTypeTag, Out](c: Context)(op: c.Expr[_], f: c.Expr[_]*) = {
    val helper = new MacroHelper[c.type](c)
    helper.opReduceGen[A, Out](op, f.toSeq)
  }


  def mSize[A:c.WeakTypeTag](c:Context) =
    newReduceOp[A, Long](c)(c.universe.reify{SizeOp})

  def mIsEmpty[A:c.WeakTypeTag](c:Context)(weaver:c.Expr[Weaver]) = {
    import c.universe._
    val fc_e = new MacroHelper[c.type](c).createFContext
    reify {
      {
        val prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val fc = fc_e.splice
        SizeOp(SilkUtil.newUUIDOf(classOf[SizeOp[_]], fc, prefix), fc, prefix).get(weaver.splice) == 0
      }
    }
  }

  def mMapWith[A:c.WeakTypeTag, B:c.WeakTypeTag, R1:c.WeakTypeTag](c:Context)(r1:c.Expr[Silk[R1]])(f:c.Expr[(A, R1) => B]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _fc = fc.splice
        val _r1 = r1.splice
        MapWithOp[A, B, R1](SilkUtil.newUUIDOf(classOf[MapWithOp[_, _, _]], _fc, _prefix, _r1), _fc, _prefix, _r1, f.splice)
      }
    }
  }

  def mFlatMapWith[A:c.WeakTypeTag, B:c.WeakTypeTag, R1:c.WeakTypeTag](c:Context)(r1:c.Expr[Silk[R1]])(f:c.Expr[(A, R1) => Silk[B]]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _fc = fc.splice
        val _r1 = r1.splice
        FlatMapWithOp[A, B, R1](SilkUtil.newUUIDOf(classOf[FlatMapWithOp[_, _, _]], _fc, _prefix, _r1), _fc, _prefix, _r1, f.splice)
      }
    }
  }

  def mFlatMapSeqWith[A:c.WeakTypeTag, B:c.WeakTypeTag, R1:c.WeakTypeTag](c:Context)(r1:c.Expr[Silk[R1]])(f:c.Expr[(A, R1) => GenTraversable[B]]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _fc = fc.splice
        val _r1 = r1.splice
        FlatMapSeqWithOp[A, B, R1](SilkUtil.newUUIDOf(classOf[FlatMapSeqWithOp[_, _, _]], _fc, _prefix, _r1), _fc, _prefix, _r1, f.splice)
      }
    }
  }


  def mFlatMap2With[A:c.WeakTypeTag, B:c.WeakTypeTag, R1:c.WeakTypeTag, R2:c.WeakTypeTag](c:Context)(r1:c.Expr[Silk[R1]], r2:c.Expr[Silk[R2]])(f:c.Expr[(A, R1, R2) => Silk[B]]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _fc = fc.splice
        val _r1 = r1.splice
        val _r2 = r2.splice
        FlatMap2WithOp[A, B, R1, R2](SilkUtil.newUUIDOf(classOf[FlatMap2WithOp[_, _, _, _]], _fc, _prefix, _r1, _r2), _fc, _prefix, _r1, _r2, f.splice)
      }
    }
  }

  def mMap2With[A:c.WeakTypeTag, B:c.WeakTypeTag, R1:c.WeakTypeTag, R2:c.WeakTypeTag](c:Context)(r1:c.Expr[Silk[R1]], r2:c.Expr[Silk[R2]])(f:c.Expr[(A, R1, R2) => B]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _fc = fc.splice
        val _r1 = r1.splice
        val _r2 = r2.splice
        Map2WithOp[A, B, R1, R2](SilkUtil.newUUIDOf(classOf[Map2WithOp[_, _, _, _]], _fc, _prefix, _r1, _r2), _fc, _prefix, _r1, _r2, f.splice)
      }
    }
  }

  def mForeach[A:c.WeakTypeTag, B](c: Context)(f: c.Expr[A => B]) =
    newOp[A, B](c)(c.universe.reify { ForeachOp }, f)
  def mMap[A:c.WeakTypeTag, B:c.WeakTypeTag](c: Context)(f: c.Expr[A => B]) =
    newOp[A, B](c)(c.universe.reify{MapOp}, f)
  def mFlatMap[A:c.WeakTypeTag, B](c: Context)(f: c.Expr[A => SilkSeq[B]]) =
    newOp[A, B](c)(c.universe.reify { FlatMapOp }, f)
  def mFlatMapSeq[A:c.WeakTypeTag, B](c: Context)(f: c.Expr[A => GenTraversable[B]]) =
    newOp[A, B](c)(c.universe.reify { FlatMapSeqOp }, f)
  def mapSingleImpl[A:c.WeakTypeTag, B](c: Context)(f: c.Expr[A => B]) =
    newSingleOp[A, B](c)(c.universe.reify {MapSingleOp}, f)
  def mGroupBy[A:c.WeakTypeTag, K:c.WeakTypeTag](c: Context)(f: c.Expr[A => K]) =
    newOp[A, (K, SilkSeq[A])](c)(c.universe.reify{GroupByOp}, f)
  def flatMapSingleImpl[A:c.WeakTypeTag, B](c: Context)(f: c.Expr[A => SilkSeq[B]]) =
    newOp[A, B](c)(c.universe.reify { FlatMapOp}, f)
  def mFilter[A:c.WeakTypeTag](c: Context)(cond: c.Expr[A => Boolean]) =
    newOp[A, A](c)(c.universe.reify{FilterOp}, cond)
  def mFilterNot[A:c.WeakTypeTag](c: Context)(cond: c.Expr[A => Boolean]) =
    newOp[A,A](c)(c.universe.reify{FilterOp}, c.universe.reify{(x:A) => !cond.splice(x)})

  def mFilterSingle[A:c.WeakTypeTag](c: Context)(cond: c.Expr[A => Boolean]) =
    newSingleOp[A, A](c)(c.universe.reify { FilterSingleOp}, cond)

  def mSplit[A:c.WeakTypeTag](c:Context) =
    newOp[A, SilkSeq[A]](c)(c.universe.reify{SplitOp})

  def mConcat[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context) =
    newOp[A, B](c)(c.universe.reify{ConcatOp})

  def mSampling[A:c.WeakTypeTag](c: Context)(proportion: c.Expr[Double]) = {
    import c.universe._
    newOp[A, A](c)(reify{SamplingOp}, proportion)
  }

  def mSortBy[A:c.WeakTypeTag, K](c: Context)(keyExtractor: c.Expr[A=>K])(ord:c.Expr[Ordering[K]]) = {
    import c.universe._
    newOp[A, A](c)(reify{SortOp}, keyExtractor, ord)
  }

  def mSorted[A:c.WeakTypeTag](c: Context)(partitioner:c.Expr[Partitioner[A]])(ord:c.Expr[Ordering[A]]) =
    newOp[A, A](c)(c.universe.reify{SortOp}, ord, partitioner)

  def mZip[A:c.WeakTypeTag, B:c.WeakTypeTag](c: Context)(other: c.Expr[SilkSeq[B]]) = {
    import c.universe._
    newOp[A, (A, B)](c)(reify{ZipOp}, other)
  }

  def mZipWithIndex[A:c.WeakTypeTag](c: Context) = {
    import c.universe._
    newOp[A, (A, Int)](c)(reify{ZipWithIndexOp})
  }

  def mMkStringDefault[A:c.WeakTypeTag](c:Context) = {
    import c.universe._
    newReduceOp[A, String](c)(reify{MkStringOp}, reify{""}, reify{""}, reify{""})
  }

  def mMkStringSep[A:c.WeakTypeTag](c:Context)(sep:c.Expr[String]) = {
    import c.universe._
    newReduceOp[A, String](c)(reify{MkStringOp}, reify{""}, sep, reify{""})
  }

  def mMkString[A:c.WeakTypeTag](c:Context)(start:c.Expr[String], sep:c.Expr[String], end:c.Expr[String]) = {
    import c.universe._
    newReduceOp[A, String](c)(reify{MkStringOp}, start, sep, end)
  }

  def mReduce[A:c.WeakTypeTag](c: Context)(f: c.Expr[(A, A) => A]) =
    newReduceOp[A, A](c)(c.universe.reify { ReduceOp }, f)

  def mSum[A:c.WeakTypeTag](c:Context)(num:c.Expr[Numeric[A]]) = {
    import c.universe._
    newReduceOp[A, A](c)(reify{NumericFold}, reify{num.splice.zero}, reify{{(x:A, y:A)=>num.splice.plus(x, y)}})
  }

  def mProduct[A:c.WeakTypeTag](c:Context)(num:c.Expr[Numeric[A]]) = {
    import c.universe._
    newReduceOp[A, A](c)(reify{NumericFold}, reify{num.splice.one}, reify{{(x:A, y:A) => num.splice.times(x, y)}})
  }

  def mMin[A:c.WeakTypeTag](c:Context)(cmp:c.Expr[Ordering[A]]) = {
    import c.universe._
    newReduceOp[A, A](c)(reify{NumericReduce}, reify{{(x:A, y:A) => if (cmp.splice.lteq(x, y)) x else y }})
  }

  def mMax[A:c.WeakTypeTag](c:Context)(cmp:c.Expr[Ordering[A]]) = {
    import c.universe._
    newReduceOp[A, A](c)(reify{NumericReduce}, reify{{(x:A, y:A) => if (cmp.splice.gteq(x, y)) x else y }})
  }

  def mMinBy[A, B](c:Context)(f: c.Expr[A=>B])(cmp:c.Expr[Ordering[B]]) = {
    import c.universe._
    newReduceOp[A, A](c)(reify{NumericReduce}, reify{{(x:A, y:A) => if (cmp.splice.lteq(f.splice(x), f.splice(y))) x else y }})
  }

  def mMaxBy[A, B](c:Context)(f: c.Expr[A=>B])(cmp:c.Expr[Ordering[B]]) = {
    import c.universe._
    newReduceOp[A, A](c)(reify{NumericReduce}, reify{{(x:A, y:A) => if (cmp.splice.gteq(f.splice(x), f.splice(y))) x else y }})
  }

  def mHead[A:c.WeakTypeTag](c:Context) = {
    newReduceOp[A, A](c)(c.universe.reify(HeadOp))
  }

  def mCollect[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(pf:c.Expr[PartialFunction[A,B]]) = {
    import c.universe._
    newOp[A, B](c)(reify{CollectOp}, pf)
  }

  def mCollectFirst[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(pf:c.Expr[PartialFunction[A,B]]) = {
    import c.universe._
    newReduceOp[A, Option[B]](c)(reify{CollectFirstOp}, pf)
  }

  def mDistinct[A:c.WeakTypeTag](c:Context) = {
    import c.universe._
    newOp[A, A](c)(reify{DistinctOp})
  }

  def mAggregate[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(z:c.Expr[B])(seqop:c.Expr[(B,A)=>B], combop:c.Expr[(B,B)=>B]) = {
    import c.universe._
    newReduceOp[A, B](c)(reify{AggregateOp}, z, seqop, combop)
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


  def mCommand(c:Context)(args:c.Expr[Any]*) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val argSeq = c.Expr[Seq[Any]](Apply(Select(reify{Seq}.tree, newTermName("apply")), args.map(_.tree).toList))
//    val exprGenSeq = for(a <- args) yield {
//      val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(a.tree))
//      c.Expr[Expr[ru.Expr[_]]](t).tree
//    }
    //val argExprSeq = c.Expr[Seq[ru.Expr[_]]](Apply(Select(reify{Seq}.tree, newTermName("apply")), exprGenSeq.toList))
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        val _prefix = c.prefix.splice.asInstanceOf[CommandBuilder]
        val _args = argSeq.splice
        val _inputs = Seq(Command.templateString(_prefix.sc)) ++ Command.silkInputs(_args)
        CommandOp(SilkUtil.newUUIDOf(classOf[CommandOp], _fc, _inputs:_*), _fc, _prefix.sc, _args, None)
      }
    }
  }

  def mShuffle[A:c.WeakTypeTag](c:Context)(partitioner:c.Expr[Partitioner[A]]) = {
    import c.universe._
    newOp[A, (Int, SilkSeq[A])](c)(reify{ShuffleOp}, partitioner)
  }

  def mShuffleReduce[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context) = {
    import c.universe._
    newOp[A, (Int, SilkSeq[B])](c)(reify{ShuffleReduceOp})
  }

  def mShuffleMerge[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(a:c.Expr[SilkSeq[A]], b:c.Expr[SilkSeq[B]], probeA:c.Expr[A=>Int], probeB:c.Expr[B=>Int]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        val _a = a.splice
        val _b = b.splice
        ShuffleMergeOp[A, B](SilkUtil.newUUIDOf(classOf[ShuffleMergeOp[_,_]], _fc, _a, _b), _fc, _a, _b, probeA.splice, probeB.splice)
      }
    }
  }

  def mNaturalJoin[A: c.WeakTypeTag, B](c: Context)(other: c.Expr[SilkSeq[B]])(ev1: c.Expr[scala.reflect.ClassTag[A]], ev2: c.Expr[scala.reflect.ClassTag[B]]): c.Expr[SilkSeq[(A, B)]] = {
    import c.universe._
    val helper = new MacroHelper(c)
    val fc = helper.createFContext
    reify {
      {
        val _fc = fc.splice
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _other = other.splice
        NaturalJoinOp(SilkUtil.newUUIDOf(classOf[NaturalJoinOp[_, _]], _fc, _prefix, _other), _fc, _prefix, _other)(ev1.splice, ev2.splice)
      }
    }
  }

  def mJoin[A:c.WeakTypeTag, K:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(other:c.Expr[SilkSeq[B]], k1:c.Expr[A=>K], k2:c.Expr[B=>K]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify {
      {
        val _fc = fc.splice
        val _prefix = c.prefix.splice.asInstanceOf[SilkSeq[A]]
        val _other = other.splice
        JoinOp(SilkUtil.newUUIDOf(classOf[JoinOp[_,_,_]], _fc, _prefix, _other), _fc, _prefix, _other, k1.splice, k2.splice)
      }
    }
  }

  def mFiles(c:Context)(pattern:c.Expr[String]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify {
      {
        val _fc = fc.splice
        val _pattern = pattern.splice
        ListFilesOp(SilkUtil.newUUIDOf(classOf[ListFilesOp], _fc, _pattern), _fc, _pattern)
      }
    }
  }

  def mDirs(c:Context)(pattern:c.Expr[String]) = {
    import c.universe._
    val fc = new MacroHelper[c.type](c).createFContext
    reify {
      {
        val _fc = fc.splice
        val _pattern = pattern.splice
        ListDirsOp(SilkUtil.newUUIDOf(classOf[ListFilesOp], _fc, _pattern), _fc, _pattern)
      }
    }
  }

  def mSubscribeSeq[A:c.WeakTypeTag](c: Context) =
    newOp[A, A](c)(c.universe.reify{SubscribeSeqOp})

  def mSubscribeSingle[A:c.WeakTypeTag](c: Context) =
    newSingleOp[A, A](c)(c.universe.reify{SubscribeSingleOp})

}