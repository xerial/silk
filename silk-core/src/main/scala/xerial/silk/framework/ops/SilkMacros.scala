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
import scala.reflect.runtime.{universe=>ru}
import xerial.silk.util.MacroUtil
import java.io.File

/**
 * @author Taro L. Saito
 */
object SilkMacros {

  class MacroHelper[C <: Context](val c: C) {

    import c.universe._

    /**
     * Removes nested reifyTree application to Silk operations.
     */
    object RemoveDoubleReify extends Transformer {
      override def transform(tree: c.Tree) = {
        tree match {
          case Apply(TypeApply(s@Select(idt@Ident(q), termname), _), reified :: tail)
            if termname.decoded == "apply" && q.decoded.endsWith("Op")
          => c.unreifyTree(reified)
          case _ => super.transform(tree)
        }
      }
    }

    def removeDoubleReify(tree: c.Tree) = {
      RemoveDoubleReify.transform(tree)
    }

    def createFContext: c.Expr[FContext[_]] = {
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decoded
        case _ => "<init>"
      }

      val mne = c.literal(methodName)
      val self = c.Expr[Class[_]](This(tpnme.EMPTY))
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
      //println(s"vd: ${showRaw(vdTree)}")
      reify {
        FContext(self.splice.getClass, mne.splice, vdTree.splice)
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
      if(m == null)
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
   * @param c
   * @param in
   * @tparam A
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

  def loadImpl[A](c:Context)(file:c.Expr[File])(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFContext
    reify {
      val fref = frefExpr.splice
      val r = LoadFile[A](fref, file.splice)(ev.splice)
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
    val e = c.Expr[SilkSeq[Out]](Apply(Select(op, newTermName("apply")), List(frefTree, c.prefix.tree, f.tree, exprGen)))
    val result = reify {
      val silk = e.splice
      silk
    }
    result
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
    val e = c.Expr[SilkSingle[Out]](Apply(Select(op, newTermName("apply")), List(frefTree, c.prefix.tree, f.tree, exprGen)))
    val result = reify {
      val silk = e.splice
      silk
    }
    result
  }



  import xerial.core.log.Logger

  def mapImpl[A, B](c: Context)(f: c.Expr[A => B]) = {
    newOp[A => B, B](c)(c.universe.reify {
      MapOp
    }.tree, f)
  }
  def mapSingleImpl[A, B](c: Context)(f: c.Expr[A => B]) = {
    newSingleOp[A => B, B](c)(c.universe.reify {
      MapSingleOp
    }.tree, f)
  }

  def flatMapImpl[A, B](c: Context)(f: c.Expr[A => SilkSeq[B]]) = {
    newOp[A => SilkSeq[B], B](c)(c.universe.reify {
      FlatMapOp
    }.tree, f)
  }

  def filterImpl[A](c: Context)(f: c.Expr[A => Boolean]) = {
    newOp[A => Boolean, A](c)(c.universe.reify {
      FilterOp
    }.tree, f)
  }

  def filterSingleImpl[A](c: Context)(f: c.Expr[A => Boolean]) = {
    newSingleOp[A => Boolean, A](c)(c.universe.reify {
      FilterSingleOp
    }.tree, f)
  }


  def naturalJoinImpl[A: c.WeakTypeTag, B](c: Context)(other: c.Expr[SilkSeq[B]])(ev1: c.Expr[scala.reflect.ClassTag[A]], ev2: c.Expr[scala.reflect.ClassTag[B]]): c.Expr[SilkSeq[(A, B)]] = {
    import c.universe._

    val helper = new MacroHelper(c)
    val fref = helper.createFContext
    reify {
      JoinOp(fref.splice, c.prefix.splice.asInstanceOf[SilkSeq[A]], other.splice)(ev1.splice, ev2.splice)
    }
  }

  def reduceImpl[A](c: Context)(f: c.Expr[(A, A) => A]) = {
    newSingleOp[(A, A) => A, A](c)(c.universe.reify {
      ReduceOp
    }.tree, f)
  }


}