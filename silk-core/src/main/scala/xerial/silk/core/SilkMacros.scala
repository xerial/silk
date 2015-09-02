package xerial.silk.core

import java.io.File

import scala.reflect.macros.blackbox.Context

/**
 *
 */
object SilkMacros {
  def mShellCommand(c: Context)(args: c.Tree*) = {
    import c.universe._
    q"ShellCommand(${fc(c)}, ${c.prefix.tree}.sc, Seq(..$args))"
  }

  /**
   * Generating a new InputFrame[A] from Seq[A]
   * @return
   */
  def mNewFrame[A: c.WeakTypeTag](c: Context)(in: c.Expr[Seq[A]]) = {
    import c.universe._
    q"InputFrame(${fc(c)}, $in)"
  }

  def mFileInput[A:c.WeakTypeTag](c:Context)(in:c.Expr[File[A]]) = {
    import c.universe._
    q"FileInput(${fc(c)}, $in})"
  }

  def mSQL(c: Context)(args: c.Tree*) = {
    import c.universe._
    q"RawSQL(${fc(c)}, ${c.prefix.tree}, Seq(..$args))"
  }

  def fc(c: Context) = new MacroHelper[c.type](c).createFContext

  def mAs[A: c.WeakTypeTag](c: Context) = {
    import c.universe._
    q"CastAs(${fc(c)}, ${c.prefix.tree})"
  }

  def mFilter[A: c.WeakTypeTag](c: Context)(condition: c.Tree) = {
    import c.universe._
    q"FilterOp(${fc(c)}, ${c.prefix.tree}, ${condition})"
  }

  def mSelect[A: c.WeakTypeTag](c: Context)(cols: c.Tree*) = {
    import c.universe._
    q"ProjectOp(${fc(c)}, ${c.prefix.tree}, Seq(..$cols))"
  }

  def mLimit[A: c.WeakTypeTag](c: Context)(rows: c.Tree) = {
    import c.universe._
    q"LimitOp(${fc(c)}, ${c.prefix.tree}, ${rows}, 0)"
  }

  def mLimitWithOffset[A: c.WeakTypeTag](c: Context)(rows: c.Tree, offset: c.Tree) = {
    import c.universe._
    q"LimitOp(${fc(c)}, ${c.prefix.tree}, ${rows}, ${offset})"
  }

  class MacroHelper[C <: Context](val c: C) {

    import c.universe._

    /**
     * Find a function/variable/class context where the expression is used
     * @return
     */
    def createFContext: c.Expr[FContext] = {
      // Find the enclosing method.
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decodedName.toString
        case other =>
          "<constructor>"
      }


      val selfCl = c.Expr[AnyRef](This(typeNames.EMPTY))
      val vd = findValDef
      var parent: c.Expr[Option[String]] = reify {
        None
      }
      val vdTree = if (vd.isEmpty) {
        val nme = c.literal(c.internal.enclosingOwner.name.decodedName.toString)
        reify {
          Some(nme.splice)
        }
      }
      else {
        if (!vd.tail.isEmpty) {
          parent = reify {
            Some(c.literal(vd.tail.head.name.decodedName.toString).splice)
          }
        }
        val nme = c.literal(vd.head.name.decodedName.toString)
        reify {
          Some(nme.splice)
        }
      }
      //      val vdTree = vd match {
      //        case Some(v) =>
      //          val nme = c.literal(v.name.decodedName.toString)
      //          reify { Some(nme.splice)}
      //        case None =>
      //          reify { None }
      //      }

      val pos = c.enclosingPosition
      c.Expr[FContext](q"FContext($selfCl.getClass, $methodName, $vdTree, $parent, ${pos.source.path}, ${pos.line}, ${pos.column})")
    }

    // Find a target variable of the operation result by scanning closest ValDefs
    def findValDef: List[ValOrDefDef] = {

      def print(p: c.Position) = s"${p.line}(${p.column})"

      val prefixPos = c.prefix.tree.pos

      class Finder
        extends Traverser {

        var enclosingDef: List[ValOrDefDef] = List.empty
        var cursor: c.Position = null

        private def contains(p: c.Position, start: c.Position, end: c.Position) =
          start.precedes(p) && p.precedes(end)

        override def traverse(tree: Tree): Unit = {
          if (tree.pos.isDefined) {
            cursor = tree.pos
          }
          tree match {
            // Check whether the rhs of variable definition contains the prefix expression
            case vd@ValDef(mod, varName, tpt, rhs) =>
              // Record the start and end positions of the variable definition block
              val startPos = vd.pos
              super.traverse(rhs)
              val endPos = cursor
              if (contains(prefixPos, startPos, endPos)) {
                enclosingDef = vd :: enclosingDef
              }
            case other =>
              super.traverse(other)
          }
        }

        def enclosingValDef = enclosingDef.reverse.headOption
      }

      val f = new
          Finder()
      val m = c.enclosingMethod
      if (m == null) {
        f.traverse(c.enclosingClass)
      }
      else {
        f.traverse(m)
      }
      f.enclosingDef.reverse
    }

    def createVDef[A: c.WeakTypeTag](op: c.Expr[_]) = {
      val fc = createFContext
      reify {
        val _prefix = c.prefix.splice.asInstanceOf[Frame[A]]
        val _fc = fc.splice
        val _cl = op.splice.getClass
      }
    }
  }

}
