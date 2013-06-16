//--------------------------------------
//
// ProgramTree.scala
// Since: 2013/06/16 16:13
//
//--------------------------------------

package xerial.silk.framework.ops

import scala.reflect.ClassTag
import xerial.silk.mini.{FContext, SilkMini}
import xerial.lens.ObjectSchema
import xerial.core.log.Logger

case class FilterOp[A: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: A => Boolean, @transient fe: ru.Expr[A => Boolean])
  extends SilkMini[A](fref)
  with SplitOp[A => Boolean, A, A] {

}

case class FlatMapOp[A, B: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: A => SilkMini[B], @transient fe: ru.Expr[A => SilkMini[B]])
  extends SilkMini[B](fref)
  with SplitOp[A => SilkMini[B], A, B] {
  private[silk] def fwrap = f.asInstanceOf[Any => SilkMini[Any]]
}


case class MapOp[A, B: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: A => B, @transient fe: ru.Expr[A => B])
  extends SilkMini[B](fref)
  with SplitOp[A => B, A, B] {
  private[silk] def fwrap = f.asInstanceOf[Any => Any]
}
/**
 * @author Taro L. Saito
 */
case class RawSeq[+A: ClassTag](override val fref: FContext[_], @transient in:Seq[A])
  extends SilkMini[A](fref, newUUIDOf(in)) {

}

case class ReduceOp[A: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: (A, A) => A, @transient fe: ru.Expr[(A, A) => A])
  extends SilkMini[A](fref) {

  override def getFirstInput = Some(in)
  override def inputs = Seq(in)
}

/**
 * @author Taro L. Saito
 */
case class ShuffleOp[A: ClassTag, K](override val fref: FContext[_], in: SilkMini[A], keyParam: Parameter, partitioner: K => Int)
  extends SilkMini[A](fref)


/**
 * @author Taro L. Saito
 */
case class MergeShuffleOp[A: ClassTag, B: ClassTag](override val fref: FContext[_], left: SilkMini[A], right: SilkMini[B])
  extends SilkMini[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)
}

/**
 * @author Taro L. Saito
 */
case class JoinOp[A: ClassTag, B: ClassTag](override val fref: FContext[_], left: SilkMini[A], right: SilkMini[B])
  extends SilkMini[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)


  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}

case class DistributedSeq[+A: ClassTag](override val fref: FContext[_], slices: Seq[Slice[A]])
  extends SilkMini[A](fref) {

  override def slice[A1 >: A](ss:SilkSession) = slices
}





/**
 * @author Taro L. Saito
 */
trait SplitOp[F, P, A] extends Logger {
  self: SilkMini[A] =>


  val in: SilkMini[P]
  @transient val fe: ru.Expr[F]

  override def getFirstInput = Some(in)
  override def inputs = Seq(in)

  def functionClass: Class[Function1[_, _]] = {
    MacroUtil.mirror.runtimeClass(fe.staticType).asInstanceOf[Class[Function1[_, _]]]
  }

  @transient override val argVariable = {
    fe.tree match {
      case f@Function(List(ValDef(mod, name, e1, e2)), body) =>
        fe.staticType match {
          case TypeRef(prefix, symbol, List(from, to)) =>
            Some(ValType(name.decoded, from))
          case _ => None
        }
      case _ => None
    }
  }

  @transient override val freeVariables = {

    val fvNameSet = (for (v <- fe.tree.freeTerms) yield v.name.decoded).toSet
    val b = Seq.newBuilder[ValType]

    val tv = new Traverser {
      override def traverse(tree: ru.Tree) {
        def matchIdent(idt: Ident): ru.Tree = {
          val name = idt.name.decoded
          if (fvNameSet.contains(name)) {
            val tt: ru.Tree = MacroUtil.toolbox.typeCheck(idt, silent = true)
            b += ValType(idt.name.decoded, tt.tpe)
            tt
          }
          else
            idt
        }

        tree match {
          case idt@Ident(term) =>
            matchIdent(idt)
          case other => super.traverse(other)
        }
      }
    }
    tv.traverse(fe.tree)

    // Remove duplicate occurrences.
    b.result.distinct
  }

}
