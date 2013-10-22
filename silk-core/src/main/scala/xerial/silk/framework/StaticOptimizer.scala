//--------------------------------------
//
// StaticOptimizer.scala
// Since: 2013/10/17 18:52
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.framework.ops._
import xerial.silk._
import scala.annotation.tailrec
import xerial.silk.framework.ops.ShuffleReduceOp
import xerial.silk.framework.ops.ShuffleOp
import xerial.silk.framework.ops.JoinOp
import xerial.silk.framework.ops.FilterOp
import xerial.silk.framework.ops.FlatMapOp
import xerial.silk.framework.ops.SortOp
import xerial.silk.framework.ops.MapOp


object StaticOptimizer {
  implicit class ToSeq(val op:Silk[_]) extends AnyVal {
    def asSeq = if (!op.isSingle) op.asInstanceOf[SilkSeq[_]] else SilkException.error(s"illegal conversion: ${op}")
  }

}


trait StaticOptimizer {

  import StaticOptimizer._

  /**
   * Transform the input operation to an optimized one. If no optimization is performed, it return the original operation.
   * @param op input
   */
  def transform(op:Silk[_]):Silk[_]

  /**
   * Recursively optimize the input operation.
   * @param op
   * @return
   */
  def optimize(op:Silk[_]):Silk[_] = {

    @tailrec
    def rTransform(a:Silk[_]):Silk[_] = {
      val t = transform(a)
      if (t eq a) a else rTransform(t)
    }

    // Optimize the leaf operation
    val opt = rTransform(op)

    // Optimize the parent operations
    opt match {
      case MapOp(id, fc, in, f) =>
        MapOp(id, fc, rTransform(in).asSeq, f)
      case FlatMapOp(id, fc, in, f) =>
        FlatMapOp(id, fc, rTransform(in).asSeq, f)
      case FilterOp(id, fc, in, f) =>
        FilterOp(id, fc, rTransform(in).asSeq, f)
      // TODO add transformation for the other operations
      case JoinOp(id, fc, left, right, k1, k2) =>
        JoinOp(id, fc, rTransform(left).asSeq, rTransform(right).asSeq, k1, k2)
      case _ => opt
    }
  }

}

class DeforestationOptimizer extends StaticOptimizer {

  private implicit class ToGeneric[A](f:A=>Boolean) {
    def toGen : Any => Boolean = f.asInstanceOf[Any=>Boolean]
  }

  def transform(op:Silk[_]):Silk[_] = op match {
    case MapOp(id2, fc2, MapOp(id1, fc1, in, f1), f2) =>
      MapOp(id2, fc2, in, f1.andThen(f2))
    case FilterOp(id2, fc2, FilterOp(id1, fc1, in, f1), f2) =>
      FilterOp[Any](id2, fc2, in, { v : Any => f1.toGen(v) && f2.toGen(v) })
    case _ => op
  }
}

class ShuffleReduceOptimizer extends StaticOptimizer {

  def transform(op:Silk[_]) = op match {
    case SortOp(id, fc, in, ord, partitioner) =>
      val shuffler = ShuffleOp(SilkUtil.newUUID, fc, in, partitioner.asInstanceOf[Partitioner[Any]])
      val shuffleReducer = ShuffleReduceOp(id, fc, shuffler, ord.asInstanceOf[Ordering[Any]])
      shuffleReducer
    case _ => op
  }
}


class PushingDownSelectionOptimizer extends StaticOptimizer {
  def transform(op:Silk[_]) = op match {
    /**
     * TODO Utilize columnar DB access.
     * If the filter operation on object A only touches a certain set of paremeters,
     * read only these parameters from the DB, then extract other parameters.
     *
     * If the target data is stored in SQL DB, use 'where' clause to access the data.
     */
    //    case FilterOp(id2, fc2, LoadBlock(id, fc, blockID), f) =>
    //      LoadBlockWithFilter(id, fc, blockID, f)
    case _ => op
  }
}
