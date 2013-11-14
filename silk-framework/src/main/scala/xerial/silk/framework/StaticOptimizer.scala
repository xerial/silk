//--------------------------------------
//
// StaticOptimizer.scala
// Since: 2013/10/17 18:52
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.framework.core._
import xerial.silk._
import scala.annotation.tailrec
import xerial.silk.framework.core.ShuffleReduceOp
import xerial.silk.framework.core.ShuffleOp
import xerial.silk.framework.core.FilterOp
import xerial.silk.framework.core.SortOp
import xerial.silk.framework.core.MapOp
import xerial.lens.{ObjectSchema}
import xerial.core.log.Logger
import scala.reflect.ClassTag
import xerial.silk.core.Partitioner


trait StaticOptimizer extends SilkTransformer with Logger {

  /**
   * Transform the input operation to an optimized one. If no optimization is performed, it return the original operation.
   * @param op input
   */
  protected def transform(op:Silk[_]):Silk[_]

  override def transformSilk[A](op:Silk[A]) : Silk[A] = {
    val newOp = transform(op).asInstanceOf[Silk[A]]
    if(!(op eq newOp))
      trace(s"optimized:\n$op to\n$newOp")
    newOp
  }

  /**
   * Recursively optimize the input operation.
   * @param op
   * @return
   */
  def optimize(op:Silk[_]):Silk[_] = transformRep(op)
}

/**
 * Optimizer that merges map-map chains
 */
class DeforestationOptimizer extends StaticOptimizer {

  private implicit class ToGeneric[A](f:A=>Boolean) {
    def toGen : Any => Boolean = f.asInstanceOf[Any=>Boolean]
  }

  protected def transform(op:Silk[_]):Silk[_] = op match {
    case MapOp(id2, fc2, MapOp(id1, fc1, in, f1), f2) =>
      MapOp(id2, fc2, in, f1.andThen(f2))
    case FilterOp(id2, fc2, FilterOp(id1, fc1, in, f1), f2) =>
      FilterOp[Any](id2, fc2, in, { v : Any => f1.toGen(v) && f2.toGen(v) })
    case FilterOp(id2, fc2, MapOp(id1, fc1, in, f1), f2) =>
      MapFilterOp(id2, fc2, in, f1, f2)
    case FilterOp(id2, fc2, FlatMapSeqOp(id1, fc1, in, f1), f2) =>
      FlatMapFilterOp(id2, fc2, in, f1, f2)
    case _ => op
  }
}



/**
 * Optimizer that replace sorting operation to map-shuffle-reduce
 */
class ShuffleReduceOptimizer extends StaticOptimizer {

  protected def transform(op:Silk[_]) = op match {
    case SortOp(id, fc, in, ord, partitioner) =>
      val shuffler = ShuffleOp(SilkUtil.newUUIDOf(classOf[ShuffleOp[_]], fc, in), fc, in, partitioner.asInstanceOf[Partitioner[Any]])
      val shuffleReducer = ShuffleReduceSortOp(id, fc, shuffler, ord.asInstanceOf[Ordering[Any]])
      shuffleReducer
    //case JoinOp(id, fc, a, b, ap, bp) =>
    //      HashJoin(.. )
    case _ => op
  }
}


class TakeHeadOptimizer extends StaticOptimizer {

  protected def transform(op:Silk[_]) = op match {
    case HeadOp(id1, fc, CommandOutputLinesOp(id2, fc2, sc, args)) =>
      val p = sc.parts.asInstanceOf[Seq[String]]
      // TODO support Windows
      val l  = p.dropRight(1) :+ s"${p.last} | head -1"
      CommandOutputLinesOp(id1, fc2, new StringContext(l:_*), args)
    case _ => op
  }
}


class PushingDownSelectionOptimizer extends StaticOptimizer {
  protected def transform(op:Silk[_]) = op match {
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

