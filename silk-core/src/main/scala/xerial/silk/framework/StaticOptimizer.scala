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
import xerial.silk.framework.ops.FilterOp
import xerial.silk.framework.ops.SortOp
import xerial.silk.framework.ops.MapOp
import xerial.lens.{ObjectSchema}
import xerial.core.log.Logger
import scala.reflect.ClassTag



trait StaticOptimizer extends Logger {

  /**
   * Transform the input operation to an optimized one. If no optimization is performed, it return the original operation.
   * @param op input
   */
  protected def transform(op:Silk[_]):Silk[_]


  /**
   * Recursively optimize the input operation.
   * @param op
   * @return
   */
  def optimize(op:Silk[_]):Silk[_] = {

    // Optimize the parent input operations, first

    // Retrieve the constructor type of the input operation
    val schema = ObjectSchema(op.getClass)
    val params = Array.newBuilder[AnyRef]

    // Optimize the Silk inputs
    for(p <- schema.constructor.params) {
      val param = p.get(op)
      val optimizedParam = param match {
        case s:Silk[_] => optimize(s)
        case other => other
      }
      params += optimizedParam.asInstanceOf[AnyRef]
    }
    // Populate ClassTag parameters that are needed in the constructor
    val classTagParamLength = schema.constructor.cl.getConstructors()(0).getParameterTypes.length - schema.constructor.params.length
    for(p <- 0 until classTagParamLength) {
      params += ClassTag.AnyRef
    }

    // Create a new instance of the input op whose input Silk nodes are optimized
    val paramsWithClassTags = params.result
    trace(s"params: ${paramsWithClassTags.mkString(", ")}")
    val optParent = schema.constructor.newInstance(paramsWithClassTags).asInstanceOf[Silk[_]]

    @tailrec
    def rTransform(a:Silk[_]):Silk[_] = {
      val t = transform(a)
      if (t eq a) a else rTransform(t)
    }

    // Optimize the leaf operation
    val opt = rTransform(optParent)
    opt
  }

}

/**
 * Optimizer that merges map-map chains
 */
class DeforestationOptimizer extends StaticOptimizer {

  private implicit class ToGeneric[A](f:A=>Boolean) {
    def toGen : Any => Boolean = f.asInstanceOf[Any=>Boolean]
  }

  protected def transform(op:Silk[_]):Silk[_] = op match {
    case MapOp(fc2, MapOp(fc1, in, f1), f2) =>
      MapOp(fc2, in, f1.andThen(f2))
    case FilterOp(fc2, FilterOp(fc1, in, f1), f2) =>
      FilterOp[Any](fc2, in, { v : Any => f1.toGen(v) && f2.toGen(v) })
    case _ => op
  }
}

/**
 * Optimizer that replace sorting operation to map-shuffle-reduce
 */
class ShuffleReduceOptimizer extends StaticOptimizer {

  protected def transform(op:Silk[_]) = op match {
    case SortOp(fc, in, ord, partitioner) =>
      val shuffler = ShuffleOp(fc, in, partitioner.asInstanceOf[Partitioner[Any]])
      val shuffleReducer = ShuffleReduceOp(op.id, fc, shuffler, ord.asInstanceOf[Ordering[Any]])
      shuffleReducer
    //case JoinOp(id, fc, a, b, ap, bp) =>
    //      HashJoin(.. )
    case _ => op
  }
}


class TakeHeadOptimizer extends StaticOptimizer {

  protected def transform(op:Silk[_]) = op match {
    case HeadOp(fc, CommandOutputLinesOp(id2, fc2, sc, args)) =>
      val p = sc.parts.asInstanceOf[Seq[String]]
      // TODO support Windows
      val l  = p.dropRight(1) :+ s"${p.last} | head -1"
      CommandOutputLinesOp(id2, fc2, new StringContext(l:_*), args)
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
