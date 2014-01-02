package xerial.silk.core

import scala.collection.GenTraversable
import xerial.silk.SilkSeq

/**
 * Utilities to wrap functions in a generic form
 * @author Taro L. Saito
 */
trait FunctionWrap {

  implicit class toGenFun[A, B](f: A => B) {
    def toF1: Any => Any = f.asInstanceOf[Any => Any]

    def toFlatMap: Any => SilkSeq[Any] = f.asInstanceOf[Any => SilkSeq[Any]]
    def tofMap: Any => GenTraversable[Any] = f.asInstanceOf[Any => GenTraversable[Any]]
    def toFilter: Any => Boolean = f.asInstanceOf[Any => Boolean]
  }

  implicit class toGenFMap[A, B](f: A => GenTraversable[B]) {
    def toFmap = f.asInstanceOf[Any => GenTraversable[Any]]
  }

  implicit class toAgg[A, B, C](f: (A, B) => C) {
    def toAgg = f.asInstanceOf[(Any, Any) => Any]
  }

  implicit class toFMapRes[A, B, C](f: (A, B) => GenTraversable[C]) {
    def toFmapRes = f.asInstanceOf[(Any, Any) => GenTraversable[Any]]
  }

}
