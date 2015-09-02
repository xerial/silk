//--------------------------------------
//
// ClosureCleaner.scala
// Since: 2013/11/06 15:00
//
//--------------------------------------

package xerial.silk.cluster.closure

import xerial.core.log.Logger
import xerial.silk.core.Silk
import scala.collection.GenTraversableOnce
import xerial.silk.framework.SilkTransformer

/**
 * @author Taro L. Saito
 */
object ClosureCleaner extends Logger {

  def clean[A](op:Silk[A]) : Silk[A] = {
    val t = new SilkTransformer {
      override def transformParam[A](param:A) = {
        param match {
          case s:GenTraversableOnce[_] => s.asInstanceOf[A]
          case f1:Function1[_, _] =>
            trace(s"cleanup ${f1.getClass}")
            ClosureSerializer.cleanupF1(f1).asInstanceOf[A]
          case _ => param
        }
      }
      def transformSilk[A](op:Silk[A]) = op
    }

    t.transform(op, isRecursive = false)
  }

}