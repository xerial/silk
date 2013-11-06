//--------------------------------------
//
// ClosureCleaner.scala
// Since: 2013/11/06 15:00
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.Silk
import xerial.lens.ObjectSchema
import scala.reflect.ClassTag
import xerial.silk.core.ClosureSerializer

/**
 * @author Taro L. Saito
 */
object ClosureCleaner {

  def clean[A](op:Silk[A]) : Silk[A] = {
    val t = new SilkTransformer {
      override def transformParam[A](param:A) = {
        param match {
          case f1:Function1[_, _] =>
            ClosureSerializer.cleanupF1(f1).asInstanceOf[A]
          case _ => param
        }
      }
      def transformSilk[A](op:Silk[A]) = op
    }

    t.transformOnce(op)
  }

}