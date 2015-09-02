//--------------------------------------
//
// SilkTransformer.scala
// Since: 2013/11/06 15:04
//
//--------------------------------------

package xerial.silk.core

import xerial.core.log.Logger
import xerial.lens.ObjectSchema

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Helper class to transform a Silk operation into another form.
 *
 * @author Taro L. Saito
 */
trait SilkTransformer extends Logger {

  /**
   * Transform the input operation. This operation should not traverse and transform the parents.
   * @param op
   * @tparam A
   * @return
   */
  def transformSilk[A](op:Silk[A]) : Silk[A]

  def transformParam[A](op:A) : A = op

  def transform[A](op:Silk[A], isRecursive:Boolean = false) : Silk[A] = {
    trace(s"Transform $op")
    // Get the object schema of the input Silk
    val sc = ObjectSchema(op.getClass)
    val params = Array.newBuilder[AnyRef]

    // Optimize the Silk inputs
    for(p <- sc.constructor.params) {
      val param = p.get(op)
      val transformed = param match {
        case s:Silk[_] => transform(s, isRecursive)
        case other => transformParam(other)
      }
      params += transformed.asInstanceOf[AnyRef]
    }

    // Populate ClassTag parameters that are needed in the constructor
    val classTagParamLength = sc.constructor.cl.getConstructors()(0).getParameterTypes.length - sc.constructor.params.length
    for(p <- 0 until classTagParamLength) {
      params += ClassTag.AnyRef
    }

    // Create a new instance of the input op whose input Silk nodes are optimized
    val paramsWithClassTags = params.result
    val transformed = sc.constructor.newInstance(paramsWithClassTags).asInstanceOf[Silk[A]]


    @tailrec
    def rTransform[B](a:Silk[B]):Silk[B] = {
      val t = transformSilk(a)
      if (t eq a) a else rTransform(t)
    }

    if(isRecursive)
      rTransform(transformed)
    else
      transformSilk(transformed)
  }

  /**
   * Repetitively transform the operation until transformOnce does not change it
   * @param op
   * @tparam A
   * @return
   */
  def transformRep[A](op:Silk[A]): Silk[A] = transform(op, true)


}