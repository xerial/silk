//--------------------------------------
//
// ObjectProjectorscala
// Since: 2013/04/03 5:28 PM
//
//--------------------------------------

package xerial.silk.index

import xerial.lens.{ObjectBuilder, ObjectSchema}

/**
 * @author Taro L. Saito
 */
object ObjectProjector {

  def projectedClass[A](cl:Class[A], params:Seq[String]) : Class[_] = {


    null
  }

  def project[A](a:A, params:Seq[String]) : A = {

    val schema = ObjectSchema(a.getClass)
    val b = ObjectBuilder(a.getClass)
    for(p <- params) {
      val v = schema.getParameter(p).get(a)
      b.set(p, v)
    }
    b.build
  }

}