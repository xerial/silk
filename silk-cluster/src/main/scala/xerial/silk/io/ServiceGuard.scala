package xerial.silk.io

import xerial.silk.SilkException


/**
 * A created service will be closed after the for-comprehension
 * {{{
 *   for(a <- service) {
 *     // do something
 *   } // service will be closed
 * }}}
 * @author Taro L. Saito
 */
trait ServiceGuard[Service] extends Iterable[Service] { self =>

  def close: Unit

  protected[silk] val service : Service

  def iterator = SilkException.NA
// new Iterator[Service] {
//    @transient var processed = false
//
//    def hasNext = !processed
//    def next() = {
//      if(hasNext) {
//        processed = true
//        service
//      }
//      else
//        throw new NoSuchElementException("next")
//    }
//  }

  private def wrap[R](f: Service => R) : R = {
    try {
      f(service)
    }
    finally
      close
  }

  //def map[B](f: Service => B) : B = wrap(f)
  //def flatMap[B](f:Service => ServiceGuard[B]) : ServiceGuard[B] = wrap(f)

  override def foreach[U](f:Service=>U) { wrap(f) }

  def whenMissing[B](f: => B) : self.type = { self }
}

class MissingService[Service] extends ServiceGuard[Service] { self =>
  def close {}
  protected[silk] val service : Service = null.asInstanceOf[Service]


  override def foreach[U](f:Service=>U) {
  // do nothing
  }

  override def whenMissing[B](f: => B) = { f; self }

}
