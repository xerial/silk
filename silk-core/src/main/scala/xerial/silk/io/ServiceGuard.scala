package xerial.silk.io

import xerial.silk.SilkException
import xerial.core.log.Logger


object ServiceGuard {

  def empty = new ServiceGuard[Int] {
    def close = {
      // do nothing
    }
    protected[silk] val service : Int = 0
  }

}

/**
 * A created service will be closed after the for-comprehension
 * {{{
 *   for(a <- service) {
 *     // do something
 *   } // service will be closed
 * }}}
 * @author Taro L. Saito
 */
trait ServiceGuard[Service] extends Iterable[Service] with Logger { self =>

  def close: Unit

  protected[silk] val service : Service

  def iterator = SilkException.NA

  def wrap[R](f: Service => R) : R = {
    try {
      f(service)
    }
    finally {
      close
    }
  }

  override def foreach[U](f:Service=>U) { wrap(f) }
//  def map[B](f:Service => B) = new ServiceGuard[B] {
//    override def wrap[R](g:B => R) : R = {
//      try {
//        f
//      }
//      finally {
//        self.close
//
//      }
//    }
//  }
//  def map[R](f:Service => R) : R = { wrap(f) }
//  def flatMap[R](f:Service => R) : ServiceGuard[R] = new ServiceGuard[R] {
//    override def
//  }

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
