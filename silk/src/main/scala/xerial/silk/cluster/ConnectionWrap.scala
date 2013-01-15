//--------------------------------------
//
// ConnectionWrap.scala
// Since: 2013/01/15 2:49 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.EmptyConnectionException

/**
 * Connection wrapper 
 *
 * {{{
 *   for(c <- ConnectionWrap(new YourClosableConnection)) yield {
 *      // do something
 *   }
 *   // a created connection will be closed after the for-comprehension
 * }}}
 *
 * @author Taro L. Saito
 */
trait ConnectionWrap[+Conn <: { def close: Unit } ] {
  def map[B](f: (Conn) => B) : B
  def flatMap[B](f: (Conn) => B) : Option[B]
  def foreach[U](f: (Conn) => U) : Unit
}


object ConnectionWrap {
  object EmptyConnection extends ConnectionWrap[Nothing] {
    def map[B](f: (Nothing) => B) = throw EmptyConnectionException
    def flatMap[B](f: (Nothing) => B) = None
    def foreach[U](f: (Nothing) => U) {}
  }

  def empty = EmptyConnection
  def apply[Conn <: { def close: Unit}](conn:Conn) : ConnectionWrap[Conn] = new ConnectionWrapImpl(conn)
}

class ConnectionWrapImpl[A <: { def close: Unit} ](connection:A) extends ConnectionWrap[A] {
  private def wrap[B](f: (A) => B) : B = {
    try {
      f(connection)
    }
    finally
      connection.close
  }
  def map[B](f: (A) => B) = wrap(f)
  def flatMap[B](f: (A) => B) = Some(wrap(f))
  def foreach[U](f: (A) => U) { wrap(f) }
}