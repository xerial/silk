//--------------------------------------
//
// ConnectionWrap.scala
// Since: 2013/01/15 2:49 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.EmptyConnectionException
import scala.Iterator
import collection.generic.CanBuildFrom
import scala.language.reflectiveCalls

/**
 * Connection wrapper 
 *
 * {{{
 *   for(c <- ConnectionWrap(new YourClosableConnection)) yield {
 *      // do something using c
 *   }
 *   // a created connection will be closed after the for-comprehension
 * }}}
 *
 * @author Taro L. Saito
 */
trait ConnectionWrap[+Conn <: { def close: Unit } ] {
  def map[B, That](f: (Conn) => B)(implicit bf:CanBuildFrom[Seq[Conn], B, That]) : That
  def flatMap[B](f: (Conn) => B) : Option[B]
  def foreach[U](f: (Conn) => U) : Unit
  def whenMissing[B](f: => B) : ConnectionWrap[Conn]
}


object ConnectionWrap {

  object EmptyConnection extends ConnectionWrap[Nothing] {
    def map[B, That](f: (Nothing) => B)(implicit bf:CanBuildFrom[Seq[Nothing], B, That]) = throw EmptyConnectionException
    def flatMap[B](f: (Nothing) => B) = None
    def foreach[U](f: (Nothing) => U) {}
    def whenMissing[B](f: => B) = { f; this }
  }

  private class ConnectionWrapImpl[A <: { def close: Unit} ](connection:A) extends ConnectionWrap[A] {
    private def wrap[B](f: (A) => B) : B = {
      try {
        f(connection)
      }
      finally
        connection.close
    }
    def map[B, That](f: (A) => B)(implicit bf:CanBuildFrom[Seq[A], B, That]) = {
      val b = bf()
      b += wrap(f)
      b.result
    }
    def flatMap[B](f: (A) => B) = Some(wrap(f))
    def foreach[U](f: (A) => U) { wrap(f) }
    def whenMissing[B](f: => B) = { this }
  }

  def empty = EmptyConnection
  def apply[Conn <: { def close: Unit}](conn:Conn) : ConnectionWrap[Conn] = new ConnectionWrapImpl(conn)
}

