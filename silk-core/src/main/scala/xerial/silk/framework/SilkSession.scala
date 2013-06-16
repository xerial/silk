//--------------------------------------
//
// SilkSession.scala
// Since: 2013/06/16 9:27
//
//--------------------------------------

package xerial.silk.framework

import java.util.UUID
import xerial.core.log.Logger
import scala.reflect.ClassTag
import xerial.silk.mini.{Slice, SilkMini}
import xerial.silk.SilkException


/**
 * Session manages already computed data.
 */
trait SessionComponent extends IDUtil with ProgramTreeComponent {
  self: SilkFramework
    with CacheComponent
    with ExecutorComponent =>

  type Session <: SessionAPI

  /**
   * Create a new session
   * @return
   */
  def newSession : Session

  /**
   * Create a new session with a given name. This method is used for
   * repeatable evaluation of the code.
   */
  def newSession(name:String)

  trait SessionAPI {
    session =>

    def id : UUID

    /**
     * We would like to provide newSilk but it needs a macro-based implementation,
     * which cannot be used to override the method defined in the interface as of Scala 2.10.2,
     * so this function must be implmented without using inheritance.
     */
    // def newSilk[A](in:Seq[A]) : Silk[A]

    /**
     * Path to store session data
     * @param op
     * @tparam A
     */
    def pathOf[A](op:Silk[A]) : String


    /**
     * Get the result of an silk
     * @param op
     * @tparam A
     * @return
     */
    def get[A](op:Silk[A]) : ResultRef[A] = {
      val path = pathOf(op)
      if(cache.contains(path))
        cache.get(path)

    }

    /**
     * Get the result of the target in the given silk
     * @param op
     * @param target
     * @tparam A
     * @return
     */
    def get[A](op:Silk[A], target:String) : ResultRef[_] = {
      import ProgramTree._
      findTarget(op, target) match {
        case Some(x) => get(x)
        case None => throw new IllegalArgumentException(s"target $target is not found")
      }
    }

    /**
     * Set or replace the result of the target silk
     * @param op
     * @param result
     * @tparam A
     */
    def set[A](op:Silk[A], result:ResultRef[A])

    /**
     * Clear the the result of the target silk and its dependent results
     * @param op
     * @tparam A
     */
    def clear[A](op:Silk[A])

    /**
     * Run the given Silk operation and return the result
     * @param silk
     * @return
     */
    def run[A](silk: Silk[A]): Result[A]

    /**
     * Run a specific target (val or function name) used in a given silk operation.
     * @param silk
     * @param targetName
     * @tparam A
     * @return
     */
    def run[A](silk:Silk[_], targetName:String) : Result[A] = {
      import ProgramTree._

      val matchingOps = findTarget(silk, targetName)
      matchingOps match {
        case None => throw new IllegalArgumentException(s"target $targetName is not found")
        case Some(x) => session.run(x.asInstanceOf[Silk[A]])
      }
    }
  }

}

/**
 * A standard implementation of SilkSessionComponent
 */
trait StandardSessionImpl
  extends SessionComponent
  with ProgramTreeComponent
{
   self: SilkFramework with CacheComponent =>

  type Session = SessionImpl

  class SessionImpl(val id:UUID) extends SessionAPI {
    def get[A](op: Silk[A]) = {
      cache.contains()

    }
    def get[A](op: Silk[A], target: String) = ???
    def set[A](op: Silk[A], result: ResultRef[A]) {}
    def clear[A](op: Silk[A]) {}
    def run[A](silk: Silk[A]) = ???
  }
}


