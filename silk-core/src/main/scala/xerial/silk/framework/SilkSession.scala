//--------------------------------------
//
// SilkSession.scala
// Since: 2013/06/16 9:27
//
//--------------------------------------

package xerial.silk.framework

import java.util.UUID
import java.nio.charset.Charset


/**
 * Session is a reference to the computed result of a Silk operation.
 *
 */
trait SessionComponent extends IDUtil with ProgramTreeComponent {
  self: SilkFramework  with CacheComponent =>

  type Session <: SessionAPI

  /**
   * Create a new session
   * @return
   */
  def newSession : Session = newSession("default")

  /**
   * Create a new session with a given name. This method is used for
   * repeatable evaluation of the code.
   */
  def newSession(name:String) : Session //= Session(UUID.nameUUIDFromBytes(name.getBytes(Charset.forName("UTF8"))), name)

  trait SessionAPI {
    thisSession =>
    def name: String
    def id: UUID

    /**
     * We would like to provide newSilk but it needs a macro-based implementation,
     * which cannot override the method defined in the interface as of Scala 2.10.2,
     * so this function must be implemented without using the inheritance.
     */
    // def newSilk[A](in:Seq[A]) : Silk[A]

    /**
     * Path to store Silk data
     * @param silk
     * @tparam A
     * @return
     */
    def pathOf[A](silk:Silk[A]) : String = s"/silk/session/${id.path}/${silk.idPrefix}"

    /**
     * Run the given Silk operation and return the result
     * @param silk
     * @return
     */
    def run[A](silk:Silk[A]) : ResultRef[A]

    /**
     * Run a specific target (val or function name) within a given silk operation.
     * @param silk
     * @param targetName
     * @tparam A
     * @return
     */
    def run[A](silk:Silk[_], targetName:String) : ResultRef[A] = {
      import ProgramTree._
      val matchingOps = find[A](silk, targetName)
      matchingOps match {
        case None => throw new IllegalArgumentException(s"target $targetName is not found")
        case Some(x) => run(x.asInstanceOf[Silk[A]])
      }
    }

    /**
     * Find an operation assigned to a given target variable name
     * @param from
     * @param varName
     * @tparam A
     * @return
     */
    def find[A](from:Silk[_], varName:String) : Option[Silk[A]] = {
      import ProgramTree._
      findTarget(from, varName) map (_.asInstanceOf[Silk[A]])
    }

    /**
     * Set or replace the result of the target silk
     * @param target
     * @param newSilk
     * @tparam A
     */
    def set[A](target:Silk[A], newSilk:Silk[A]) = {
      // TODO
    }

    /**
     * Clear the the result of the target silk and its dependent results
     * @param silk
     * @tparam A
     */
    def clear[A](silk:Silk[A]) = {
      import ProgramTree._
      for(desc <- descendantsOf(silk)) {
        cache.clear(pathOf(desc))
      }
    }
  }
}



trait SilkSessionComponent extends SessionComponent {
  self: SilkFramework with CacheComponent with ExecutorComponent =>

  type Session = SilkSession

  /**
   * Create a new session with a given name. This method is used for
   * repeatable evaluation of the code.
   */
  def newSession(name:String) : Session = SilkSession(UUID.nameUUIDFromBytes(name.getBytes(Charset.forName("UTF8"))), name)


  case class SilkSession(id:UUID, name:String) extends SessionAPI {

    /**
     * Run the given Silk operation and return the result
     * @param silk
     * @return
     */
    def run[A](silk: Silk[A]) : ResultRef[A] = {
      val p = pathOf(silk)
      executor.run(this, silk)
    }

  }

}



