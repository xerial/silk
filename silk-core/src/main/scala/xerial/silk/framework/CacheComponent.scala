package xerial.silk.framework

import xerial.silk.mini.Guard
import java.util.UUID

/**
 * Cache for storing intermediate results
 */
trait CacheComponent extends SessionComponent {

  type Cache <: CacheAPI
  val cache : Cache

  trait CacheAPI {
    def getOrElseUpdate[A](op:Silk[A], result:ResultRef[A]) : ResultRef[A]
    def update[A](op:Silk[A], result:ResultRef[A])
    def remove[A](op:Silk[A])
    def clear : Unit
  }
}


/**
 * LocalCache implementation
 */
trait LocalCache extends CacheComponent {
  type Cache = CacheImpl
  val cache = new CacheImpl

  class CacheImpl extends CacheAPI with Guard
  {
    import collection.mutable
    private val table = mutable.Map[UUID, mutable.Map[UUID, ResultRef[_]]]()

    private def key[A](op:Silk[A]) = op.uuid

    private def resultTable: mutable.Map[UUID, ResultRef[_]] = table.getOrElseUpdate(session.sessionID, mutable.Map.empty)

    def getOrElseUpdate[A](op:Silk[A], result:ResultRef[A]) : ResultRef[A] = guard {
      resultTable.getOrElseUpdate(key(op), result).asInstanceOf[ResultRef[A]]
    }
    def update[A](op:Silk[A], result:ResultRef[A]) = guard {
      resultTable.update(key(op), result)
    }
    def remove[A](op:Silk[A]) = guard {
      resultTable.remove(key(op))
    }
    def clear : Unit = guard {
      resultTable.clear()
    }

  }
}

