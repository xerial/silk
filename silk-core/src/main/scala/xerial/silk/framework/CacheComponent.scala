package xerial.silk.framework

import xerial.silk.util.Guard


/**
 * Cache for storing intermediate results
 */
trait CacheComponent {

  type Cache <: CacheAPI
  val cache : Cache

}

trait CacheAPI {
  def getOrAwait(path:String) : SilkFuture[Array[Byte]]
  def getOrElseUpdate(path:String, data: => Array[Byte]) : Array[Byte]
  def contains(path:String) : Boolean
  def get(path:String) : Option[Array[Byte]]
  def update(path:String, data:Array[Byte])
  def remove(path:String)
  def clear(path:String) : Unit
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
    private val table = mutable.Map[String, Array[Byte]]()
    private val futureTable = mutable.Map[String, SilkFuture[Array[Byte]]]()

    def contains(path:String) : Boolean = guard {
      table.contains(path)
    }

    def get(path:String) = guard {
      table.get(path)
    }

    def getOrAwait(path: String) = guard {
      if(!table.contains(path)) {
        new SilkFutureMultiThread(Some(table(path)))
      }
      else {
        val f = new SilkFutureMultiThread(Some(table(path)))
        futureTable += path -> f
        f
      }
    }

    private def removeFuture(path:String, data:Array[Byte]) {
      futureTable.remove(path).map(_.set(data))
    }

    def getOrElseUpdate(path:String, data: => Array[Byte]) : Array[Byte] = guard {
      table.getOrElseUpdate(path, {
        removeFuture(path, data)
        data
      })
    }
    def update(path:String, data:Array[Byte]) = guard {
      removeFuture(path, data)
      table.update(path, data)
    }
    def remove(path:String) = guard {
      removeFuture(path, null)
      table.remove(path)
    }
    def clear(path:String) : Unit = guard {
      val d = s"${path}/"
      val target = for(k <- table.keys if k.startsWith(d)) yield k
      for(p <- target) {
        removeFuture(path, null)
        table.remove(p)
      }
    }

  }
}

