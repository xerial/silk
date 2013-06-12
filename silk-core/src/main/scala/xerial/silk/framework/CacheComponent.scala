package xerial.silk.framework

import xerial.silk.mini.Guard
import java.util.UUID

/**
 * Cache for storing intermediate results
 */
trait CacheComponent {

  type Cache <: CacheAPI
  val cache : Cache

  trait CacheAPI {
    def getOrElseUpdate(path:String, data: => Array[Byte]) : Array[Byte]
    def update(path:String, data:Array[Byte])
    def remove(path:String)
    def clear(path:String) : Unit
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
    private val table = mutable.Map[String, Array[Byte]]()

    def getOrElseUpdate(path:String, data: => Array[Byte]) : Array[Byte] = guard {
      table.getOrElseUpdate(path, data)
    }
    def update(path:String, data:Array[Byte]) = guard {
      table.update(path, data)
    }
    def remove(path:String) = guard {
      table.remove(path)
    }
    def clear(path:String) : Unit = guard {
      val d = s"${path}/"
      val target = for(k <- table.keys if k.startsWith(d)) yield k
      for(p <- target)
        table.remove(p)
    }

  }
}

