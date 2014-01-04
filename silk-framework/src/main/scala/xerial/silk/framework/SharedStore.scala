//--------------------------------------
//
// SharedStore.scala
// Since: 2014/01/04 23:50
//
//--------------------------------------

package xerial.silk.framework

/**
 * Storing key-value data for sharing relatively small data set between cluster nodes
 *
 * @author Taro L. Saito
 */
trait SharedStoreComponent {

  type Store <: SharedStore
  val store : Store

  trait SharedStore {
    def exist(path:String) : Boolean
    def get(path:String) : Option[Array[Byte]]
    def apply(path:String) : Array[Byte]
    def update(path:String, data:Array[Byte]) : Unit
    def remove(path:String)
    def ls(path:String) : Seq[String]
  }

}

/**
 * In-memory implementation of SharedStoreComponent for debug purpose
 */
trait InMemorySharedStoreComponent extends SharedStoreComponent {

  type Store = InMemorySharedStore
  val store : Store = new InMemorySharedStore

  class InMemorySharedStore extends SharedStore {
    private val table = collection.mutable.Map[String, Array[Byte]]()
    def exist(path: String) = table.contains(path)
    def get(path: String) = table.get(path)
    def apply(path: String) = table(path)
    def update(path: String, data: Array[Byte]) = table.update(path, data)
    def remove(path: String) = table.remove(path)
    def ls(path:String) : Seq[String] = for{
      k <- table.keySet.toSeq
      if k.startsWith(path) && !k.substring(path.length).contains("/")
    } yield k

  }
}

