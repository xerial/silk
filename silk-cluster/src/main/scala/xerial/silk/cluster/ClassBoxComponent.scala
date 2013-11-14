package xerial.silk.cluster

import java.util.UUID
import xerial.silk.Silk
import xerial.silk.cluster.store.DataServerComponent
import xerial.silk.framework.{CacheComponent, SerializationService, IDUtil, SilkFramework}


trait ClassBoxAPI {
  def classLoader : ClassLoader
}

/**
 * ClassBoxComponent has a role to provide the current ClassBoxID and distribute
 * the ClassBox to cluster nodes.
 */
trait ClassBoxComponent {
  self: SilkFramework =>

  type ClassBoxType <: ClassBoxAPI

  /**
   * Get the current class box id
   */
  def classBoxID : UUID

  /**
   * Retrieve the class box having the specified id
   * @param classBoxID
   * @return
   */
  def getClassBox(classBoxID:UUID) : ClassBoxAPI

}



trait ClassBoxComponentImpl extends ClassBoxComponent with IDUtil with SerializationService {
  self : SilkFramework
    with CacheComponent
    with DataServerComponent
  =>

  type ClassBoxType = ClassBox

  private[this] val classBoxTable = collection.mutable.Map[UUID, ClassBox]()

  private def classBoxPath(cbid:UUID) = s"classbox/${cbid.prefix}"


  def classBoxID : UUID = synchronized {
    val cbLocal = ClassBox.current
    classBoxTable.getOrElseUpdate(cbLocal.id, {
      val cb = ClassBox(Silk.localhost.address, dataServer.port, cbLocal.entries)
      // register (nodeName, cb) pair to the cache
      cache.update(classBoxPath(cb.id), cb.serialize)
      dataServer.register(cb)
      cb
    })
    cbLocal.id
  }

  /**
   * Retrieve the class box having the specified id
   * @param classBoxID
   * @return
   */
  def getClassBox(classBoxID:UUID) : ClassBox = synchronized {
    classBoxTable.getOrElseUpdate(classBoxID, {
      val path = classBoxPath(classBoxID)
      val remoteCb : ClassBox= cache.getOrAwait(path).map(_.deserialize[ClassBox]).get
      val cb = ClassBox.sync(remoteCb)
      // Register retrieved class box
      cache.getOrElseUpdate(classBoxPath(cb.id), cb.serialize)
      cb
    })
  }

}

