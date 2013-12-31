package xerial.silk.cluster

import java.util.UUID
import xerial.silk.cluster.store.DataServerComponent
import xerial.silk.framework.{HomeConfig, CacheComponent, SerializationService}
import xerial.silk.core.IDUtil
import java.io._
import xerial.silk.io.Digest
import xerial.core.log.Logger
import java.net.URL
import xerial.core.io.IOUtil

/**
 * ClassBoxComponent has a role to provide the current ClassBoxID and distribute
 * the ClassBox to cluster nodes.
 */
trait ClassBoxComponent {
  self: ClusterWeaver
    with CacheComponent
    with DataServerComponent
  =>

  val classBox = new ClassBoxUtil

  class ClassBoxUtil extends IDUtil with SerializationService with Logger {

    private[this] val classBoxTable = collection.mutable.Map[UUID, ClassBox]()
    import ClassBox._

    /**
     * Current context class box
     */
    lazy val current : ClassBox = ClassBox.getCurrent(config.home.silkTmpDir, config.cluster.dataServerPort)

    private def classBoxPath(cbid:UUID) = s"classbox/${cbid.prefix}"

    /**
     * Get the current class box id
     */
    def classBoxID : UUID = synchronized {
      val cbLocal = current
      classBoxTable.getOrElseUpdate(cbLocal.id, {
        val cb = ClassBox(SilkCluster.localhost.address, dataServer.port, cbLocal.entries)
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
        val cb = sync(remoteCb)
        // Register retrieved class box
        cache.getOrElseUpdate(classBoxPath(cb.id), cb.serialize)
        cb
      })
    }


    import IOUtil._

    /**
     * Check all jar entries in the given ClassBox. If there is missing jars,
     * retrieve them from the host.
     *
     * TODO: caching the results
     *
     * @return
     */
    def sync(cb:ClassBox) : ClassBox = {
      info(s"synchronizing ClassBox ${cb.id.prefix} at ${cb.urlPrefix}")

      val s = Seq.newBuilder[ClassBox.ClassBoxEntry]
      var hasChanged = false
      for(e <- cb.entries) {
        e match {
          case JarEntry(path, sha1sum, lastModified) =>
            val f = new File(path.getPath)
            if(!f.exists || e.sha1sum != Digest.sha1sum(f)) {

              // Jar file is not present in this machine.
              val jarURL = new URL(s"${cb.urlPrefix}/${e.sha1sum}")
              val jarFile = localJarPath(config.home.silkTmpDir, e.sha1sum)
              jarFile.deleteOnExit()

              withResource(new BufferedOutputStream(new FileOutputStream(jarFile))) { out =>
                debug(s"Connecting to ${jarURL}")
                withResource(jarURL.openStream) { in =>
                  val buf = new Array[Byte](8192)
                  var readBytes = 0
                  while({ readBytes = in.read(buf); readBytes != -1}) {
                    out.write(buf, 0, readBytes)
                  }
                }
              }
              s += ClassBox.JarEntry(jarFile.toURI.toURL, sha1sum, lastModified)
              hasChanged = true
            }
            else
              s += e
          case LocalPathEntry(path, sha1sum) => {
            s += e // do nothing for local paths
          }
        }
      }

      info(s"done.")
      if(hasChanged)
        new ClassBox(SilkCluster.localhost.address, config.cluster.dataServerPort, s.result)
      else
        cb
    }

  }

}


