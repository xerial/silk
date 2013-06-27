package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.silk.cluster.SilkClient
import xerial.core.log.Logger
import xerial.silk.core.SilkSerializer
import xerial.silk.SilkException
import xerial.core.io.IOUtil
import java.net.URL
import xerial.larray.{LArray, MMapMode}
import xerial.silk.cluster.DataServer.{RawData, ByteData, MmapData}

/**
 * @author Taro L. Saito
 */
trait DistributedSliceStorage extends SliceStorageComponent {
  self: SilkFramework with DistributedCache with NodeManagerComponent with LocalClientComponent =>

  type LocalClient = SilkClient
  val sliceStorage = new SliceStorage

  class SliceStorage extends SliceStorageAPI with Logger {

    private def slicePath(op:Silk[_], index:Int) = {
      // TODO append session path: s"${session.sessionIDPrefix}/slice/${op.idPrefix}/${index}"
      s"slice/${op.idPrefix}/${index}"
    }

    private def stageInfoPath(op:Silk[_]) = {
      // TODO append session path: s"${session.sessionIDPrefix}/slice/${op.idPrefix}/${index}"
      s"slice/${op.idPrefix}/info"
    }

    def getStageInfo(op:Silk[_]) : Option[StageInfo] = {
      val p = stageInfoPath(op)
      cache.get(p).map(b => SilkSerializer.deserializeObj[StageInfo](b))
    }

    def setStageInfo(op:Silk[_], stageInfo:StageInfo) {
      val p = stageInfoPath(op)
      info(s"Set stage info: $p $stageInfo")
      cache.update(p, SilkSerializer.serializeObj(stageInfo))
    }

    def get(op: Silk[_], index: Int) : Future[Slice[_]] = {
      val p = slicePath(op, index)

      cache.getOrAwait(p).map{b =>
        if(b == null) {
          // when Slice is not available (reported by poke)
          throw SilkException.error(s"Failed to retrieve slice ${op.idPrefix}/$index")
        }
        else
          SilkSerializer.deserializeObj[Slice[_]](b)
      }
    }

    def poke(op:Silk[_], index: Int) {
      cache.update(slicePath(op, index), null)
    }

    def put(op: Silk[_], index: Int, slice: Slice[_], data:Seq[_]) {
      val path = s"${op.idPrefix}/${index}"
      debug(s"put slice $path")
      localClient.dataServer.registerData(path, data)
      cache.update(slicePath(op, index), SilkSerializer.serializeObj(slice))
    }

    def contains(op: Silk[_], index: Int) : Boolean = {
      cache.contains(slicePath(op, index))
    }

    def retrieve[A](op:Silk[A], slice: Slice[A]) = {
      val dataID = s"${op.idPrefix}/${slice.index}"
      if(slice.nodeName == localClient.currentNodeName) {
        debug(s"retrieve $dataID from local DataServer")
        SilkClient.client.flatMap { c =>
          c.dataServer.getData(dataID) map {
            case RawData(s, _) => s.asInstanceOf[Seq[_]]
            case ByteData(b, _) => SilkSerializer.deserializeObj[Seq[_]](b)
            case MmapData(file, _) => {
              val mmapped = LArray.mmap(file, 0, file.length, MMapMode.READ_ONLY)
              SilkSerializer.deserializeObj[Seq[A]](mmapped.toInputStream)
            }
          }
        } getOrElse { SilkException.error(s"no slice data is found: ${slice}") }
      }
      else {
        nodeManager.getNode(slice.nodeName).map { n =>
          val url = new URL(s"http://${n.address}:${n.dataServerPort}/data/${dataID}")
          debug(s"retrieve $dataID from $url")
          val result = IOUtil.readFully(url.openStream) { data =>
            SilkSerializer.deserializeObj[Seq[_]](data)
          }
          result
        } getOrElse { SilkException.error(s"invalid node name: ${slice.nodeName}") }
      }
    }
  }


}
