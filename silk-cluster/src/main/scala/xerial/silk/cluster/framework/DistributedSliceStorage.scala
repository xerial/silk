package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.core.log.Logger
import xerial.silk.core.SilkSerializer
import xerial.silk.{Silk, SilkException}
import xerial.core.io.IOUtil
import java.net.URL
import xerial.larray.{LArray, MMapMode}
import xerial.silk.cluster.DataServer.{RawData, ByteData, MmapData}
import java.util.UUID
import xerial.silk.cluster.{DataServerComponent, SilkClient}
import xerial.core.util.Timer
import java.io.BufferedInputStream

/**
 * @author Taro L. Saito
 */
trait DistributedSliceStorage extends SliceStorageComponent with IDUtil {
  self: SilkFramework
    with DistributedCache
    with DataServerComponent
    with NodeManagerComponent
    with LocalClientComponent =>

  val sliceStorage = new SliceStorage

  class SliceStorage extends SliceStorageAPI with Timer with Logger {

    private def slicePath(op:Silk[_], index:Int) = {
      // TODO append session path: s"${session.sessionIDPrefix}/slice/${op.idPrefix}/${index}"
      s"slice/${op.idPrefix}/${index}"
    }
    private def slicePath(opid:UUID, index:Int) = {
      // TODO append session path: s"${session.sessionIDPrefix}/slice/${op.idPrefix}/${index}"
      s"slice/${opid.prefix}/${index}"
    }

    private def partitionSlicePath(opid:UUID, partition:Int, index:Int) = {
      s"slice/${opid.prefix}/${partition}:${index}"
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
      info(s"Update: $p $stageInfo")
      cache.update(p, SilkSerializer.serializeObj(stageInfo))
    }

    def get(opid: UUID, index: Int) : SilkFuture[Slice] = {
      val p = slicePath(opid, index)

      cache.getOrAwait(p).map{b =>
        if(b == null) {
          // when Slice is not available (reported by poke)
          SilkException.error(s"Failed to retrieve slice ${opid.prefix}/$index")
        }
        else
          SilkSerializer.deserializeObj[Slice](b)
      }
    }

    def poke(opid:UUID, index: Int) {
      cache.update(slicePath(opid, index), null)
    }

    def poke(opid: UUID, partition: Int, index: Int) {
      cache.update(partitionSlicePath(opid, partition, index), null)
    }

    def put(opid: UUID, index: Int, slice: Slice, data:Seq[_]) {
      putRaw(opid, index, slice, SilkSerializer.serializeObj(data))
    }

    def putRaw(opid: UUID, index: Int, slice: Slice, data:Array[Byte]) {
      val path = s"${opid.prefix}/${index}"
      debug(s"put slice $path")
      dataServer.registerByteData(path, data)
      cache.update(slicePath(opid, index), SilkSerializer.serializeObj(slice))
    }



    def contains(op: Silk[_], index: Int) : Boolean = {
      cache.contains(slicePath(op, index))
    }

    def retrieve(opid:UUID, slice: Slice) = {
      val dataID = s"${opid.prefix}/${slice.path}"
      if(slice.nodeName == localClient.currentNodeName) {
        SilkClient.client.flatMap { c =>
          debug(s"retrieve $dataID from local DataServer: http://localhost:${c.dataServer.port}/data/${dataID}")
          val result : Option[Seq[_]] = c.dataServer.getData(dataID) map {
            case RawData(s, _) => s.asInstanceOf[Seq[_]]
            case ByteData(b, _) => SilkSerializer.deserializeObj[Seq[_]](b)
            case MmapData(file, _) => {
              val mmapped = LArray.mmap(file, 0, file.length, MMapMode.READ_ONLY)
              SilkSerializer.deserializeObj[Seq[_]](mmapped.toInputStream)
            }
          }
          debug(f"Retrieved ${result.get.size}%,d entries")
          result
        } getOrElse { SilkException.error(s"no slice data is found: [${opid.prefix}] ${slice}") }
      }
      else {
        nodeManager.getNode(slice.nodeName).map { n =>
          val url = new URL(s"http://${n.address}:${n.dataServerPort}/data/${dataID}")
          debug(s"retrieve $dataID from $url (${slice.nodeName})")
          val result = IOUtil.withResource(new BufferedInputStream(url.openStream)) { in =>
            val desr = SilkSerializer.deserializeObj[Seq[_]](in)
            debug(f"Deserialized ${desr.length}%,d entries")
            desr
          }
          result
        } getOrElse { SilkException.error(s"invalid node name: ${slice.nodeName}") }
      }
    }

    def putSlice(opid: UUID, partition: Int, index: Int, slice: Slice, data: Seq[_]) {
      val p = partitionSlicePath(opid, partition, index)
      val path = s"${opid.prefix}/${partition}:${index}"
      debug(s"put slice $path")
      dataServer.registerData(path, data)
      cache.update(p, SilkSerializer.serializeObj(slice))
    }

    def getSlice(opid: UUID, partition: Int, index: Int) = {
      val p = partitionSlicePath(opid, partition, index)
      cache.getOrAwait(p).map { b =>
        if(b == null)
          SilkException.error(s"Failed to retrieve partition slice ${opid.prefix}/$partition:$index")
        else
          SilkSerializer.deserializeObj[Slice](b)
      }
    }

  }


}
