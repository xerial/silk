package xerial.silk.framework.memory

import scala.language.experimental.macros
import scala.language.higherKinds

import java.util.UUID
import xerial.silk.util.Guard
import xerial.silk._
import xerial.silk.framework._
import xerial.silk.core.IDUtil
import xerial.silk.framework.StageInfo
import xerial.silk.framework.Slice



trait InMemorySliceStorage extends SliceStorageComponent with IDUtil {
  self: Weaver =>

  val sliceStorage = new SliceStorageAPI with Guard {
    private val table = collection.mutable.Map[(UUID, Int), Slice]()
    private val futureToResolve = collection.mutable.Map[(UUID, Int), SilkFuture[Slice]]()

    private val infoTable = collection.mutable.Map[UUID, StageInfo]()
    private val sliceTable = collection.mutable.Map[(UUID, Int), Seq[_]]()

    def get(opid: UUID, index: Int): SilkFuture[Slice] = guard {
      val key = (opid, index)
      if (futureToResolve.contains(key)) {
        futureToResolve(key)
      }
      else {
        val f = new SilkFutureMultiThread[Slice]
        if (table.contains(key)) {
          f.set(table(key))
        }
        else
          futureToResolve += key -> f
        f
      }
    }

    def poke(opid: UUID, index: Int): Unit = guard {
      val key = (opid, index)
      if (futureToResolve.contains(key)) {
        futureToResolve(key).set(null)
        futureToResolve -= key
      }
    }

    def put(opid: UUID, index: Int, slice: Slice, data: Seq[_]) {
      guard {
        val key = (opid, index)
        if (!table.contains(key)) {
          table += key -> slice
          sliceTable += (opid, index) -> data
        }
        if (futureToResolve.contains(key)) {
          futureToResolve(key).set(slice)
          futureToResolve -= key
        }
      }
    }

    def putRaw(opid: UUID, index: Int, slice: Slice, data: Array[Byte]) {
      SilkException.NA
    }

    def contains(op: Silk[_], index: Int) = guard {
      val key = (op.id, index)
      table.contains(key)
    }
    def getStageInfo(opid: UUID) = guard {
      infoTable.get(opid)
    }

    def setStageInfo(opid: UUID, si: StageInfo) {
      guard {
        infoTable += opid -> si
      }
    }

    def retrieve(opid: UUID, slice: Slice) = {
      val key = (opid, slice.index)
      sliceTable(key).asInstanceOf[Seq[_]]
    }
    def poke(opid: UUID, partition: Int, index: Int) {
      SilkException.NA
    }
    def putSlice(opid: UUID, partition: Int, index: Int, Slice: Slice, data: Seq[_]) {
      SilkException.NA
    }
    def getSlice(opid: UUID, partition: Int, index: Int) = {
      SilkException.NA
    }

  }


}


