package xerial.silk.framework

import scala.language.higherKinds
import java.util.{Date, UUID}
import xerial.silk.Silk
import java.text.DateFormat


/**
 * Slice is a data unit of distributed data set
 * @param nodeName node name where this slice data is stored.
 * @param keyIndex used for partitioning. If this value -1, no key is used
 * @param index index of this slice
 */
case class Slice(nodeName: String, keyIndex:Int, index: Int, numEntries:Long) {
  def path = if(keyIndex == -1) s"$index" else s"$keyIndex:$index"
}


sealed trait StageStatus {
  def isFailed : Boolean = false
  def timeStamp : Long

  protected def className = s"${this.getClass.getSimpleName.replace("$", "")}"

  override def toString = s"${className}(${DateFormat.getDateTimeInstance.format(date)})"
  def date = new Date(timeStamp)
}
case class StageStarted(timeStamp:Long) extends StageStatus
case class StageFinished(timeStamp:Long) extends StageStatus
case class StageAborted(cause:String, timeStamp:Long) extends StageStatus {
  override def toString = s"${className}(${DateFormat.getDateTimeInstance.format(date)}, $cause)"
  override def isFailed = true
}
case class StageInfo(numKeys:Int, numSlices:Int, status:StageStatus) {
  def isFailed = status.isFailed
}


trait SliceStorageAPI {
  def get(opid: UUID, index: Int): SilkFuture[Slice]
  def poke(opid: UUID, index: Int)
  def poke(opid: UUID, partition:Int, index: Int)
  def getStageInfo(op:Silk[_]) : Option[StageInfo]
  def setStageInfo(op:Silk[_], si:StageInfo) : Unit
  def put(opid: UUID, index: Int, slice: Slice, data:Seq[_]): Unit
  def putRaw(opid: UUID, index: Int, slice: Slice, data:Array[Byte]): Unit

  def putSlice(opid:UUID, partition:Int, index:Int, Slice:Slice, data:Seq[_]) : Unit
  def getSlice(opid:UUID, partition:Int, index:Int) : SilkFuture[Slice]

  def contains(op: Silk[_], index: Int): Boolean
  def retrieve(opid:UUID, slice:Slice) : Seq[_]
}


/**
 * @author Taro L. Saito
 */
trait SliceStorageComponent {
  self: SilkFramework =>

  val sliceStorage: SliceStorageAPI

}
