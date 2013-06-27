package xerial.silk.framework

import scala.language.higherKinds
import java.util.UUID


// Slice is an abstraction of distributed data set

// SilkSeq[A].map(f:A=>B) =>  SliceList(id, Slice[A]_1, ...)* =>  SliceList(id, Slice[B]_1, ...)* => SilkSeq[B]
// SilkSeq -> Slice* ->

case class Slice[+A](nodeName: String, index: Int)

case class SliceList[A](id:UUID, slices:Slice[A])


sealed trait StageStatus {
  def isFailed : Boolean = false
}
case class StageStarted(timeStamp:Long) extends StageStatus
case class StageFinished(timeStamp:Long) extends StageStatus
case class StageAborted(cause:String, timeStamp:Long) extends StageStatus {
  override def isFailed = true
}
case class StageInfo(numSlices:Int, status:StageStatus) {
  def isFailed = status.isFailed
}



/**
 * @author Taro L. Saito
 */
trait SliceComponent {

  self:SilkFramework =>


}




/**
 * @author Taro L. Saito
 */
trait SliceStorageComponent extends SliceComponent {
  self: SilkFramework =>

  val sliceStorage: SliceStorageAPI

  trait SliceStorageAPI {
    def get(op: Silk[_], index: Int): Future[Slice[_]]
    def poke(op: Silk[_], index: Int)
    def getStageInfo(op:Silk[_]) : Option[StageInfo]
    def setStageInfo(op:Silk[_], si:StageInfo) : Unit
    def put(op: Silk[_], index: Int, slice: Slice[_], data:Seq[_]): Unit
    def contains(op: Silk[_], index: Int): Boolean
    def retrieve[A](op:Silk[A], slice:Slice[A]) : Seq[_]
  }
}
