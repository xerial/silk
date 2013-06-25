package xerial.silk.framework

import scala.language.higherKinds
import java.util.UUID


// Slice is an abstraction of distributed data set

// SilkSeq[A].map(f:A=>B) =>  SliceList(id, Slice[A]_1, ...)* =>  SliceList(id, Slice[B]_1, ...)* => SilkSeq[B]
// SilkSeq -> Slice* ->

case class Slice[+A](nodeName: String, index: Int)


case class SliceInfo(numSlices:Int)

case class SliceList[A](id:UUID, slices:Slice[A])







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
    def getSliceInfo(op:Silk[_]) : Option[SliceInfo]
    def setSliceInfo(op:Silk[_], si:SliceInfo) : Unit
    def put(op: Silk[_], index: Int, slice: Slice[_]): Unit
    def contains(op: Silk[_], index: Int): Boolean
    def retrieve[A](op:Silk[A], slice:Slice[A]) : Seq[A]
  }
}
