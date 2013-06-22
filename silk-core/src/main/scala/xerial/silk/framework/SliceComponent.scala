package xerial.silk.framework

import scala.language.higherKinds
import java.util.UUID


// Slice is an abstraction of distributed data set

// SilkSeq[A].map(f:A=>B) =>  SliceList(id, Slice[A]_1, ...)* =>  SliceList(id, Slice[B]_1, ...)* => SilkSeq[B]
// SilkSeq -> Slice* ->

abstract class Slice[+A](val nodeName: String, val index: Int) {
  def data: Seq[A]
}

case class SliceList[A](id:UUID, slices:Slice[A])


//case class RawSlice[A](override val nodeName: String, override val index: Int, data: Seq[A]) extends Slice[A](nodeName, index)




/**
 * @author Taro L. Saito
 */
trait SliceComponent {

  self:SilkFramework =>

  /**
   * Slice might be a future
   * @tparam A
   */
  type Slice[A] <: SliceAPI[A]

  trait SliceAPI[A] {
    def index: Int
    def data: Seq[A]
  }


}




/**
 * @author Taro L. Saito
 */
trait SliceStorageComponent extends SliceComponent {
  self: SilkFramework =>

  val sliceStorage: SliceStorageAPI

  trait SliceStorageAPI {
    def get(op: Silk[_], index: Int): Future[Slice[_]]
    def put(op: Silk[_], index: Int, slice: Slice[_]): Unit
    def contains(op: Silk[_], index: Int): Boolean
  }
}
