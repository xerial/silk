package xerial.silk.framework

import scala.language.higherKinds

// Slice is an abstraction of distributed data set

// SilkOps[A].map(f:A=>B) =>  f(Slice[A]_1, ...)* =>  Slice[B]_1, ... => SilkMini[B]
// SilkOps -> Slice* ->

abstract class Slice[+A](val host: Host, val index: Int) {
  def data: Seq[A]
}
case class RawSlice[A](override val host: Host, override val index: Int, data: Seq[A]) extends Slice[A](host, index)

/**
 * Partitioned slice has the same structure with RawSlice.
 * @param host
 * @param index
 * @param data
 * @tparam A
 */
case class PartitionedSlice[A](override val host: Host, override val index: Int, data: Seq[A]) extends Slice[A](host, index) {
  def partitionID = index
}



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
