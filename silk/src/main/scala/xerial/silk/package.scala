package xerial

import silk.core.{SilkInMemory, Silk}

/**
 * @author Taro L. Saito
 */
package object silk {
  class SilkWrap[A](a:A) {
    def save = {
      // do something to store Silk data
    }
  }

  class SilkArrayWrap[A](a:Array[A]) {
    def toSilk : Silk[A] = {
      // TODO impl
      null
    }
  }

  class SilkSeqWrap[A](a:Seq[A]) {
    def toSilk : Silk[A] = SilkInMemory[A](a)
  }



  implicit def wrapAsSilk[A](a:A) = new SilkWrap(a)
  implicit def wrapAsSilkArray[A](a:Array[A]) = new SilkArrayWrap(a)
  implicit def asSilkSeq[A](a:Seq[A]) = new SilkSeqWrap(a)
  //implicit def wrapAsSilkSeq[A](a:Array[A]) = new SilkSeqWrap(a)


}
