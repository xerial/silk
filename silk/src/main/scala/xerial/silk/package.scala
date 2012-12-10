package xerial

/**
 * @author Taro L. Saito
 */
package object silk {
  class SilkWrap[A](a:A) {
    def save = {
      // do something to store Silk data
    }
  }

  implicit def wrapAsSilk[A](a:A) = new SilkWrap(a)
}
