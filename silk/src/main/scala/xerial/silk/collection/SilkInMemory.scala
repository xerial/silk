package xerial.silk.collection

import collection.generic.CanBuildFrom

/**
 * @author Taro L. Saito
 */
object SilkInMemory {
  def apply[A](s:Seq[A]) = new SilkInMemory(s)

  def newBuilder[A] : collection.mutable.Builder[A, SilkInMemory[A]] = new InMemorySilkBuilder[A]

  class InMemorySilkBuilder[A] extends collection.mutable.Builder[A, SilkInMemory[A]] {
    private val b = Seq.newBuilder[A]

    def +=(elem: A) = {
      b += elem
      this
    }
    def clear() { b.clear }

    def result() = new SilkInMemory[A](b.result)
  }


  class InMemorySilkCanBuildFrom[A, B, That] extends CanBuildFrom[A, B, Silk[B]] {
    def apply(from: A) = newBuilder
    def apply() = newBuilder
  }

}


/**
 * Silk data in memory
 * @param elem
 * @tparam A
 */
class SilkInMemory[A](elem:Seq[A]) extends Silk[A] with SilkLike[A] {
  override def toString = elem.mkString("[", ", ", "]")

  def iterator = elem.iterator

  def newBuilder[T] = SilkInMemory.newBuilder[T]
  def eval = this
}



