package xerial.silk.collection

import collection.generic.CanBuildFrom

/**
 * @author Taro L. Saito
 */
object InMemorySilk {
  def apply[A](s:Seq[A]) = new InMemorySilk(s)

  def newBuilder[A] : collection.mutable.Builder[A, InMemorySilk[A]] = new InMemorySilkBuilder[A]

  class InMemorySilkBuilder[A] extends collection.mutable.Builder[A, InMemorySilk[A]] {
    private val b = Seq.newBuilder[A]

    def +=(elem: A) = {
      b += elem
      this
    }
    def clear() { b.clear }

    def result() = new InMemorySilk[A](b.result)
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
class InMemorySilk[A](elem:Seq[A]) extends Silk[A] with SilkLike[A] {
  def iterator = elem.iterator

  def newBuilder[T] = InMemorySilk.newBuilder[T]

}



