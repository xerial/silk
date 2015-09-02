package xerial.silk.core

import java.util.UUID

/**
 *
 */
trait Silk[A] {
  def id : UUID
  def inputs: Seq[Silk[_]]

  def name : String
  def summary : String
}

object Silk {
  val empty : Silk[Nothing] = null
}