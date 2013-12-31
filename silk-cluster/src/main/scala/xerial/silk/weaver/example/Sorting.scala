//--------------------------------------
//
// Sorting.scala
// Since: 2013/11/22 11:53 AM
//
//--------------------------------------

package xerial.silk.weaver.example

import xerial.lens.cui.{option, command}
import xerial.silk.{Silk}
import scala.util.Random
import xerial.core.log.Logger
import xerial.core.util.Timer
import xerial.silk.framework.RangePartitioner
import xerial.silk.weaver.Weaver

/**
 * @author Taro L. Saito
 */
class Sorting(val env:Weaver) extends Logger with Timer {

  implicit val e = env

  @command(description = "Sort data set")
  def sort(@option(prefix = "-N", description = "num entries")
           N: Int = 1024 * 1024,
           @option(prefix = "-m", description = "num mappers")
           M: Int = 10,
           @option(prefix = "-r", description = "num reducers")
           numReducer: Int = 3,
           forceEval : Boolean = true
            ) = {

    info("Preparing random data")
    val B = (N.toDouble / M).ceil.toInt

    info(f"N=$N%,d, B=$B%,d, M=$M")
    val seed = Silk.scatter((0 until M).toIndexedSeq, M)
    val seedf = if(forceEval) seed.forceEval else seed
    val input = seedf.fMap{s =>
      val numElems = if(s == (M-1) && (N % B != 0)) N % B else B
      (0 until numElems).map(x => Random.nextInt(N))
    }
    val sorted = input.sorted(new RangePartitioner(numReducer, input))
    sorted.size
  }

  @command(description = "Sort objects")
  def objectSort(@option(prefix = "-N", description = "num entries")
                 N: Int = 1024 * 1024,
                 @option(prefix = "-m", description = "num mappers")
                 M: Int = 10,
                 @option(prefix = "-r", description = "num reducers")
                 R: Int = 3,
                 forceEval : Boolean = true) = {
    // Create a random Int sequence
    info("Preparing random data")
    val B = (N.toDouble / M).ceil.toInt
    info(f"N=$N%,d, B=$B%,d, M=$M")
    val seed = Silk.scatter((0 until M).toIndexedSeq, M)
    val seedf = if(forceEval) seed.forceEval else seed
    val input = seedf.fMap{s =>
      val numElems = if(s == (M-1) && (N % B != 0)) N % B else B
      (0 until numElems).map(x => new Person(Random.nextInt(N), Person.randomName))
    }
    val sorted = input.sorted(new RangePartitioner(R, input))
    sorted.size
  }

}