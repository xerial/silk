//--------------------------------------
//
// CompressedFieldWriterTest.scala
// Since: 2013/02/05 2:23 PM
//
//--------------------------------------

package xerial.silk.index

import xerial.silk.util.SilkSpec
import xerial.lens.ObjectType
import util.Random

/**
 * @author Taro L. Saito
 */
class CompressedFieldWriterTest extends SilkSpec {

  import StructureEncoderTest._

  val emp1 = Employee(3, "aina", Seq(Address("X-Y-Z Avenue", Some("222-3333")), Address("ABC State", None)))
  val emp2 = Employee(4, "silk", Seq(Address("Q Town", None)))
  val emp3 = Employee(5, "sam", Seq(Address("Windy Street", Some("999-9999"))))

  val r = new Random(0)

  def randomEmp(id:Int) = Employee(id, randomName(math.max(3, r.nextInt(10))), randomAddr)

  def randomName(len:Int) : String = {
    val alphabet = "abcdefghijklmnopqrstuvwxyz "
    val s = new StringBuilder
    for(i <- 0 until len) yield
      s += alphabet(r.nextInt(alphabet.length))
    s.result
  }
  def randomAddr = for(i <- 0 until r.nextInt(2)) yield {
    Address(randomName(math.max(5, r.nextInt(15))),  randomPhone)
  }
  def randomPhone : Option[String] = if(r.nextBoolean) {
    val p = f"${r.nextInt(1000)}%03d-${r.nextInt(10000)}%04d"
    Some(p)
  }
  else
    None

  def randomEmpDataSet(N:Int) = {
    val emps = Seq() ++ (for(i <- (0 until N).par) yield {
      randomEmp(i)
    })
    emps
  }


  "CompressedFieldWriter" should {

    "compress object streams" in {
      val e = new ColumnarEncoder
      val N = 100
      val emps = randomEmpDataSet(N)
      debug("encoding start")
      var c : Seq[ColumnBlock] = null
      time("encode") {
        e.encode(emps)
        c = e.compress
      }
      val containerSize = c.map{ct => (ct.uncompressedSize, ct.byteLength)}
      debug(f"container size: ${containerSize.mkString(", ")}")
      val uncompressedTotal = c.map(_.uncompressedSize).sum
      val total = c.map{_.byteLength}.sum
      debug(f"compression $uncompressedTotal%,d => $total%,d (${total.toDouble / uncompressedTotal * 100.0}%.2f%)")
    }

    "support Javassist-based FieldEncoder" taggedAs("ja") in {
      val e = new ColumnarEncoder(JavassistEncoder)
      val N = 100
      val emps = randomEmpDataSet(N)
      var c : Seq[ColumnBlock] = null
      time("encode with javassist") {
        e.encode(emps)
        c = e.compress
      }
      val containerSize = c.map{ct => (ct.uncompressedSize, ct.byteLength)}
      debug(f"container size: ${containerSize.mkString(", ")}")
      val uncompressedTotal = c.map(_.uncompressedSize).sum
      val total = c.map{_.byteLength}.sum
      debug(f"compression $uncompressedTotal%,d => $total%,d (${total.toDouble / uncompressedTotal * 100.0}%.2f%)")
    }

    "test performances" taggedAs("perf") in {
      val N = 100000
      val R = 10
      val emps = randomEmpDataSet(N)
      time("encode", repeat = R) {
        block("reflection") {
          val e = new ColumnarEncoder
          e.encode(emps)
          e.compress
        }
        block("javassist") {
          val e = new ColumnarEncoder(JavassistEncoder)
          e.encode(emps)
          e.compress
        }

      }

    }

  }
}