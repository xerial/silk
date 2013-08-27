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
import org.xerial.snappy.Snappy
import collection.GenSeq
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output


object CompressedFieldWriterTest {
  case class Employee(id:Int, code:Long, age:Int, name:String, address:Seq[Address])
  case class Address(line:String, phone:Option[String])

}

/**
 * @author Taro L. Saito
 */
class CompressedFieldWriterTest extends SilkSpec {

  import CompressedFieldWriterTest._


  val r = new Random(0)

  def randomEmp(id:Int) = Employee(id, r.nextLong, r.nextInt(100), randomName(math.max(3, r.nextInt(10))), randomAddr)

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
      val e = new ColumnarEncoder(ReflectionEncoder)
      val N = 100
      val emps = randomEmpDataSet(N)
      debug("encoding start")
      var c : GenSeq[ColumnBlock] = null
      time("encode") {
        e.encode(emps)
        c = e.compress
      }
      val containerSize = c.map{ct => (ct.uncompressedSize, ct.byteLength)}
      debug(f"container size: ${containerSize.mkString(", ")}")
      val uncompressedTotal = c.map(_.uncompressedSize).sum
      val total = c.map{_.byteLength}.sum
      debug(f"compression ${uncompressedTotal}%,d => ${total}%,d (${total.toDouble / uncompressedTotal * 100.0}%.2f%%")
    }

    "support Javassist-based FieldEncoder" taggedAs("ja") in {
      val e = new ColumnarEncoder(JavassistEncoder)
      val N = 100
      val emps = randomEmpDataSet(N)
      var c : GenSeq[ColumnBlock] = null
      time("encode with javassist") {
        e.encode(emps)
        c = e.compress
      }
      val containerSize = c.map{ct => (ct.uncompressedSize, ct.byteLength)}
      debug(f"container size: ${containerSize.mkString(", ")}")
      val uncompressedTotal = c.map(_.uncompressedSize).sum
      val total = c.map{_.byteLength}.sum
      debug(f"compression $uncompressedTotal%,d => $total%,d (${total.toDouble / uncompressedTotal * 100.0}%.2f%%)")
    }

    "test performances" taggedAs("perf") in {
      val R = 5

      debug("start bench")
      time("encode", repeat = R) {
        for(n <- Seq(100, 1000, 10000))  {
          val N = n
          val emps = randomEmpDataSet(N)
          block(s"reflection$N") {
            val e = new ColumnarEncoder(ReflectionEncoder)
            e.encode(emps)
            //e.compress
          }

          block(s"javassist$N") {
            val e = new ColumnarEncoder(JavassistEncoder)
            e.encode(emps)
            // e.compress
          }

          block(s"jserializer$N") {
            val b = new ByteArrayOutputStream
            val s = new ObjectOutputStream(b)
            emps.foreach { e => s.writeObject(e) }
            s.close
            val out = b.toByteArray
            //Snappy.compress(out)
          }

          block(s"kryo$N") {
            val b = new ByteArrayOutputStream
            val o = new Output(b)
            val k = new Kryo()
            emps.foreach { e => k.writeObject(o, e) }
            o.close
            val out = b.toByteArray
            //Snappy.compress(out)
          }
        }

      }

    }

  }
}