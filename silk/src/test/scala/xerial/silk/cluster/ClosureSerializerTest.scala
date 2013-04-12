//--------------------------------------
//
// ClosureSerializerTest.scala
// Since: 2013/04/03 2:56 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec

object ClosureSerializerTest {
  case class A(id:Int, name:String)
}

/**
 * @author Taro L. Saito
 */
class ClosureSerializerTest extends SilkSpec {

  import ClosureSerializerTest._

  "ClosureSerializer" should {
    "detect accessed variables in nested functions" in {
      def f(x:A) : Boolean = { x.id == 1 }
      val accessedFields = ClosureSerializer.accessedFieldsInClosure(classOf[A], f)
      accessedFields should contain ("id")
    }


    "detect accsessed fields recursively" taggedAs("acc") in {
      def mylog = { warn("dive in deep") }
      val ser = ClosureSerializer.serializeClosure(mylog)
      Remote.run(ser)
    }

    "serialize outer variable" taggedAs("outer") in {
      var v : Int = 100
      var s : String = "hello"
      def p = { println(v); println(s) }
      val s1 = ClosureSerializer.serializeClosure(p)
      Remote.run(s1)
      v += 1
      s = "world"
      val s2 = ClosureSerializer.serializeClosure(p)
      Remote.run(s2)
    }

    "serialize second outer variable" taggedAs("outer2") in {
      var v : Int = 100

      for(i <- 0 until 2) {
        v += i
        def p = { println(v) }
        val s1 = ClosureSerializer.serializeClosure(p)
        Remote.run(s1)
      }
    }

    "serialize log and outer variable" taggedAs("outer3") in {
      pending
      var v : Int = 1000

      for(i <- 0 until 1) {
        v += i
        def p = { info(v) }
        val s1 = ClosureSerializer.serializeClosure(p)
        Remote.run(s1)
      }
    }

  }
}