//--------------------------------------
//
// ClosureSerializerTest.scala
// Since: 2013/06/25 12:20
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.silk.core.ClosureSerializer

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


    "detect accessed fields recursively" taggedAs("acc") in {
      def mylog = { warn("dive in deep") }
      val ser = ClosureSerializer.serializeClosure(mylog)
      Remote.run(ser)
    }

    "serialize outer variables" taggedAs("outer") in {
      var v : Int = 100
      var s : String = "hello"
      def p = { println(v); println(s) }
      val s1 = ClosureSerializer.serializeClosure(p)
      val out = captureOut {
        Remote.run(s1)
      }
      out should (include ("100"))
      out should (include ("hello"))
      v += 1
      s = "world"
      val s2 = ClosureSerializer.serializeClosure(p)
      val out2 = captureOut {
        Remote.run(s2)
      }
      out2 should (include ("101"))
      out2 should (include ("world"))
    }

    "serialize outer variables having the same name" taggedAs("outer2") in {
      var v : Int = 100 // This will be stored as v$2

      for(i <- 0 until 2) {
        v += i
        def p = { println(v) }
        val s1 = ClosureSerializer.serializeClosure(p)
        val out = captureOut {
          Remote.run(s1)
        }
        out.trim shouldBe v.toString
      }
    }

    "serialize logger and outer variable" taggedAs("outer3") in {
      // TODO this value is stored in <init>
      var v : Int = 1000

      for(i <- 1 until 2) {
        v += i
        def p = {
          val m = v.toString
          info(m)
        }
        val s1 = ClosureSerializer.serializeClosure(p)
        Remote.run(s1)
      }
    }

    "retrieve return type of Function3" taggedAs("f3") in {
      def f(a:Int, b:String, c:Int) : Unit = {}

      val f3 = f(_, _, _)

      for(m <- f3.getClass.getDeclaredMethods.find(m => m.getName == "apply" && !m.isSynthetic)) {
        val retType = m.getReturnType
        info(s"f3 class: ${f3.getClass.getName}")
        info(s"return type of f3: $retType")
      }


    }

  }
}