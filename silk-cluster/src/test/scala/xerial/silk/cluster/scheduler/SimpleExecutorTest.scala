//--------------------------------------
//
// SimpleExecutorTest.scala
// Since: 2013/05/15 8:54
//
//--------------------------------------

package xerial.silk.cluster.scheduler

import xerial.silk.util.SilkSpec
import xerial.silk.core.CallGraph
import xerial.silk.core.SilkFlow.{FlatMap, RawInputSingle}

/**
 * @author Taro L. Saito
 */
class SimpleExecutorTest extends SilkSpec {
  "SimpleExecutor" should {

    import xerial.silk._
    implicit val s = new SimpleExecutor

//    "evaluate Silk" in {
//      // Simple Silk program
//      val input = (for(i <- 1 to 10) yield i).toSilk
//      val r = input.map(_*2)
//
//      r.sum.get shouldBe 110
//
//      val ans = (for(i <- 1 to 10) yield i).map(_*2)
//      r.toSeq shouldBe ans
//    }
//
//    "evaluate nested loops" taggedAs("nested") in {
//      val xl = Seq(true, false).toSilk
//      val yl = Seq(0.1, 0.2).toSilk
//      val result = for(x <- xl; y <- yl) yield {
//        (x, y)
//      }
//
//      debug(result.toSeq)
//    }

    "rewrite Silk data retrieval" taggedAs("rewrite") in {
      val as = RawInputSingle(100)
      val r = for(i <- Seq(1, 2, 3).toSilk; a<- as) yield {
        i * a
      }

      import scala.reflect.runtime.{universe=>ru}
      import scala.tools.reflect.ToolBox
      import ru._

      val mirror = ru.runtimeMirror(Thread.currentThread.getContextClassLoader)
      val tb : ToolBox[ru.type] = mirror.mkToolBox()


      class Rewriter(target:Set[String]) extends Transformer {

        override def transform(tree: ru.Tree) = {
          tree match {
            case Select(i @ Ident(term), op) if target.contains(term.decoded) =>
              debug(s"Ident($term)")
              Select(Select(i, ru.newTermName("eval")), op)
            case _ => super.transform(tree)
          }
        }

        override def transformIdents(trees: List[Ident]) = {
          for(t <- trees) yield {
            debug(t)
            t
          }
          super.transformIdents(trees)
        }
      }


      r match {
        case FlatMap(prev, f, fExpr) =>
          debug(s"fExpr ${showRaw(fExpr)}")
          fExpr match {
            case Expr(fn @ Function(_, _)) =>
              debug(s"function: ${showRaw(fn)}")
              //val ff = tb.eval(fn)
              //debug(ff)
            case _ =>
          }

          val freeTerms = fExpr.tree.freeTerms
          val ftNameSet = (for(ft <- freeTerms if CallGraph.isSilkTypeSymbol(ft)) yield {
            ft.name.decoded
          }).toSet

          val rw = new Rewriter(ftNameSet)
          val rewritten = rw.transform(fExpr.tree)
          debug(s"rewritten: ${showRaw(rewritten)}")
          //tb.eval(rewritten)
          //val checked = tb.typeCheck(rewritten)
          //val ev = tb.eval(rewritten)
          //debug(s"evaluated: $ev")
        case _ =>
      }


    }

  }
}