//--------------------------------------
//
// GraphTest.scala
// Since: 2012/05/22 2:27 PM
//
//--------------------------------------

package xerial.silk.workflow

import xerial.silk.util.SilkSpec

/**
 * @author leo
 */
class GraphTest extends SilkSpec {

  "graph" should {

    "replicate graphs" in {
      val a = Graph("A")
      debug(a.toGraphviz)
      val an = a.replicate(3)
      debug(an)

      val b = Graph("B")
      val bn = b.replicate(3)
      debug(bn)

      val c = an->bn
      debug(c)

    }

    "take OR of graphs" in {
      val b = Graph("b")
      val c = Graph("c")
      val d = Graph("d")
      val e = b -> c | b -> d
      debug(e)
    }

    "create bipartite graphs" in {
      val an = Graph("A").replicate(4)
      val bn = Graph("B").replicate(4)

      val c = an ->> bn
      debug(c)
    }

    "merge outgoing edges" in {
      val an = Graph("A").replicate(3)
      val c = Graph("c")
      val d = an -> c
      debug(d)
      val bn = Graph("B").replicate(3)
      val e = an -> c -> bn
      debug(e)

      val g = e | an -> bn
      debug(g)
    }

    "connect two graphs" in {
      val g = Graph("A") -> Graph("C") -> Graph("D") -> Graph("B") | Graph("A") -> Graph("F") -> Graph("B")
      debug(g)
    }


  }




}