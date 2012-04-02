/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xerial.silk.glens

import xerial.silk.util.SilkSpec

//--------------------------------------
//
// PrioritySearchTreeTest.scala
// Since: 2012/04/02 9:17
//
//--------------------------------------

/**
 * @author leo
 */
class PrioritySearchTreeTest extends SilkSpec {

  trait Fixture {
    val p = new PrioritySearchTree[String]()
    p.insert("A", 1, 2);
    p.insert("B", 2, 4);
    p.insert("C", 1, 8);
    p.insert("D", 3, 3);
    p.insert("E", 5, 6);
  }


  "PrioritySearchTree" should {
    "support insert" in {
      new Fixture {
        p.size must be(5)
      }
    }

    "support range queries" in {
      new Fixture {
        val q = p.rangeQuery(1, 2, 6)
        q.size must be(2)
        q.contains("A") must be(true)
        q.contains("B") must be(true)

        val q2 = p.rangeQuery(2, 5, 4)
        q2.size must be(2)
        q2.contains("B") must be(true)
        q2.contains("D") must be(true)

        val q3 = p.rangeQuery(2, 5, 10)
        q3.size must be(3)
        q3.contains("B") must be(true)
        q3.contains("E") must be(true)
        q3.contains("D") must be(true)
      }
    }

    "support node removal" in {
      new Fixture {
        p.remove("B", 2, 4)
        val q = p.rangeQuery(1, 2, 6)
        q.size must be(1)
        q.contains("A") must be(true)
        q.contains("B") must be(false)

        val q2 = p.rangeQuery(2, 5, 4)
        q2.size must be(1)
        q2.contains("D") must be(true)
      }
    }

    "support iteration" in {
      new Fixture {
        val nodes = p.toArray
        nodes.length must be(5)
        Array("A", "B", "C", "D", "E").foreach {
          nodes.contains(_) must be(true)
        }
      }
    }

    "support insertion of the same interval" in {
      val p = new PrioritySearchTree[String]
      val N = 10000
      (0 until N).foreach(i => p.insert("A", 10, 20))

      val q = p.rangeQuery(0, 100, 30);
      q.size must be (N)
    }


  }
}