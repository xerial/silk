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
// GenomeRangeTest.scala
// Since: 2012/04/02 16:05
//
//--------------------------------------

/**
 * @author leo
 */
class GenomeRangeTest extends SilkSpec {
  
  "interval" should {
    "have equality for the same range" in {
      val i1 = new Interval(1, 100)
      val i2 = new Interval(1, 100)
      i1.hashCode must be (i2.hashCode)
      i1 must be (i2)

      val i3 = new Interval(1, 109)
      i3 must not be (i1)
    }
  }

  "GInterval" should {
    "satisfy equality" in {
      val g1 = new GInterval("chr1", 34, 140, Forward)
      val g2 = new GInterval("chr1", 34, 140, Forward)
      g1.hashCode must be (g2.hashCode)
      g1 must be (g2)

      // compare different type of objects
      val g3 = new Interval(34, 140)
      g3 must not be (g1)
      g3.hashCode must not be (g1.hashCode)
    }
  }

  "GLocus" should {
    "satisfy equality" in {
      val l1 = new GLocus("chr1", 134134, Forward)
      val l2 = new GLocus("chr1", 134134, Forward)
      l1 must be (l2)
      l1.hashCode must be (l2.hashCode)

      val l3 = new GLocus("chr2", 134134, Forward)
      val l4 = new GLocus("chr1", 134134, Reverse)
      l1 must not be(l3)
      l1.hashCode must not be(l3.hashCode)
      l1 must not be(l4)
      l1.hashCode must not be(l4.hashCode)
    }
  }

}