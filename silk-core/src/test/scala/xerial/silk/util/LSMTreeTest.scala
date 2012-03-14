package xerial.silk.util

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

import scala.actors.Actor._
import util.Random


//--------------------------------------
//
// LSMTreeTest.scala
// Since: 2012/02/19 3:59
//
//--------------------------------------

/**
 * @author leo
 */
class LSMTreeTest extends SilkSpec {

  "LSMTree" should {

    "support concurrent insertion" in {

      val l = new LSMTree[Int, String]


      def producer(name: String) = actor {

        def randomInt: Int = Random.nextInt()
        def randomStr: String = {
          val size = Random.nextInt(100)
          (0 until size).map {
            _ => ('a' + Random.nextInt(26)).asInstanceOf[Char]
          }.mkString
        }

        trace("[%s] thread:%s", name, Thread.currentThread.getId)
        l.put(randomInt, randomStr)
      }

      import xerial.silk.util.TimeMeasure._

      val N = 1000
      time("insert", repeat = 2) {
        block("concurrent") {
          for (i <- 0 until N) {
            val a = producer("concurrent")
            a.start
          }
        }

        val p = producer("single")
        block("single") {
          for (i <- 0 until N) {
            p.start()
          }
        }
      }

    }

  }

}