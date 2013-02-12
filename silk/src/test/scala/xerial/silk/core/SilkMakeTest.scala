//--------------------------------------
//
// SilkMakeTest.scala
// Since: 2013/02/12 3:03 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class SilkMakeTest extends SilkSpec {

  object MyTask extends SilkMake {

    def input = task ~ "echo hello"
    def wc = task ~ input | "wc"

    def sort = task ~ "seq 10" | "sort -nr"
  }


  "SilkMake" should {
    "describe workflows" in {
      MyTask.input.run
    }
  }

}