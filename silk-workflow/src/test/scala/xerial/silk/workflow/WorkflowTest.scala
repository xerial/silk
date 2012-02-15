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

package xerial.silk.workflow

import xerial.silk.util.SilkSpec
import java.io.File

//--------------------------------------
//
// WorkflowTest.scala
// Since: 2012/02/10 9:28
//
//--------------------------------------


object WorkflowTest {

  import Workflow._

  class MyWork {

    // %.o : %.c
    //   gcc -c $< -o $@
    def compile(src:File): File = {
      // do gcc
      new File("")
    }

    def prepareSrc = {


    }


    //(compile _) <= prepareSrc
    
    class Read
    class Alignment
    class FMIndex

    val fastqSource = Task({f:File => new Read})
    val align = Task({(fmIndex:FMIndex, read:Read) => new Alignment() })

    align <= fastqSource
    
    
    
  }

}

/**
 * @author leo
 */
class WorkflowTest extends SilkSpec {
  import WorkflowTest._
  "Workflow" should {
    "define dependencies" in {
      new MyWork()
    }


  }

}