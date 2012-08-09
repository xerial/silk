package xerial.silk.model

import xerial.silk.util.{SilkSpec, SilkFlatSpec}

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

class SilkModelTest extends SilkSpec {

  "SilkNameSpace" should {
    "create a single level package" in {
      val p = SilkNameSpace("utgenome")
      p.leafName must be ("utgenome")
    }

    "create new packages from strings" in {
      val p = SilkNameSpace("utgenome.bio")
      p.name must be ("utgenome.bio")
    }


    "create the root package" in {
      val r = SilkNameSpace.root
      r.name must be ("_root")
    }

    "detect prefixed module names" in {
      val c = SilkNameSpace("utgenome.bio.fasta")
      c.name must be ("utgenome.bio.fasta")
      c.leafName must be ("fasta")
    }

  }

}