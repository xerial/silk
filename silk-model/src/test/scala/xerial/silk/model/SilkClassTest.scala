package xerial.silk.model

import xerial.silk.util.{SilkWordSpec, SilkSpec}


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

class SilkClassTest extends SilkWordSpec {

  "SilkPackage" should {
    "create a single level package" in {
      val p = SilkPackage("utgenome")
      p.name must be ("utgenome")
    }

    "create new packages from strings" in {
      val p = SilkPackage("utgenome.bio")
      p.name must be ("utgenome.bio")
    }


    "create the root package" in {
      val r = SilkPackage.root
      r.name must be ("_root")
    }
  }


  "SilkClass" should {

    "detect prefixed package names" in {
      val c = SilkClass("utgenome.bio.fasta")
      c.fullName must be ("utgenome.bio.fasta")
      c.name must be ("fasta")
    }


  }

}