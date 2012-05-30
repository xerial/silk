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

package xerial.silk.util

//--------------------------------------
//
// CNameTest.scala
// Since: 2012/02/16 15:18
//
//--------------------------------------

/**
 * @author leo
 */
class CNameTest extends SilkSpec {
  import CName._
  "CName" should {
    "translate variable name" in {

      val naturalNameAndVarNameSets = Seq(
        ("distance to SL1", "distanceToSL1"),
        ("hello world", "helloWorld"),
        ("XML string", "XML_String"),
        ("param name", "paramName"),
        ("allow TAB in var name", "allowTABinVarName"),
        ("wiki name like var name", "WikiNameLikeVarName"),
        ("var arg01", "var_arg01"),
        ("para1", "para1"),
        ("tip and dale", "tip_andDale"),
        ("action package", ("actionPackage"))
      )

      

      for((naturalName, varName) <- naturalNameAndVarNameSets) {
        when("translating '%s' into '%s'".format(varName, naturalName))
        toNaturalName(varName) should be (naturalName)
      }
    }

    "find prefix XML" in {
      when("input is XMLParser")
      val c1 = CName("XMLParser")
      c1.naturalName should be ("XML parser")
    }

    "has the same order between different naming" in {
      val c1 = CName("XMLParser")
      val c2 = CName("XML parser")
      
      c1 should be equals (c2)
      c1.canonicalName should be ("xmlparser")
      c1.canonicalName should be (c2.canonicalName)


    }

    
  }

}