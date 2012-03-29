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
import xerial.silk.io.SilkTextWriter
import xerial.silk.lens.ObjectSchema

/**
 * @author leo
 */
class BEDGeneTest extends SilkSpec {

    "BEDGene" should {
        "be desribed as as silk" in {
          val schema = ObjectSchema.of[BEDGene]
          debug("schema:" + schema)
          debug("parameters:" + schema.parameters.mkString(","))
          val g = new BEDGene("chr1", 10000, 20000, Forward, "mygene", 100, 11000, 19000, "0", 0, Array.empty, Array.empty)
          debug(SilkTextWriter.toSilk("bed", g))


        }
    }

}
