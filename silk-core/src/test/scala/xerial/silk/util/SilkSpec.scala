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

import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import org.xerial.util.FileResource
import java.io.{BufferedReader, BufferedInputStream}
import org.scalatest._


//--------------------------------------
//
// SilkFlatSpec.scalacala
// Since: 2012/01/09 8:45
//
//--------------------------------------

/**
 * Test case generation helper
 * @author leo
 */
trait SilkFlatSpec extends FlatSpec with ShouldMatchers with MustMatchers with GivenWhenThen with Logger {

}

trait SilkSpec extends WordSpec with ShouldMatchers with MustMatchers with GivenWhenThen with OptionValues with Logger {

  def withByteResource(relativePath:String)(f: BufferedInputStream => Unit) : Unit = {
    val b = FileResource.openByteStream(this.getClass, relativePath)
    try {
      f(b)
    }
    finally {
      if(b != null)
      b.close()
    }
  }

  def withResource(relativePath:String)(f: BufferedReader => Unit) : Unit = {
    val b = FileResource.open(this.getClass, relativePath)
    try {
      f(b)
    }
    finally {
      if(b != null)
        b.close()
    }
  }

  def resource(relativePath:String) : String = {
    FileResource.loadIntoString(this.getClass, relativePath)
  }

  def resourceStream(relativePath:String) : BufferedInputStream  = {
    FileResource.openByteStream(this.getClass, relativePath)
  }

  def resourceReader(relativePath:String) : BufferedReader = {
    FileResource.open(this.getClass, relativePath)
  }

}
