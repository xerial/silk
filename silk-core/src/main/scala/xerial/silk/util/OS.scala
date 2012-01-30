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
// OS.scala
// Since: 2012/01/30 11:58
//
//--------------------------------------


/**
 * OS type resolver
 *
 * @author leo
 */
object OSInfo {

  def isWindows = getOSType == OS.Windows

  def getOSType : OS = {
    val osName : String = System.getProperty("os.name", "unknown").toLowerCase
    if(osName.contains("win")){
      OS.Windows
    }
    else if(osName.contains("mac")) {
      OS.Mac
    }
    else if(osName.contains("linux")) {
      OS.Linux
    }
    else
      OS.Other
  }


}