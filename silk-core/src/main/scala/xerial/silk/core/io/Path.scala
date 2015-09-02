/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// Path.scala
// Since: 2013/10/16 10:42
//
//--------------------------------------

package xerial.silk.core.io

import java.io.{File => JFile}

import scala.language.implicitConversions

/**
 * Utilities for managing files and paths
 * @author Taro L. Saito
 */
object Path {
  implicit def wrap(s:String) = new Path(new JFile(s))
  implicit def wrap(f:JFile) = new Path(f)
}

class Path(val f:JFile) extends AnyVal {
  def / (s:String) : JFile = new JFile(f, s)

  def ls : Seq[Path] = {
    if(f.isDirectory)
      f.listFiles.map{ c => new Path(c) }
    else
      Seq.empty
  }

  def rmdirs {
    if(f.isDirectory) {
      ls foreach { _.rmdirs }
    }
    f.delete()
  }

  def relativeTo(base:JFile) : JFile = {
    new JFile(f.getPath.replaceFirst(s"${base.getPath}\\/", ""))
  }

  def removeExt(dotExt:String) : String = {
    val path = f.getPath
    val pos = path.lastIndexOf(s"$dotExt")
    if(pos == -1)
      path
    else
      path.substring(0, pos)
  }

}

