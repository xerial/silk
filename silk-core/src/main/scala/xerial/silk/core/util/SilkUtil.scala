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
package xerial.silk.core.util

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.util.UUID

import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
object SilkUtil extends Logger {

  // Count the silk operations defined in the same FContext
  private val idTable = collection.mutable.Map[String, Int]()

//  private[silk] def newUUIDOf(opcl:Class[_], fc:FContext, inputs:Any*) = {
//    val inputIDs = inputs.map {
//      case s:Silk[_] => s.id.toString.substring(0, 8)
//      case other => other.toString
//    }
//
//    val refID = synchronized {
//      val r = fc.refID
//      val count = idTable.getOrElseUpdate(r, 0)
//      idTable.update(r, count+1)
//      s"${r}:$count"
//    }
//
//    val hashStr = Seq(refID, inputIDs).mkString("-")
//    val uuid = newUUIDFromString(hashStr)
//    trace(s"$hashStr: $uuid")
//    uuid
//  }

  private [silk] def newUUIDFromString(s:String) =
    UUID.nameUUIDFromBytes(s.getBytes("UTF8"))

  private[silk] def newUUID: UUID = UUID.randomUUID

  private[silk] def newUUIDOf[A](in: Seq[A]): UUID = {
    val b = new ByteArrayOutputStream
    val os = new ObjectOutputStream(b)
    for (x <- in)
      os.writeObject(x)
    os.close
    UUID.nameUUIDFromBytes(b.toByteArray)
  }


  private[silk] def getVersionFile = {
    val home = System.getProperty("prog.home")
    new File(home, "VERSION")
  }

  def getVersion : String = sys.props.getOrElse("prog.version", "unknown")

  def getBuildTime : Option[Long] = {
    val versionFile = getVersionFile
    if (versionFile.exists())
      Some(versionFile.lastModified())
    else
      None
  }


}
