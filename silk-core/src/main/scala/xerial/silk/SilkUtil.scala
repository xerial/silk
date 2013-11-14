package xerial.silk

import java.util.UUID
import java.io.{File, ObjectOutputStream, ByteArrayOutputStream}
import scala.io.Source
import xerial.silk.framework.{IDUtil, Host}
import xerial.core.util.Shell
import xerial.core.log.Logger
import xerial.silk.framework.core.FContext
import scala.reflect.ClassTag

/**
 * @author Taro L. Saito
 */
object SilkUtil extends IDUtil with Logger {

  // Count the silk operations defined in the same FContext
  private val idTable = collection.mutable.Map[String, Int]()



  private[silk] def newUUIDOf(opcl:Class[_], fc:FContext, inputs:Any*) = {
    val inputIDs = inputs.map {
      case s:Silk[_] => s.idPrefix
      case other => other.toString
    }

    val refID = synchronized {
      val r = fc.refID
      val count = idTable.getOrElseUpdate(r, 0)
      idTable.update(r, count+1)
      s"${r}:$count"
    }

    val hashStr = Seq(refID, inputIDs).mkString("-")
    val uuid = newUUIDFromString(hashStr)
    trace(s"$hashStr: $uuid")
    uuid
  }

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

  /**
   * Check wheather silk is installed
   * @param h
   */
  def isSilkInstalled(h:Host) : Boolean = {
    val ret = Shell.exec("ssh -n %s '$SHELL -l -c silk version'".format(h.name))
    ret == 0
  }

}
