package xerial.silk

import java.util.UUID
import java.io.{File, ObjectOutputStream, ByteArrayOutputStream}
import scala.io.Source
import xerial.silk.framework.{IDUtil, Host}
import xerial.core.util.Shell
import xerial.core.log.Logger
import xerial.silk.framework.ops.FContext
import scala.reflect.ClassTag

/**
 * @author Taro L. Saito
 */
object SilkUtil extends IDUtil with Logger {

  // Count the silk operations defined in the same FContext
  private val idTable = collection.mutable.Map[String, Int]()


  private[silk] def newUUID(inputs:Seq[Silk[_]], fc:FContext) = {
    val parentIds = for(i <- inputs) yield i.idPrefix
    val p = s"${fc.refID}-${parentIds.mkString("-")}"
    newUUIDFromString(p)
  }

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

    newUUIDFromString(Seq(refID, inputIDs).mkString("-"))
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

  def getVersion : String = {
    val versionFile = getVersionFile
    val versionNumber =
      if (versionFile.exists()) {
        // read properties file
        val prop = (for{
          line <- Source.fromFile(versionFile).getLines
          c = line.split(":=")
          pair <- if(c.length == 2) Some((c(0).trim, c(1).trim)) else None
        } yield pair).toMap

        prop.get("version")
      }
      else
        None

    val v = versionNumber getOrElse "unknown"
    v
  }

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
