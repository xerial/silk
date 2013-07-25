package xerial.silk

import java.util.UUID
import java.io.{File, ObjectOutputStream, ByteArrayOutputStream}
import scala.io.Source

/**
 * @author Taro L. Saito
 */
object SilkUtil {

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

}
