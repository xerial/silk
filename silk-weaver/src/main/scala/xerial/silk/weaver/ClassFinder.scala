//--------------------------------------
//
// ClassFinder.scala
// Since: 2013/11/13 12:41 PM
//
//--------------------------------------

package xerial.silk.weaver

import xerial.silk.framework.ClassBox
import java.net.URL
import java.util.jar.JarFile
import scala.annotation.tailrec
import java.io.File
import xerial.silk.util.Path
import xerial.core.log.Logger

/**
 * ClassFinder finds a full class name from its partial class name
 *
 * @author Taro L. Saito
 */
object ClassFinder extends Logger {

  def findClass(clName:String, classLoader: => ClassLoader = Thread.currentThread.getContextClassLoader) : Option[String] = {

    val cname = {
      val pos = clName.lastIndexOf(".")
      if(pos == -1)
        clName
      else
        clName.substring(pos+1)
    }

    import scala.collection.JavaConversions._
    val classPathEntries = ClassBox.classPathEntries(classLoader)

    val isFullPath = clName.lastIndexOf(".") != -1
    val clPath = s"${clName.replaceAll("\\.", "/")}.class"
    val clFile = s"${cname}.class"

    def removeExt(s:String) = s.replaceAll("\\.class$", "")

    def findTargetClassFile(resource:URL) : Option[String] = {
      if(ClassBox.isJarFile(resource)) {
        // Find the target class from a jar file
        val path = resource.getPath
        val pos: Int = path.indexOf("!")
        if(pos == -1)
          None
        else {
          val jarPath = path.substring(0, pos).replaceAll("%20", " ")
          val jarFilePath = jarPath.replace("file:", "")
          val jar = new JarFile(jarFilePath)

          val entryName = if(isFullPath)
            Option(jar.getEntry(s"/$clPath")).map(_.getName)
          else
            jar.entries.collectFirst{
              case e if e.getName.endsWith(clFile) =>
                e.getName
            }
          entryName.map(name => removeExt(name))
        }
      }
      else if(resource.getProtocol == "file") {
        // Find the target class from a directory
        @tailrec
        def find(lst:List[File]) : Option[File] = {
          if(lst.isEmpty)
            None
          else {
            val h = lst.head
            if(h.isDirectory)
              find(h.listFiles.toList ::: lst.tail)
            else {
              val fileName = h.getName
              if(fileName.endsWith(".class") && fileName == clFile)
                Some(h)
              else
                find(lst.tail)
            }
          }
        }

        import Path._
        val filePath = resource.getPath
        val base = new File(filePath)
        if(isFullPath) {
          // Search the target file by directly specifying the file name
          val f = new File(filePath, clPath)
          if(f.exists())
            Some(f.relativeTo(base).getPath)
          else
            None
        }
        else {
          // Search directories recursively
          find(List(base)).map{ f => f.relativeTo(base).getPath }
        }
      }
      else
        None
    }

    val targetClassName = classPathEntries.toIterator.map(findTargetClassFile).collectFirst{
      case Some(relativePathToClass) => {
        removeExt(relativePathToClass).replaceAll("\\/", ".")
      }
    }

    targetClassName
  }

}