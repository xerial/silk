package xerial.silk.cui;

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
    val classPathEntries =  sys.props.getOrElse("java.class.path", "")
      .split(File.pathSeparator)
      .map { e => new File(e).toURI.toURL } ++
      ClassBox.classPathEntries(classLoader)

    trace(s"classpath entries:\n${classPathEntries.mkString("\n")}")

    val isFullPath = clName.lastIndexOf(".") != -1
    val clPath = s"${clName.replaceAll("\\.", "/")}.class"
    val clFile = s"${cname}.class"

    def removeExt(s:String) = s.replaceAll("\\.class$", "")

    def findTargetClassFile(resource:URL) : Option[String] = {
      if(ClassBox.isJarFile(resource)) {
        // Find the target class from a jar file
        val path = resource.getPath
        val jarPath = path.replaceAll("%20", " ")
        val jarFilePath = jarPath.replace("file:", "")
        val jar = new JarFile(jarFilePath)
        val entryName = if(isFullPath)
          Option(jar.getEntry(s"/$clPath")).map(_.getName)
        else {
          jar.entries.collectFirst{
            case e if e.getName.endsWith(clFile) =>
              e.getName
          }
        }
        entryName.map(name => removeExt(name))
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
