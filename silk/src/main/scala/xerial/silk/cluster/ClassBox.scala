//--------------------------------------
//
// ClassBox.scala
// Since: 2012/12/20 10:31 AM
//
//--------------------------------------

package xerial.silk.cluster

import java.io._
import java.net.{URL, URLClassLoader}
import xerial.core.log.Logger
import xerial.silk.io.Digest
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry
import xerial.core.io.IOUtil._

object ClassBox extends Logger {

  /**
   * Current context class box
   */
  lazy val current : ClassBox = {

    def listURLs(cl:ClassLoader) : Seq[URL] = {
      if(cl == null || cl == ClassLoader.getSystemClassLoader)
        Seq.empty[URL]
      else {
        val urls = cl match {
          case u:URLClassLoader =>  u.getURLs.toSeq
          case _ => Seq.empty[URL]
        }
        listURLs(cl.getParent) ++ urls
      }
    }
    val cl = Thread.currentThread().getContextClassLoader()
    debug("Enumerating class URLs in class loader: %s", cl.getClass)
    val cp = listURLs(cl)
    trace("class path entries:\n%s", cp.mkString("\n"))

    def isJarFile(u:URL) = u.getProtocol == "file" && u.getFile.endsWith(".jar")

    val jarEntries = for(jarURL <- cp.filter(isJarFile)) yield
       JarEntry(jarURL, Digest.md5sum(new File(jarURL.getFile)))

    def listFiles(dir:File) : Seq[FilePath] = {
      def list(f:File) : Seq[FilePath] = {
        if(f.isDirectory)
          f.listFiles.flatMap(list)
        else
          Seq(FilePath(dir, f))
      }
      if(dir.isDirectory)
        dir.listFiles.flatMap(list(_))
      else
        Seq.empty
    }

    // Alphabetically sorted list of classes
    val nonJarFiles = (for{
      url <- cp.filterNot(isJarFile)
      f <- listFiles(new File(url.getFile))
    } yield f).distinct.sortBy(_.fullPath.getCanonicalPath)


    val je = Seq(createJarFile(nonJarFiles)) ++ jarEntries
    trace("jar entries:\n%s", je.mkString("\n"))
    new ClassBox(je)
  }

  private[cluster] case class FilePath(dir:File, fullPath:File) {
    val relativePath : String = {
      val d = dir.getCanonicalPath + File.separator
      val f = fullPath.getCanonicalPath
      val pos = f.indexOf(d)
      if(pos == 0)
        f.substring(d.length)
      else
        f
    }

    override def hashCode = {
      relativePath.hashCode
    }

    override def equals(other:Any) = {
      relativePath.equals(other.asInstanceOf[FilePath].relativePath)
    }
  }

  /**
   * Creat a new jar file from a given set of files
   * @param entries
   * @return
   */
  private[cluster] def createJarFile(entries:Seq[FilePath]) : JarEntry = {
    val tmpJar = File.createTempFile("context", ".jar", SILK_TMPDIR)
    tmpJar.deleteOnExit()

    debug("creating context.jar file: %s", tmpJar)

    val jar = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(tmpJar)))

    // write MANIFEST
    jar.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"))
    val manifestHeader = "Manifest-Version: 1.0\n".getBytes()
    jar.write(manifestHeader)
    jar.closeEntry()

    // put files into jar
    val buf = new Array[Byte](8192)
    for(e <- entries) {
      trace("Copying entry: %s, %s", e.relativePath, e.fullPath)
      val ze = new ZipEntry(e.relativePath)
      jar.putNextEntry(ze)

      withResource(new FileInputStream(e.fullPath)) { in =>
        var readBytes = 0
        while({readBytes = in.read(buf); readBytes != -1} ) {
          jar.write(buf, 0, readBytes)
        }
      }
      jar.closeEntry()
    }
    jar.close()
    JarEntry(tmpJar.toURI.toURL, Digest.md5sum(tmpJar))
  }


  case class JarEntry(path:URL, md5sum:String)

  def withClassLoader[U](cl:ClassLoader)(f: => U) : U = {
    val prevCl = Thread.currentThread.getContextClassLoader
    try {
      Thread.currentThread.setContextClassLoader(cl)
      f
    }
    finally{
      Thread.currentThread.setContextClassLoader(prevCl)
    }
  }

}

/**
 * Container of class files (including *.class and *.jar files)
 *
 * @author Taro L. Saito
 */
class ClassBox(entries:Seq[ClassBox.JarEntry]) {
  def md5sum = {
    val md5sum_seq = entries.map(_.md5sum).mkString(":")
    withResource(new ByteArrayInputStream(md5sum_seq.getBytes)) { s =>
      Digest.md5sum(s)
    }
  }

  /**
   * Return the class loader
   * @return
   */
  def classLoader : URLClassLoader = {
    val urls = entries.map(_.path).toArray
    new URLClassLoader(urls, ClassLoader.getSystemClassLoader)
  }
}

