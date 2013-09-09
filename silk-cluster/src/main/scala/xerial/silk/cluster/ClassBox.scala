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

//--------------------------------------
//
// ClassBox.scala
// Since: 2012/12/20 10:31 AM
//
//--------------------------------------

package xerial.silk.cluster

import java.io._
import java.net.{URISyntaxException, URL, URLClassLoader}
import xerial.core.log.Logger
import xerial.silk.io.Digest
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry
import xerial.core.io.IOUtil._
import xerial.silk.{SilkUtil, Silk, cluster}
import java.util.{UUID, Calendar}
import java.text.{SimpleDateFormat, DateFormat}
import java.nio.charset.Charset
import util.matching.Regex
import xerial.silk.framework.{IDUtil, ClassBoxAPI}

object ClassBox extends IDUtil with Logger {


  def jarEntries = {

    val cl = Thread.currentThread().getContextClassLoader()
    debug(s"Enumerating class URLs in class loader: ${cl.getClass}")
    val cp = listEntryURLs(cl)
    trace(s"class path entries:\n${cp.mkString("\n")}")

    def isJarFile(u:URL) = u.getProtocol == "file" && u.getFile.endsWith(".jar")

    val jarEntries = for(jarURL <- cp.filter(isJarFile)) yield {
      val f = new File(jarURL.getFile)
      JarEntry(jarURL, Digest.sha1sum(f), f.lastModified)
    }

    jarEntries
  }

  private def listFilesIn(dir:File) : Seq[FilePath] = {
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

  private def listEntryURLs(cl:ClassLoader) : Seq[URL] = {
    if(cl == null || cl == ClassLoader.getSystemClassLoader)
      Seq.empty[URL]
    else {
      val urls = cl match {
        case u:URLClassLoader =>  u.getURLs.toSeq
        case _ => Seq.empty[URL]
      }
      listEntryURLs(cl.getParent) ++ urls
    }
  }

  private def isJarFile(u:URL) = u.getProtocol == "file" && u.getFile.endsWith(".jar")

  /**
   * Create a ClassBox for multi-jvm testing (no synchronization between Java processes is requried)
   * @return
   */
  def localOnlyClassBox : ClassBox = {
    val cl = Thread.currentThread().getContextClassLoader()
    debug(s"Enumerating class URLs in class loader: ${cl.getClass}")
    val cp = listEntryURLs(cl)
    trace(s"class path entries:\n${cp.mkString("\n")}")

    val jarEntries = for(jarURL <- cp.filter(isJarFile)) yield {
      val f = new File(jarURL.getFile)
      JarEntry(jarURL, Digest.sha1sum(f), f.lastModified)
    }

    // Alphabetically sorted list of classes
    val nonJarFiles = cp.filterNot(isJarFile).distinct.map(url => LocalPathEntry(url, Digest.sha1sum(url.toString.getBytes))).sortBy(_.path.toString)
    val je = nonJarFiles ++ jarEntries
    trace(s"jar entries:\n${je.mkString("\n")}")
    ClassBox(localhost.address, config.dataServerPort, je)
  }

  /**
   * Current context class box
   */
  lazy val current : ClassBox = {

    val cl = Thread.currentThread().getContextClassLoader()
    debug(s"Enumerating class URLs in class loader: ${cl.getClass}")
    val cp = listEntryURLs(cl)
    trace(s"class path entries:\n${cp.mkString("\n")}")

      val jarEntries = for(jarURL <- cp.filter(isJarFile)) yield {
      val f = new File(jarURL.getFile)
      JarEntry(jarURL, Digest.sha1sum(f), f.lastModified)
    }

    // Alphabetically sorted list of classes
    val nonJarFiles = (for{
      url <- cp.filterNot(isJarFile)
      f <- listFilesIn(new File(url.getFile))
    } yield f).distinct.sortBy(_.fullPath.getCanonicalPath)


    val je = Seq(createJarFile(nonJarFiles)) ++ jarEntries
    trace(s"jar entries:\n${je.mkString("\n")}")
    ClassBox(localhost.address, config.dataServerPort, je)
  }

  /**
   * Representing a class file path in a base directory
   * @param dir
   * @param fullPath
   */
  private[cluster] case class FilePath(dir:File, fullPath:File) {
    val relativePath : String = {
      val d = dir.getCanonicalPath + File.separator
      val f = fullPath.getCanonicalPath
      val pos = f.indexOf(d)
      val path = if(pos == 0) f.substring(d.length) else f
      // Convert windows path separators (\) to '/'
      path.replaceAll(Regex.quoteReplacement(File.separator), "/")
    }

    override def hashCode = {
      relativePath.hashCode
    }

    override def equals(other:Any) = {
      relativePath.equals(other.asInstanceOf[FilePath].relativePath)
    }
  }

  lazy private val buildTime = SilkUtil.getBuildTime getOrElse (new SimpleDateFormat("yyy/MM/dd").parse("2012/12/20").getTime)

  private val utf8 = Charset.forName("UTF-8")

  /**
   * Creat a new jar file from a given set of files
   * @param entries
   * @return
   */
  private[cluster] def createJarFile(entries:Seq[FilePath]) : JarEntry = {

    debug("Creating current context jar")

    val binary = new ByteArrayOutputStream()
    val jar = new JarOutputStream(binary)

    // write MANIFEST
    val mEntry = new ZipEntry("META-INF/MANIFEST.MF")
    mEntry.setTime(buildTime)
    jar.putNextEntry(mEntry)
    val manifestHeader = "Manifest-Version: 1.0\n".getBytes(utf8)
    jar.write(manifestHeader)
    jar.closeEntry()

    // put files into jar
    val buf = new Array[Byte](8192)
    for(e <- entries) {
      //trace(s"Copying entry: ${e.relativePath} to ${e.fullPath}")
      val ze = new ZipEntry(e.relativePath)
      ze.setTime(e.fullPath.lastModified)
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

    val b = binary.toByteArray
    val sha1sum = Digest.sha1sum(b)
    val jarFile = localJarPath(sha1sum)
    // Check whether the jar file with the same sha1sum is already created
    if(!jarFile.exists || Digest.sha1sum(jarFile) != sha1sum) {
      debug(s"Created a context jar file into $jarFile")
      val f = new FileOutputStream(jarFile)
      f.write(b)
      f.close
    }
    JarEntry(jarFile.toURI.toURL, sha1sum, buildTime)
  }

  trait ClassBoxEntry {
    def path:URL
    def sha1sum:String
  }
  case class LocalPathEntry(path:URL, sha1sum:String) extends ClassBoxEntry
  case class JarEntry(path:URL, sha1sum:String, lastModified:Long) extends ClassBoxEntry

  /**
   * Temporary switch the context class loader to the given one, then execute the body function
   * @param cl
   * @param f
   * @tparam U
   * @return
   */
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

  def localJarPath(sha1sum:String) : File = {
    val d = new File(config.silkTmpDir, s"jars/${sha1sum.substring(0, 2)}/$sha1sum")
    val dir = d.getParentFile
    if(!dir.exists)
      dir.mkdirs
    d
  }


  /**
   * Check all jar entries in this ClassBox. If there is missing jars,
   * retrieve them from the host.
   *
   * TODO: caching the results
   *
   * @return
   */
  def sync(cb:ClassBox) : ClassBox = {
    info(s"synchronizing ClassBox ${cb.id.prefix} at ${cb.urlPrefix}")

    val s = Seq.newBuilder[ClassBox.ClassBoxEntry]
    var hasChanged = false
    for(e <- cb.entries) {
      e match {
        case JarEntry(path, sha1sum, lastModified) =>
          val f = new File(path.getPath)
          if(!f.exists || e.sha1sum != Digest.sha1sum(f)) {

            // Jar file is not present in this machine.
            val jarURL = new URL(s"${cb.urlPrefix}/${e.sha1sum}")
            val jarFile = localJarPath(e.sha1sum)
            jarFile.deleteOnExit()

            withResource(new BufferedOutputStream(new FileOutputStream(jarFile))) { out =>
              debug(s"Connecting to ${jarURL}")
              withResource(jarURL.openStream) { in =>
                val buf = new Array[Byte](8192)
                var readBytes = 0
                while({ readBytes = in.read(buf); readBytes != -1}) {
                  out.write(buf, 0, readBytes)
                }
              }
            }
            s += ClassBox.JarEntry(jarFile.toURI.toURL, sha1sum, lastModified)
            hasChanged = true
          }
          else
            s += e
        case LocalPathEntry(path, sha1sum) => {
          s += e // do nothing for local paths
        }
      }
    }

    info(s"done.")
    if(hasChanged)
      new ClassBox(localhost.address, config.dataServerPort, s.result)
    else
      cb
  }

}



/**
 * Container of class files (including *.class and *.jar files)
 *
 * @author Taro L. Saito
 */
case class ClassBox(address:String, port:Int, entries:Seq[ClassBox.ClassBoxEntry]) extends ClassBoxAPI with Logger {

  def urlPrefix = s"http://${address}:${port}/jars"

  def sha1sum = {
    val sha1sum_seq = entries.map(_.sha1sum).mkString(":")
    withResource(new ByteArrayInputStream(sha1sum_seq.getBytes)) { s =>
      Digest.sha1sum(s)
    }
  }

  val id : UUID = {
    UUID.nameUUIDFromBytes(sha1sum.getBytes)
  }

  /**
   * Return a class loader for this ClassBox. The parent is the context class loader to use t
   * he existing Silk framework classes.
   */
  def classLoader : URLClassLoader = {
    val urls = entries.map(_.path).toArray
    new URLClassLoader(urls, Thread.currentThread.getContextClassLoader)
  }

  /**
   * Create a class loader that is isolated from the context class loadser
   */
  def isolatedClassLoader: URLClassLoader = {
    val urls = entries.map(_.path).toArray
    new URLClassLoader(urls, ClassLoader.getSystemClassLoader)
  }

}

