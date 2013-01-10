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
import java.net.{URL, URLClassLoader}
import xerial.core.log.Logger
import xerial.silk.io.Digest
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry
import xerial.core.io.IOUtil._
import xerial.silk.{cluster, SilkMain}
import javax.print.attribute.standard.DateTimeAtCompleted
import java.util.{UUID, Calendar}
import java.text.{SimpleDateFormat, DateFormat}

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

    val jarEntries = for(jarURL <- cp.filter(isJarFile)) yield {
      val f = new File(jarURL.getFile)
      JarEntry(jarURL, Digest.sha1sum(f), f.lastModified)
    }

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
    ClassBox(je)
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

  lazy private val buildTime = SilkMain.getBuildTime getOrElse (new SimpleDateFormat("yyy/MM/dd").parse("2012/12/20").getTime)

  /**
   * Creat a new jar file from a given set of files
   * @param entries
   * @return
   */
  private[cluster] def createJarFile(entries:Seq[FilePath]) : JarEntry = {
    val tmpJar = File.createTempFile("context", ".jar", config.silkTmpDir)
    // TODO delete tmpJar on some timing
    // tmpJar.deleteOnExit()
    debug("Creating current context jar file: %s", tmpJar)

    val jar = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(tmpJar)))

    // write MANIFEST
    val mEntry = new ZipEntry("META-INF/MANIFEST.MF")
    mEntry.setTime(buildTime)
    jar.putNextEntry(mEntry)
    val manifestHeader = "Manifest-Version: 1.0\n".getBytes()
    jar.write(manifestHeader)
    jar.closeEntry()

    // put files into jar
    val buf = new Array[Byte](8192)
    for(e <- entries) {
      //trace("Copying entry: %s, %s", e.relativePath, e.fullPath)
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


    JarEntry(tmpJar.toURI.toURL, Digest.sha1sum(tmpJar), buildTime)
  }


  case class JarEntry(path:URL, sha1sum:String, lastModified:Long)

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


  /**
   * Check all jar entries in this ClassBox. If there is missing jars,
   * retrieve them from the host.
   *
   * TODO: caching the results
   *
   * @return
   */
  def sync(cb:ClassBox, host:ClientAddr) : ClassBox = {
    val s = Seq.newBuilder[ClassBox.JarEntry]
    var hasChanged = false
    for(e <- cb.entries) {
      val f = new File(e.path.getPath)
      if(!f.exists || e.sha1sum != Digest.sha1sum(f)) {
        // Jar file is not present in this machine.
        val jarURL = new URL("http://%s/jars/%s".format(host.address, e.sha1sum))
        val jarFile = new File(config.silkTmpDir, "jars/%s/%s".format(e.sha1sum.substring(0, 2), e.sha1sum))
        jarFile.deleteOnExit()
        //debug("Downloading jar from %s -> %s", jarURL, jarFile)
        jarFile.getParentFile.mkdirs
        withResource(new BufferedOutputStream(new FileOutputStream(jarFile))) { out =>
          withResource(jarURL.openStream) { in =>
            val buf = new Array[Byte](8192)
            var readBytes = 0
            while({ readBytes = in.read(buf); readBytes != -1}) {
              out.write(buf, 0, readBytes)
            }
          }
        }
        s += ClassBox.JarEntry(jarFile.toURI.toURL, e.sha1sum, e.lastModified)
        hasChanged = true
      }
      else {
        s += e
      }
    }

    if(hasChanged)
      new ClassBox(s.result)
    else
      cb
  }

}

/**
 * Container of class files (including *.class and *.jar files)
 *
 * @author Taro L. Saito
 */
case class ClassBox(entries:Seq[ClassBox.JarEntry]) extends Logger {
  val sha1sum = {
    val sha1sum_seq = entries.map(_.sha1sum).mkString(":")
    withResource(new ByteArrayInputStream(sha1sum_seq.getBytes)) { s =>
      Digest.sha1sum(s)
    }
  }

  def id = sha1sum

  /**
   * Return the class loader
   * @return
   */
  def classLoader : URLClassLoader = {
    val urls = entries.map(_.path).toArray
    //trace("class loader urls:\n%s", urls.mkString("\n"))

    new URLClassLoader(urls, ClassLoader.getSystemClassLoader)
  }

}

