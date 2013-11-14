//--------------------------------------
//
// SilkPlugin.scala
// Since: 2013/11/13 2:03 PM
//
//--------------------------------------

package xerial.sbt

import sbt._
import Keys._
import complete.DefaultParsers._
import Attributed._
import xerial.core.util.Shell
import java.net.URLClassLoader
import java.io.File
import sbt.File


/**
 * @author Taro L. Saito
 */
object SilkPlugin extends sbt.Plugin {

  object SilkKeys {
    val silk = InputKey[Unit]("silk", "Run a silk command")
    val silkRun = InputKey[Unit]("silk-run", "Run a silk expression")
    val silkFork = settingKey[Boolean]("fork JVM when running silk")
    val silkVersion = settingKey[String]("silk version to use")
    val silkJvmOpts = settingKey[Seq[String]]("JVM options to use when launching Silk")
  }

  import SilkKeys._

  lazy val silkSettings = Seq[Def.Setting[_]](
    silkVersion := version.value,
    silkJvmOpts := Seq("-Xmx512m"),
    silkFork := false,
    silk := {
      val s = streams.value
      val logger = s.log
      val args : Seq[String] = spaceDelimited("<args>").parsed
      logger.debug(s"Run silk: args:[${args.mkString(", ")}]")
      val fullCp : Seq[File] = data((fullClasspath in Runtime).value)

      val cmdLine = s"${args.mkString(" ")}"
      logger.info(s"Run silk: $cmdLine")

      val doFork = silkFork.value
      if(doFork) {
        // Launch a JVM
        logger.debug(s"Fork JVM")
        logger.debug(s"class path:${fullCp}")
        val javaCmdLine = s"-classpath ${Path.makeString(fullCp)} ${silkJvmOpts.value.mkString(" ")} xerial.silk.weaver.SilkMain $cmdLine"
        logger.debug(s"command line: $javaCmdLine")
        val proc = Shell.launchJava(javaCmdLine)

        val exitValue = proc.waitFor()
        logger.info(s"terminated ($exitValue)")
      }
      else {
        // Create a new class loader, then launch the command
        logger.debug(s"Launch Silk locally")
        val prevCl = Thread.currentThread.getContextClassLoader
        val prevJavaClassPath = sys.props("java.class.path")
        val cl = new URLClassLoader(fullCp.map(_.toURI.toURL).toArray, null)
        logger.debug(s"classpaths:\n${cl.getURLs.mkString("\n")}")
        try {
          Thread.currentThread.setContextClassLoader(cl)
          sys.props.update("java.class.path", fullCp.mkString(File.pathSeparator))
          val mainClass = Class.forName("xerial.silk.weaver.SilkMain", true, cl)
          val mainMethod = mainClass.getDeclaredMethod("main", classOf[java.lang.String])
          mainMethod.invoke(null, cmdLine)
        }
        finally {
          Thread.currentThread.setContextClassLoader(prevCl)
          sys.props.update("java.class.path", prevJavaClassPath)
        }
      }
    },
    libraryDependencies <++= silkVersion { silkVer =>
      Seq("org.xerial.silk" % "silk-weaver" % silkVer)
    }
  )

}