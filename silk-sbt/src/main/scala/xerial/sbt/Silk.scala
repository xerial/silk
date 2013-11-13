//--------------------------------------
//
// Silk.scala
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


/**
 * @author Taro L. Saito
 */
object Silk extends sbt.Plugin {

  val silkRun = InputKey[Unit]("silk-run", "Run a silk expression")
  val silkFork = settingKey[Boolean]("fork JVM when running silk")
  val silkVersion = settingKey[String]("silk version to use")
  val silkJvmOpts = settingKey[Seq[String]]("JVM options to use when launching Silk")

  lazy val silkSettings = Seq[Def.Setting[_]](
    silkVersion := version.value,
    silkJvmOpts := Seq("-Xmx512m"),
    silkFork := false,
    silkRun := {
      val s = streams.value
      val logger = s.log
      val args : Seq[String] = spaceDelimited("<args>").parsed
      logger.info(s"silk run: args:[${args.mkString(", ")}]")
      val fullCp : Seq[File] = data((fullClasspath in Runtime).value)
      logger.debug(s"class path:${fullCp}")

      val evalCmdLine = s"eval ${args.mkString(" ")}"

      val doFork = silkFork.value
      if(doFork) {
        // Launch a JVM
        val cmdLine = s"-classpath ${Path.makeString(fullCp)} ${silkJvmOpts.value.mkString(" ")} xerial.silk.weaver.SilkMain $evalCmdLine"
        logger.debug(s"command line: $cmdLine")
        val proc = Shell.launchJava(cmdLine)

        val exitValue = proc.waitFor()
        logger.info(s"terminated ($exitValue)")
      }
      else {
        // Create a new class loader, then launch the command
        val prevCl = Thread.currentThread.getContextClassLoader
        val cl = new URLClassLoader(fullCp.map(_.toURI.toURL).toArray)
        try {
          Thread.currentThread.setContextClassLoader(cl)
          val mainClass = Class.forName("xerial.silk.weaver.SilkMain", true, cl)
          val mainMethod = mainClass.getDeclaredMethod("main", classOf[java.lang.String])
          //val moduleField = mainClass.getDeclaredField("MODULE$")
          //val module = moduleField.get(null)
          mainMethod.invoke(null, evalCmdLine)
        }
        finally {
          Thread.currentThread.setContextClassLoader(prevCl)
        }
      }
    },
    libraryDependencies <++= silkVersion { silkVer =>
      Seq("org.xerial.silk" % "silk-weaver" % silkVer)
    }
  )

}