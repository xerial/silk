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


/**
 * @author Taro L. Saito
 */
object Silk extends sbt.Plugin {

  val silkRun = InputKey[Unit]("silk-run", "Run a silk expression")
  val silkVersion = settingKey[String]("silk version to use")
  val silkJvmOpts = settingKey[Seq[String]]("JVM options to use when launching Silk")

  lazy val silkSettings = Seq[Def.Setting[_]](
    silkVersion := version.value,
    silkJvmOpts := Seq("-Xmx512m"),
    fork in run := true,
    silkRun := {
      val s = streams.value
      val logger = s.log
      val args : Seq[String] = spaceDelimited("<args>").parsed
      logger.info(s"silk run: args:[${args.mkString(", ")}]")
      val fullCp = (fullClasspath in Runtime).value
      logger.debug(s"class path:${fullCp}")

      val cmdLine = s"-classpath ${Path.makeString(data(fullCp))} ${silkJvmOpts.value.mkString(" ")} xerial.silk.weaver.SilkMain eval ${args.mkString(" ")}"
      logger.debug(s"command line: $cmdLine")
      val proc = Shell.launchJava(cmdLine)

      val exitValue = proc.waitFor()
      logger.info(s"terminated ($exitValue)")
      //(runner in run).value.run("xerial.silk.weaver.SilkMain", data(fullCp), Seq("--help", "eval") ++ args, logger)
    },
    libraryDependencies <++= silkVersion { silkVer =>
      Seq("org.xerial.silk" % "silk-weaver" % silkVer)
    }
  )

}