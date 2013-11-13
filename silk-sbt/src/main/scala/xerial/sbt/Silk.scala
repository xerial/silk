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

  lazy val silkSettings = Seq[Def.Setting[_]](
    silkVersion := version.value,
    fork in run := true,
    silkRun := {
      val s = streams.value
      val logger = s.log
      val args : Seq[String] = spaceDelimited("<args>").parsed
      logger.info(s"silk run: args:[${args.mkString(", ")}]")
      val fullCp = (fullClasspath in Runtime).value
      logger.debug(s"class path:${fullCp}")

      val cmdLineArgs = Seq("-classpath", Path.makeString(data(fullCp)), "xerial.silk.weaver.SilkMain", "eval") ++ args
      val cmdLine = cmdLineArgs.mkString(" ")
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