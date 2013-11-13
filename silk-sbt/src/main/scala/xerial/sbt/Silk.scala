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

/**
 * @author Taro L. Saito
 */
object Silk extends sbt.Plugin {

  val silkRun = InputKey[Unit]("silk-run", "Run a silk expression")
  val silkVersion = settingKey[String]("silk version to use")

  lazy val silkSettings = Seq[Def.Setting[_]](
    silkVersion := version.value,
    silkRun := {
      val s = streams.value
      val logger = s.log
      val args : Seq[String] = spaceDelimited("<args>").parsed
      logger.info(s"silk run: args:[${args.mkString(", ")}]")
    },
    libraryDependencies <++= silkVersion { silkVer =>
      Seq("org.xerial.silk" % "silk-weaver" % silkVer)
    }
  )

}