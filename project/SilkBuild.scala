import sbt._
import Keys._

object SilkBuild extends Build {
    lazy val root = Project(id = "silk", base = file(".")) aggregate(core, lens)
	
	lazy val core = Project(id = "silk-core", base = file("silk-core"))
	lazy val lens = Project(id = "silk-lens", base = file("silk-lens"))
}