
organization := "org.xerial.silk"
sonatypeProfileName := "org.xerial"
name := "silk"
description := "A framework for simplifying SQL pipelines"
scalaVersion in Global := "2.11.6"

packSettings

packMain := Map("silk" -> "xerial.silk.cui.SilkMain")

lazy val root = project.in(file(".")).aggregate(core, cui)

lazy val core = project.in(file("silk-core")).settings(
  libraryDependencies ++= Seq(
    "com.github.nscala-time" %% "nscala-time" % "2.0.0",
    "org.xerial" % "xerial-lens" % "3.3.6",
    "org.scala-lang" % "scalap" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.ow2.asm" % "asm-all" % "4.1",
    "com.esotericsoftware.kryo" % "kryo" % "2.20" exclude("org.ow2.asm", "asm"),
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )
)

lazy val cui = project.in(file("silk-cui")).dependsOn(core)

